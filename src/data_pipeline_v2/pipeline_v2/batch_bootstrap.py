"""Compose finite-batch ports. Status/submit need neither Kafka connection nor JVM."""
from contextlib import contextmanager, ExitStack
from pathlib import Path
import os
from .domain.batch import BatchLimits, Retention, admission_contract
from .domain.contracts import Invalid
from .adapters.files import read
from .adapters.timing import Timing


def configuration(path):
    path = Path(path).resolve()
    value = read(path)
    # server-python: one Python process on the master (Mongo ledger, real Kafka, local file lake,
    # MySQL) for batch sizes where Spark's per-job and per-commit overhead dominates.
    if value.get('batch_config_version') != 1 or value.get('mode') not in {'local','server','server-python'}:
        raise Invalid('FINITE_BATCH_CONFIG_REQUIRED')
    if not value.get('target'):
        raise Invalid('BATCH_TARGET_REQUIRED')
    for name in ('state_root','lake_root','publication_root','broker_root'):
        if name in value and '://' not in value[name]:
            value[name] = str((path.parent/value[name]).resolve())
    BatchLimits(**value.get('batch_limits',{}))
    Retention(**value.get('retention',{}))
    admission_contract(value.get('admission'))
    if value['mode'] == 'local':
        roots = [Path(value[k]).resolve() for k in ('state_root','lake_root','publication_root','broker_root')]
        if any(a == b or a.is_relative_to(b) or b.is_relative_to(a) for i,a in enumerate(roots) for b in roots[i+1:]):
            raise Invalid('LOCAL_OUTPUTS_OVERLAP')
    elif value['mode'] == 'server-python':
        if 'replace-with-' in str(value):
            raise Invalid('SERVER_PLACEHOLDERS_REQUIRED_TO_REPLACE')
        if '://' in value.get('lake_root','') or value.get('single_coordinator') is not True:
            raise Invalid('LOCAL_LAKE_AND_SINGLE_WRITER_REQUIRED')
        if Path(value['lake_root']).resolve() == Path(value['state_root']).resolve():
            raise Invalid('LOCAL_OUTPUTS_OVERLAP')
        for key in ('store','mongo','mysql','kafka'):
            if not value.get(key):
                raise Invalid('BATCH_SERVER_CONFIG_REQUIRED')
    else:
        if 'replace-with-' in str(value):
            raise Invalid('SERVER_PLACEHOLDERS_REQUIRED_TO_REPLACE')
        if not value['lake_root'].rstrip('/').endswith('/direct-v2') or value.get('single_coordinator') is not True:
            raise Invalid('SEPARATE_BATCH_NAMESPACE_AND_WRITER_REQUIRED')
        for key in ('store','lease','mongo','mysql','kafka','spark_options_env'):
            if not value.get(key):
                raise Invalid('BATCH_SERVER_CONFIG_REQUIRED')
    return value


class LazyProducer:
    def __init__(self, config):
        self.config, self.inner = config, None

    def _open(self):
        if self.inner is None:
            if self.config['mode'] == 'local':
                from .adapters.kafka_batch import FileBroker
                self.inner = FileBroker(self.config['broker_root'])
            else:
                from .adapters.kafka_batch import KafkaProducer
                self.inner = KafkaProducer(self.config['kafka'])
        return self.inner

    def send(self,key,event):
        return self._open().send(key,event)

    def send_many(self,pairs):
        return self._open().send_many(pairs)

    @property
    def failure_code(self):
        return getattr(self.inner, 'failure_code', None)

    def close(self):
        if self.inner:
            self.inner.close()


def phase_lock(config):
    from .adapters.files import locked
    # One writer per target on this host: Delta processing and MySQL publication lock separately.
    return lambda name: locked(Path(config['state_root'])/('finite-' + name + '.lock'))


@contextmanager
def runtime(config, *, batch='runtime', writable=False, phase='all'):
    from .adapters.batch_store import FileBatchStore, MongoBatchStore
    from .adapters.batch_local import LocalBatchProcessor
    from .adapters.publication import FilePublisher, MySQLPublisher
    from .adapters.files import FileLake
    from .adapters.kafka_batch import FileBroker
    from .application.outbox import Outbox
    from .application.batch import BatchRunner
    timing = Timing(config['state_root'])
    limits = BatchLimits(**config.get('batch_limits',{}))
    retention = Retention(**config.get('retention',{}))
    try:
        with timing.measure('command.total',batch):
            with ExitStack() as stack:
                if config['mode'] == 'local':
                    store = FileBatchStore(config['state_root'])
                    broker = FileBroker(config['broker_root'])
                    publisher = FilePublisher(config['publication_root'])
                    processor = LocalBatchProcessor(store,broker,FileLake(config['lake_root']),timing,limits,
                        config.get('policy',{}),retention)
                elif config['mode'] == 'server-python':
                    from .adapters.kafka_batch import KafkaRangeReader
                    settings = config['store']
                    store = MongoBatchStore(os.environ[settings['uri_env']],settings['database'],settings['replica_set'])
                    # Only the manifest's offset ranges are read; no group join, no offset commit.
                    broker = KafkaRangeReader(config['kafka'])
                    stack.callback(broker.close)
                    # The phase file locks are the writer fence: one process per phase on this host.
                    publisher = MySQLPublisher(config['mysql'],fence=lambda:None,timing=timing)
                    processor = LocalBatchProcessor(store,broker,FileLake(config['lake_root']),timing,limits,
                        config.get('policy',{}),retention)
                else:
                    from .adapters.spark_runtime import ServerLake
                    from .adapters.spark_batch import SparkBatchProcessor
                    settings = config['store']
                    store = MongoBatchStore(os.environ[settings['uri_env']],settings['database'],settings['replica_set'])
                    # A publish-only worker reads Delta and fences MySQL with its own lease target.
                    lake = ServerLake(config,limits=type('MemoryLimit',(),{'history_bytes':limits.history_bytes})(),
                        writable=writable and phase != 'publish',timing=timing,
                        lease_role='mysql-publication' if writable and phase == 'publish' else None)
                    publisher = MySQLPublisher(config['mysql'],fence=lake.check,timing=timing)
                    processor = SparkBatchProcessor(store,lake,dict(mode='server',**settings),
                        config['kafka'],timing,limits,config.get('policy',{}),retention)
                stack.callback(store.close)
                stack.callback(publisher.close)
                stack.callback(processor.close)
                producer = LazyProducer(config)
                stack.callback(producer.close)
                target = dict(logical=config['target'],store=store.identity,publisher=publisher.identity,lake=config['lake_root'])
                runner = BatchRunner(store,processor,publisher,timing,limits,config.get('policy',{}),target,
                    lock=phase_lock(config),retention=retention,
                    kafka_retention_seconds=config.get('kafka',{}).get('retention_seconds'))
                outbox = Outbox(store,producer,timing,limits,config['admission'],retention,
                    wire_payload=config.get('kafka',{}).get('wire_payload',True))
                yield runner,outbox
    finally:
        timing.flush()
