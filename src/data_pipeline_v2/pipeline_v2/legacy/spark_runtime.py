"""Explicit production standalone cluster profile; never fall back to local mode."""
import re
from urllib.parse import urlsplit
from .errors import PipelineError


def cluster_options(value):
    try:
        master=value['master'];host=value['driver_host'];parsed=urlsplit(master)
        if parsed.scheme!='spark' or not parsed.hostname or not parsed.port or parsed.username or parsed.password:raise ValueError()
        if parsed.path not in {'','/'} or parsed.query or parsed.fragment:raise ValueError()
        if not isinstance(host,str) or not re.fullmatch('[A-Za-z0-9_.-]+',host) or host in {'localhost','127.0.0.1','0.0.0.0'}:raise ValueError()
        for key in ['executor_cores','total_cores','shuffle_partitions']:
            if type(value[key]) is not int or not 1<=value[key]<=128:raise ValueError()
        if value['total_cores']<value['executor_cores']:raise ValueError()
        for key in ['executor_memory','driver_memory']:
            if not re.fullmatch('[1-9][0-9]*[mg]',value[key]):raise ValueError()
        if not value['executor_python'].startswith('/'):raise ValueError()
    except (KeyError,TypeError,ValueError,AttributeError):raise PipelineError('SPARK_CLUSTER_PROFILE_REQUIRED') from None
    return {'spark.master':master,'spark.driver.host':host,'spark.driver.bindAddress':host,
        'spark.executor.cores':str(value['executor_cores']),'spark.cores.max':str(value['total_cores']),
        'spark.executor.memory':value['executor_memory'],'spark.driver.memory':value['driver_memory'],
        'spark.scheduler.minRegisteredResourcesRatio':'1.0','spark.scheduler.maxRegisteredResourcesWaitingTime':'45s',
        'spark.sql.shuffle.partitions':str(value['shuffle_partitions']),
        'spark.default.parallelism':str(value['total_cores']),
        'spark.databricks.delta.snapshotPartitions':str(value['shuffle_partitions']),
        'spark.pyspark.python':value['executor_python']}
