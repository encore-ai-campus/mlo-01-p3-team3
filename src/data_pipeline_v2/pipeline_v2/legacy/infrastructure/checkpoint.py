"""Shared checkpoint ownership and immutable input archives; cursor is a hint only."""
from ..contracts import canonical,digest
from ..errors import Conflict,PipelineError
from ..files import parse



class InputCheckpoint:
    def __init__(self,spark,settings,lease):
        self.jvm,self.settings,self.lease=spark._jvm,settings,lease
        self.path=self.jvm.org.apache.hadoop.fs.Path(settings.checkpoint_path)
        self.fs=self.path.getFileSystem(spark._jsc.hadoopConfiguration())
        self.lease.check(settings)
        marker=self._path(self.marker_name)
        expected=dict(target_fingerprint=settings.fingerprint,stage=self.stage)
        if self.fs.exists(marker):
            if self._read(marker)!=expected:raise Conflict('SILVER_CHECKPOINT_TARGET_CONFLICT')
        else:
            if self.fs.exists(self.path) and len(self.fs.listStatus(self.path)):
                raise Conflict('SILVER_CHECKPOINT_BINDING_MISSING')
            self.fs.mkdirs(self.path)
            self._write(marker,expected,overwrite=False)

    def _path(self,name):return self.jvm.org.apache.hadoop.fs.Path(self.path,name)
    def _read(self,path):
        from ..spark_session import bump
        bump('archive_reads')
        stream=self.fs.open(path)
        try:return parse(bytes(stream.readAllBytes()).decode('utf-8'))
        finally:stream.close()
    def _write(self,path,value,overwrite):
        from ..spark_session import bump
        bump('archive_writes')
        self.lease.check(self.settings)
        stream=self.fs.create(path,overwrite)
        try:stream.write(bytearray(canonical(value).encode()));stream.hflush()
        finally:stream.close()
        self.lease.check(self.settings)

    def archive(self,value):
        import time
        from ..spark_session import bump
        started=time.monotonic()
        try:return self._archive(value)
        finally:bump('archive_ms',round((time.monotonic()-started)*1000))

    def archive_many(self,values):
        """Verified bundles keep per-entry hashes without 2N remote round trips.

        Only new bounded pages use bundles. Legacy plain-object references and
        their files remain valid; no archive migration or deletion is needed.
        """
        from ..spark_session import cache_get,cache_put,bump
        if len(values)>100:raise PipelineError('SILVER_ARCHIVE_BATCH_LIMIT')
        root=self.path.toString();refs={};pending={};size=0
        def flush():
            nonlocal pending,size
            if not pending:return
            bundle=dict(contract_version=1,kind='PIPELINE_INPUT_BUNDLE',entries=pending)
            ref=self.archive(bundle)  # full readback before publishing any ref
            for checksum in pending:
                refs[checksum]=ref+'#sha256='+checksum
                cache_put('verified_input_refs',(self.stage,root,checksum),refs[checksum])
            bump('archive_bundles');bump('archive_bundled_entries',len(pending))
            pending={};size=0
        for value in values:
            checksum=digest(value)
            if checksum in refs or checksum in pending:continue
            cached=cache_get('verified_input_refs',(self.stage,root,checksum))
            if cached is not None:refs[checksum]=cached;continue
            count=len(canonical(value).encode('utf-8'))+len(checksum)+8
            if pending and size+count>8*1024*1024:flush()
            if count>8*1024*1024:
                ref=self.archive(value);refs[checksum]=ref
                cache_put('verified_input_refs',(self.stage,root,checksum),ref)
            else:pending[checksum]=value;size+=count
        flush()
        return [refs[digest(value)] for value in values]

    def read_input(self,reference):
        """Resolve both old standalone inputs and checked bundle entry refs."""
        from urllib.parse import urldefrag
        import re
        base,fragment=urldefrag(reference)
        prefix=self.path.toString().rstrip('/')+'/inputs/'
        name=base[len(prefix):] if base.startswith(prefix) else ''
        if not re.fullmatch('[a-f0-9]{64}\\.json',name):raise PipelineError('SILVER_ARCHIVE_REF_INVALID')
        value=self._read(self.jvm.org.apache.hadoop.fs.Path(base))
        if digest(value)+'.json'!=name:raise Conflict('SILVER_INPUT_ARCHIVE_CONFLICT')
        if not fragment:return value
        if (not re.fullmatch('sha256=[a-f0-9]{64}',fragment)
                or value.get('kind')!='PIPELINE_INPUT_BUNDLE' or value.get('contract_version')!=1):
            raise PipelineError('SILVER_ARCHIVE_REF_INVALID')
        checksum=fragment[7:];entry=value.get('entries',{}).get(checksum)
        if entry is None or digest(entry)!=checksum:raise Conflict('SILVER_INPUT_ARCHIVE_CONFLICT')
        return entry

    def _archive(self,value):
        from ..spark_session import cache_get,cache_put
        checksum=digest(value)
        path=self._path('inputs/'+checksum+'.json')
        reference=path.toString();key=(self.stage,reference)
        # A successful readback of this content-addressed object is reusable
        # within one Driver only. A restart always reads storage again.
        if cache_get('verified_input_archives',key) is True:return reference
        if not getattr(self,'_archive_parent_ready',False):
            self.fs.mkdirs(path.getParent());self._archive_parent_ready=True
        if self.fs.exists(path):
            try:stored=self._read(path)
            except (PipelineError,UnicodeError) as exc:
                if isinstance(exc,PipelineError) and exc.code!='INVALID_JSON':raise
                # A truncated, never-readable input is not a committed snapshot.
                # Under the exclusive lease restore exactly the same content-addressed input.
                self._write(path,value,True)
                stored=self._read(path)
            if stored!=value:raise Conflict('SILVER_INPUT_ARCHIVE_CONFLICT')
        else:
            self._write(path,value,False)
            if self._read(path)!=value:raise Conflict('SILVER_INPUT_ARCHIVE_UNCONFIRMED')
        cache_put('verified_input_archives',key,True)
        return reference

    def cursor(self,version):
        path=self._path('discovery.json')
        if not self.fs.exists(path):return dict(bronze_version=version,after=None,epoch=0,completed=False)
        try:
            envelope=self._read(path)
            value=envelope['body']
            if envelope['sha256']!=digest(value):raise ValueError()
            if (type(value.get('completed')) is not bool or type(value.get('epoch')) is not int
                    or value['epoch']<0 or (value.get('after') is not None and
                        (not isinstance(value['after'],str) or len(value['after'])!=64))):raise ValueError()
            if value.get('bronze_version')!=version or value.get('completed'):
                return dict(bronze_version=version,after=None,epoch=value.get('epoch',0)+1,completed=False)
            if value.get('target_fingerprint')!=self.settings.fingerprint:raise ValueError()
            return value
        except (ValueError,KeyError,TypeError,PipelineError):
            # A torn optional discovery hint never authorizes skipping unproven outputs.
            return dict(bronze_version=version,after=None,epoch=0,completed=False,reset_reason='CURSOR_INVALID')

    def save(self,value):
        body=dict(value,target_fingerprint=self.settings.fingerprint)
        self._write(self._path('discovery.json'),dict(body=body,sha256=digest(body)),True)
