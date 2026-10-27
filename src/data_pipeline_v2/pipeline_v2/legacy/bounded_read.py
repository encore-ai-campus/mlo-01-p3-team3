"""Bounded JSON reads; the execution strategy can be selected after a trial."""
from .errors import PipelineError


def payload_rows(frame,*,max_rows,max_bytes,error_code,mode=None):
    """Do not send an oversized payload to Python, or retain an unbounded list.

    A failed/partially consumed iterator never grants a storage proof. The
    caller must consume it fully before accepting any output or receipt.
    """
    from pyspark.sql import functions as F
    import os
    mode=mode or os.environ.get('PIPELINE_BOUNDED_READ_MODE','aggregate')
    if mode not in {'stream','aggregate'}:raise PipelineError('INVALID_BOUNDED_READ_MODE')
    size=F.length(F.encode('payload','UTF-8'))
    safe=frame.limit(max_rows+1).withColumn('_payload_bytes',size)
    safe=safe.withColumn('payload',F.when(F.col('_payload_bytes')<=max_bytes,F.col('payload')))
    if mode=='aggregate':
        from pyspark import StorageLevel
        cached=safe.persist(StorageLevel.DISK_ONLY)
        try:
            stats=cached.agg(F.count('*').alias('count'),F.sum('_payload_bytes').alias('bytes'),
                F.sum(F.col('payload').isNull().cast('int')).alias('invalid')).first()
            if stats['count']>max_rows or (stats['bytes'] or 0)>max_bytes or stats['invalid']:
                raise PipelineError(error_code)
            yield from cached.collect()
        finally:cached.unpersist()
        return
    iterator=safe.toLocalIterator(prefetchPartitions=False)
    count=total=0
    try:
        for row in iterator:
            count+=1;total+=row['_payload_bytes'] or 0
            if (count>max_rows or total>max_bytes or row['payload'] is None
                    or row['_payload_bytes'] is None):
                raise PipelineError(error_code)
            yield row
    finally:
        close=getattr(iterator,'close',None)
        if close:close()
