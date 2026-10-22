"""Current Delta schema/properties without DESCRIBE DETAIL file statistics.

DeltaLog bridge verified against deployed Delta 4.4.1. Fail closed on runtime
incompatibility; a failed metadata read never implies a missing table.
"""
import json
from .metrics import timed,bump
from ..errors import PipelineError


def read_metadata(spark,path):
    from pyspark.sql.types import StructType
    with timed('delta_target_snapshot'):
        try:
            log=spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,path)
            empty=spark._jvm.scala.Option.empty()
            # Refresh for every writer invocation; no stale-snapshot shortcut.
            snapshot=log.update(False,empty,empty)
            version=int(snapshot.version())
            if version<0:return None
            schema=StructType.fromJson(json.loads(snapshot.schema().json()))
            metadata=snapshot.metadata();configuration=metadata.configuration()
            properties={}
            for key in ('pipeline.targetFingerprint','delta.appendOnly'):
                value=configuration.get(key)
                if value.isDefined():properties[key]=str(value.get())
            table_id=str(metadata.id())
            if not table_id:raise ValueError('empty table identity')
        except Exception:
            raise PipelineError('BRONZE_DELTA_METADATA_UNCONFIRMED') from None
    bump('delta_target_existing')
    return dict(version=version,table_id=table_id,schema=schema,properties=properties)
