"""Silver schemas and image revision checks on the shared Delta writer."""
from ..infrastructure.delta import AppendOnlyDeltaSink
from ..errors import Conflict, PipelineError
from .normalize import KEYS, semantic_hash
from .schemas import columns, EXTRA, ARRAYS, DECIMALS, LONGS, TIMES


class DeltaSink(AppendOnlyDeltaSink):
    supports_history_bridge = True
    keys, schema_tables, schema_columns = KEYS, EXTRA, staticmethod(columns)
    arrays, decimals, longs, times, booleans = ARRAYS, DECIMALS, LONGS, TIMES, set()
    hash_row = staticmethod(semantic_hash)

    def __init__(self, spark, settings, *, lease):
        super().__init__(spark, settings, lease=lease, root=settings.silver_root)

    def assert_revision(self,raw_id,revision,fingerprint):
        from pyspark.sql import functions as F
        values=(self.spark.read.format('delta').load(self.paths['product_images'])
            .filter((F.col('raw_document_id')==raw_id)&(F.col('image_state_revision')==revision))
            .select('image_state_fingerprint').distinct().limit(2).collect())
        if any(row[0]!=fingerprint for row in values):raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')


    def assert_revisions(self,revisions):
        from pyspark.sql import functions as F
        if not revisions or len(revisions)>100:raise PipelineError('SILVER_BATCH_LIMIT')
        expected={(raw,rev):fp for raw,rev,fp in revisions};condition=None
        for raw,rev in expected:
            term=(F.col('raw_document_id')==raw)&(F.col('image_state_revision')==rev)
            condition=term if condition is None else condition|term
        values=(self.spark.read.format('delta').load(self.paths['product_images']).filter(condition)
            .select('raw_document_id','image_state_revision','image_state_fingerprint').distinct().limit(len(expected)+1).collect())
        if len(values)>len(expected) or any(expected[(r[0],r[1])]!=r[2] for r in values):
            raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')
