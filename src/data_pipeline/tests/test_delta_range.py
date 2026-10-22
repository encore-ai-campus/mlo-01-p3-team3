import unittest
from product_pipeline.delta_range import validate_actions


class DeltaRangeTests(unittest.TestCase):
    def commit(self,operation='WRITE',mode='Append'):
        return {'commitInfo':{'operation':operation,'operationParameters':{'mode':mode}}}
    def add(self,path='part-one.parquet',change=True,partitions=None):
        return {'add':{'path':path,'dataChange':change,'partitionValues':partitions or {}}}
    def test_only_new_append_files_are_selected_across_compaction(self):
        rows=[self.commit(),self.add(),self.commit('OPTIMIZE'),
              {'remove':{'dataChange':False}},self.add('compacted.parquet',False)]
        self.assertEqual(validate_actions(rows,2,'s3a://bucket/raw'),['s3a://bucket/raw/part-one.parquet'])
    def test_schema_overwrite_missing_commit_or_partitioning_falls_back(self):
        for rows,count in [([self.commit(),{'metaData':{'id':'new'}}],1),
                           ([self.commit(mode='Overwrite'),self.add()],1),
                           ([self.commit(),{'remove':{'dataChange':True}}],1),
                           ([self.commit(),self.add(partitions={'p':'one'})],1),
                           ([self.commit(),self.add()],2)]:
            self.assertIsNone(validate_actions(rows,count,'s3a://bucket/raw'))
    def test_duplicate_or_escaping_file_path_is_never_read(self):
        for path in ['../other','%2e%2e/other','/tmp/other','s3a://other/data','dir\\other','dir//other']:
            self.assertIsNone(validate_actions([self.commit(),self.add(path)],1,'s3a://bucket/raw'))
        self.assertIsNone(validate_actions([self.commit(),self.add(),self.add()],1,'s3a://bucket/raw'))
