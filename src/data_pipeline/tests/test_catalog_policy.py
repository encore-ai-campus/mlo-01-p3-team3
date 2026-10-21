import unittest
from product_pipeline.catalog_policy import classify, category_id, POLICY,image_request,IMAGE_AUTHORIZATION


class CatalogPolicyTests(unittest.TestCase):
    def test_current_window_reviews_do_not_depend_on_the_full_scan_cursor(self):
        from product_pipeline.catalog_policy import product_query
        from product_pipeline.errors import PipelineError
        sql,args=product_query(100,after='danawa:999',product_keys=['danawa:12','danawa:13'])
        self.assertIn('product_key IN',sql);self.assertNotIn('product_key>',sql)
        self.assertNotIn('danawa:999',args)
        with self.assertRaises(PipelineError):product_query(100,product_keys=[])
    def test_supported_furniture_and_living_categories(self):
        for name, family in [('침대','BED'),('패브릭소파','SOFA'),('식탁의자','CHAIR'),
                             ('모션데스크','DESK'),('거실테이블','TABLE'),('책장','STORAGE'),
                             ('벽/전신거울','MIRROR'),('러그/카페트','RUG'),('암막커튼','CURTAIN'),
                             ('침구세트','BEDDING')]:
            with self.subTest(name=name):
                self.assertEqual(classify(dict(name='실제 상품',categories=[dict(category_name=name)])),family)

    def test_mixed_missing_and_parts_remain_unapproved(self):
        for normal in [dict(name='소파 교체 부품',categories=[dict(category_name='소파')]),
                       dict(name='상품',categories=[dict(category_name='침대'),dict(category_name='소파')]),
                       dict(name='상품',categories=[dict(category_name='풀세트')]),
                       dict(name='',categories=[dict(category_name='의자')])]:
            self.assertIsNone(classify(normal))

    def test_taxonomy_identity_is_service_based_and_policy_does_not_approve_prices_or_images(self):
        self.assertEqual(category_id('CHAIR'),category_id('CHAIR'))
        self.assertNotEqual(category_id('CHAIR'),category_id('BED'))
        self.assertEqual(POLICY['price_history_mode'],'SNAPSHOT_V1')
        self.assertEqual(POLICY['representative_price_kinds'],[])
        self.assertNotIn('image',str(POLICY))

    def test_internal_image_approval_is_bound_to_verified_asset_and_source_version(self):
        link=dict(id='a'*32,source_version='source-v1:test',raw_document_id='b'*24,
            checksum='c'*64,asset_status='AVAILABLE',size_bytes=100,width_px=10,height_px=10,
            usage_review_json='{"decision":"NOT_REVIEWED"}',verified_source=True)
        request=image_request(link)
        self.assertEqual(request['payload']['permission_evidence_ref'],IMAGE_AUTHORIZATION)
        self.assertEqual(request['payload']['checksum'],'c'*64)
        self.assertIn('DISPLAY',request['payload']['scopes'])
        self.assertIn('GENERATION',request['payload']['asset_policy']['denied_scopes'])
        self.assertIsNotNone(image_request(dict(link,asset_status='PENDING_REVIEW')))
        for change in [dict(asset_status='MISSING'),dict(checksum=''),dict(width_px=0),dict(verified_source=False),
                       dict(usage_review_json='{"decision":"REVOKED"}')]:
            with self.subTest(change=change):self.assertIsNone(image_request(dict(link,**change)))
