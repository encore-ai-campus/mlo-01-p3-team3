import unittest
from product_pipeline.kafka_tls import spark_options
from product_pipeline.errors import PipelineError


class KafkaTLSTests(unittest.TestCase):
    def config(self):
        return dict({'bootstrap.servers':'broker:9092','security.protocol':'SSL',
            'ssl.keystore.location':'/etc/pj3-clients/kafka/pipeline/keystore.p12',
            'ssl.truststore.location':'/etc/pj3-clients/kafka/pipeline/truststore.p12',
            'ssl.keystore.password':'fixture-store','ssl.truststore.password':'fixture-trust',
            'ssl.key.password':'fixture-key'})

    def test_keeps_distinct_store_trust_and_key_passwords(self):
        result=spark_options(self.config())
        self.assertEqual(result['kafka.ssl.truststore.password'],'fixture-trust')
        self.assertEqual(result['kafka.ssl.keystore.password'],'fixture-store')
        self.assertEqual(result['kafka.ssl.key.password'],'fixture-key')
        self.assertEqual(result['kafka.ssl.endpoint.identification.algorithm'],'https')

    def test_missing_settings_or_insecure_protocol_are_rejected(self):
        for key in self.config():
            value=self.config();value.pop(key)
            with self.subTest(key=key),self.assertRaises(PipelineError):spark_options(value)
        for key,value in [('security.protocol','PLAINTEXT'),('ssl.endpoint.identification.algorithm',''),
                          ('ssl.keystore.location','relative.p12'),('ssl.keystore.type','JKS')]:
            config=self.config();config[key]=value
            with self.subTest(key=key),self.assertRaises(PipelineError):spark_options(config)
