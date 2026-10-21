"""Explicit Kafka client TLS settings shared by Driver and Executors.

Node-specific Spark RPC stores must never be substituted for these stores.
Private passwords stay in the server's protected kafka-spark.json configuration.
"""
from .errors import PipelineError


def spark_options(config):
    required = ('bootstrap.servers', 'ssl.keystore.location', 'ssl.truststore.location',
                'ssl.keystore.password', 'ssl.truststore.password', 'ssl.key.password')
    if (config.get('security.protocol') != 'SSL'
            or any(not isinstance(config.get(k), str) or not config[k] for k in required)
            or any(not config[k].startswith('/') for k in ('ssl.keystore.location', 'ssl.truststore.location'))):
        raise PipelineError('KAFKA_SPARK_TLS_CONFIG_REQUIRED')
    if any(config.get(k, 'PKCS12') != 'PKCS12' for k in ('ssl.keystore.type', 'ssl.truststore.type')):
        raise PipelineError('KAFKA_SPARK_TLS_STORE_TYPE_INVALID')
    if config.get('ssl.endpoint.identification.algorithm', 'https') != 'https':
        raise PipelineError('KAFKA_SPARK_TLS_HOSTNAME_VERIFICATION_REQUIRED')
    values={k:config[k] for k in required}
    values.update({'security.protocol':'SSL', 'ssl.keystore.type':'PKCS12',
                   'ssl.truststore.type':'PKCS12', 'ssl.endpoint.identification.algorithm':'https'})
    return {'kafka.'+k:v for k,v in values.items()}
