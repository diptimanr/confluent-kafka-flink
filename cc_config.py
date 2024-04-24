cc_config = {
 'bootstrap.servers': '<YOUR_CLUSTER>.us-east-2.aws.confluent.cloud:9092',
 'security.protocol': 'SASL_SSL',
 'sasl.mechanisms': 'PLAIN',
 'sasl.username': '<CLUSTER_API_KEY>',
 'sasl.password': '<CLUSTER_API_SECRET>'
}

sr_config = {
 'url': 'https://<YOUR_SCHEMA_REGISTRY_ENDPOINT>.us-east-2.aws.confluent.cloud',
 'basic.auth.user.info': '<SCHEMA_REGISTRY_API_KEY>:<SCHEMA_REGISTRY_API_SECRET>'
}