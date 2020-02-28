require 'pulsar_admin'

# default is persistent operates
api = PulsarAdmin.create_client(endpoint: 'http://pulsar.reocar.lan', tenant: 'rental_car')

# List namespace in current tenant
api.list_namespaces

# Create namespace
api.create_namespace('test-ns-1')

# Delete namespace
api.delete_namespace('test-ns-1')

# List topics in namespace `public`
res = api.namespace_topics('public')

# Create topic
res = api.create_topic('default', 'topic-01')

# Delete topic
res = api.delete_topic('default', 'topic-01')

# Peek N message
res = api.peek_messages(namespace: 'promotions', topic: 'changed', sub_name: 'PromotionSearchJobConsumer', message_position: '0', count: 3)
