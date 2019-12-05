opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')
client = PulsarSdk::Client.create(opts)

topic = 'persistent://rental_car/orders/created'
topic = PulsarSdk::Protocol::PartitionedTopic.new(client, topic)

topic.partitions
