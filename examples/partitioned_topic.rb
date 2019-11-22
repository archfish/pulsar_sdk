opts = PulsarSdk::Options::Client.new(url: 'pulsar://pulsar.reocar.lan')
client = PulsarSdk::Client.new(opts)

topic = 'persistent://rental_car/orders/created'
topic = PulsarSdk::Protocol::PartitionedTopic.new(client, topic)

topic.partitions
