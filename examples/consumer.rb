opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://rental_car/orders/created'
)
client = PulsarSdk::Client.create(opts)
consumer = client.subscribe(consumer_opts)

_cmd, msg = consumer.receive

msg.ack

consumer.close

client.close
