opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://rental_car/orders/created',
  prefetch: 100
)
client = PulsarSdk::Client.create(opts)
consumer = client.subscribe(consumer_opts)
consumer.flow

# blocking until message arrived
_cmd, msg = consumer.receive

# if there no message wait 2 seconds
_cmd, msg = consumer.receive(2)
# check if there is a message
if msg.nil?
  # recall flow get another 100 message
  consumer.flow
  _cmd, msg = consumer.receive(2)
end

if msg.nil?
  raise 'no message now'
end

msg.ack

consumer.close

client.close
