opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://rental_car/orders/created'
)
client = PulsarSdk::Client.create(opts)
consumer = client.subscribe(consumer_opts)

# blocking until message arrived
_cmd, msg = consumer.receive

# if there no message wait 2 seconds
_cmd, msg = consumer.receive(2)
# check if there is a message
if msg.nil?
  # recall flow get 100 message
  consumer.flow(100)
  _cmd, msg = consumer.receive(2)
end

if msg.nil?
  raise 'no message now'
end

msg.ack

consumer.close

client.close
