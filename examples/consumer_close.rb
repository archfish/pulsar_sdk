opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

# topic persistent://rental_car/orders/none has no message
consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://rental_car/orders/none',
  prefetch: 1
)

client = PulsarSdk::Client.create(opts)
@consumer = client.subscribe(consumer_opts)

# =========stop receive============
th = Thread.new do
  @consumer.flow
  _cmd, msg = @consumer.receive
  puts "msg: #{msg || 'stoped'}"
end

# =========stop listen============

th = Thread.new do
  @consumer.listen do |cmd, msg|
    puts "msg: #{msg}"
  end
  puts "stoped"
end

@consumer.close
th.join
