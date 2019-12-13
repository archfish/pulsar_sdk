opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://pulsar.reocar.lan')

# if you want listen stop when no message in sometime, just set listen_wait
consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://rental_car/orders/created',
  prefetch: 1
)

client = PulsarSdk::Client.create(opts)
consumer = client.subscribe(consumer_opts)

#================Manual ack================#
consumer.listen do |cmd, msg|
  puts "cmd => #{cmd}"
  puts "msg => #{msg}"
  msg.ack
end

#================Auto ack================#

# Auto ack
# last value: false nack, else ack

# ack after process
consumer.listen(true) do |cmd, msg|
  puts "cmd => #{cmd}"
  puts "msg => #{msg}"
end

# nack after process
consumer.listen(true) do |cmd, msg|
  puts "cmd => #{cmd}"
  puts "msg => #{msg}"
  false
end

consumer.close

client.close
