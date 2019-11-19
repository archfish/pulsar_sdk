opts = PulsarSdk::Options::Client.new(url: 'pulsar://pulsar.reocar.lan')

producer_opts = PulsarSdk::Options::Producer.new(
  topic: 'persistent://rental_car/orders/created'
)

client = PulsarSdk::Client.new(opts)
producer = client.create_producer(producer_opts)

# ++======测试普通消息======++
producer.despatch("dang qian shi jian #{Time.now}")
producer.receipt
producer.close

# ++======测试延迟消息======++
producer = client.create_producer(producer_opts)
base_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(
    sequence_id: 0,
    num_messages: 1
  )
)

now = Time.now
deliver_at = now + 10
p_msg = PulsarSdk::ProducerMessage.new(
  "dang qian shi jian publush at: #{now}, performat at: #{deliver_at}",
  Pulsar::Proto::MessageMetadata.new(
    sequence_id: 0,
    publish_time: (now.to_f * 1000).to_i,
    deliver_at_time: (deliver_at.to_f * 1000).to_i
  )
)
producer.despatch_ex(base_cmd, p_msg)
producer.close

client.close
