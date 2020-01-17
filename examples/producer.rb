client = PulsarSdk.create_client(logical_addr: 'pulsar://pulsar.reocar.lan')
producer = PulsarSdk.create_producer(client, topic: 'persistent://rental_car/orders/created')

# ++======测试普通消息======++
base_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(
    num_messages: 1
  )
)

p_msg = PulsarSdk::Producer::Message.new("dang qian shi jian #{TimeX.now}")

# ++======消息发送后等待系统响应======++
producer.execute(base_cmd, p_msg)

# ++======调用后立即返回，服务器可能还没收到消息======++
producer.execute_async(base_cmd, p_msg)

# ++======发送消息后需要获取消息回执，因为回执与producer_id和request_id有关必须要知道真实的producer才能获取到======++
producer.real_producer(p_msg) do |producer_|
  producer_.execute(base_cmd, p_msg)
  producer_.receipt
end

producer.close

# ++======测试延迟消息======++
producer = client.create_producer(producer_opts)
base_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(
    num_messages: 1
  )
)
# message will available in 10 second
deliver_at = TimeX.now + 10
p_msg = PulsarSdk::Producer::Message.new(
  "dang qian shi jian publush at: #{now}, performat at: #{deliver_at}",
  Pulsar::Proto::MessageMetadata.new(deliver_at_time: deliver_at.timestamp)
)
producer.execute(base_cmd, p_msg)
producer.close

client.close
