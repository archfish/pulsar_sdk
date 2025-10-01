# 创建客户端
client = PulsarSdk.create_client(logical_addr: 'pulsar://localhost:6650')

# 创建producer选项
producer_opts = PulsarSdk::Options::Producer.new(
  topic: 'persistent://public/default/test-topic'
)

# 创建producer
producer = client.create_producer(producer_opts)

# 发送普通消息
base_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(
    num_messages: 1
  )
)

# 发送简单字符串消息
p_msg = PulsarSdk::Producer::Message.new("当前时间: #{Time.now}")

# 同步发送消息并等待响应
puts "同步发送消息..."
result = producer.execute(base_cmd, p_msg)
puts "消息发送结果: #{result.inspect}"

# 异步发送消息
puts "异步发送消息..."
producer.execute_async(base_cmd, p_msg)

# 发送消息并获取回执
puts "发送消息并获取回执..."
producer.real_producer(p_msg) do |producer_|
  producer_.execute(base_cmd, p_msg)
  receipt = producer_.receipt
  puts "消息回执: #{receipt.inspect}"
end

# 发送结构化数据（JSON）
json_data = {
  id: SecureRandom.uuid,
  name: "测试用户",
  timestamp: Time.now.to_i
}
json_msg = PulsarSdk::Producer::Message.new(json_data)
puts "发送JSON消息..."
producer.execute(base_cmd, json_msg)

# 发送带元数据的消息
metadata = Pulsar::Proto::MessageMetadata.new(
  partition_key: "user-123",
  properties: [
    Pulsar::Proto::KeyValue.new(key: "source", value: "web"),
    Pulsar::Proto::KeyValue.new(key: "version", value: "1.0")
  ]
)
meta_msg = PulsarSdk::Producer::Message.new("带元数据的消息", metadata)
puts "发送带元数据的消息..."
producer.execute(base_cmd, meta_msg)

# 发送延迟消息
deliver_at = Time.now + 30 # 30秒后投递
delay_metadata = Pulsar::Proto::MessageMetadata.new(
  deliver_at_time: (deliver_at.to_f * 1000).to_i # 转换为毫秒时间戳
)
delay_msg = PulsarSdk::Producer::Message.new(
  "延迟消息，将在 #{deliver_at} 投递",
  delay_metadata
)
puts "发送延迟消息..."
producer.execute(base_cmd, delay_msg)

# 关闭producer和客户端
producer.close
client.close

puts "Producer示例执行完成"