# 性能测试示例

require 'benchmark'

# 创建客户端和producer
client = PulsarSdk.create_client(logical_addr: 'pulsar://localhost:6650')
producer_opts = PulsarSdk::Options::Producer.new(
  topic: 'persistent://public/default/performance-test'
)
producer = client.create_producer(producer_opts)

# 创建消费者
consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://public/default/performance-test',
  subscription_name: 'performance-test-sub'
)
consumer = client.subscribe(consumer_opts)

# 发送性能测试
message_count = 1000
puts "开始发送 #{message_count} 条消息..."

base_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(num_messages: 1)
)

send_time = Benchmark.measure do
  message_count.times do |i|
    msg = PulsarSdk::Producer::Message.new("性能测试消息 #{i}")
    producer.execute_async(base_cmd, msg)
  end
end

puts "发送 #{message_count} 条消息耗时: #{send_time.real} 秒"
puts "发送速率: #{message_count / send_time.real} 条消息/秒"

# 接收性能测试
puts "开始接收消息..."
received_count = 0
receive_time = Benchmark.measure do
  while received_count < message_count
    _cmd, msg = consumer.receive(5) # 5秒超时
    if msg
      msg.ack
      received_count += 1
    else
      break # 超时退出
    end
  end
end

puts "接收 #{received_count} 条消息耗时: #{receive_time.real} 秒"
puts "接收速率: #{received_count / receive_time.real} 条消息/秒"

# 清理资源
consumer.close
producer.close
client.close

puts "性能测试示例执行完成"