# 创建客户端
opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://localhost:6650')

# 创建消费者选项
consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://public/default/test-topic',
  subscription_name: 'test-subscription',
  subscription_type: Pulsar::Proto::CommandSubscribe::SubType::Shared,
  prefetch: 10
)

# 创建客户端和消费者
client = PulsarSdk::Client.create(opts)
consumer = client.subscribe(consumer_opts)

puts "消费者已创建，开始监听消息..."

# 监听消息并手动确认
puts "开始手动确认模式监听..."
consumer.listen do |cmd, msg|
  begin
    puts "收到命令: #{cmd.inspect}"
    puts "收到消息: #{msg.payload.inspect}"

    # 处理消息
    if msg.payload.is_a?(String) && msg.payload.include?("延迟消息")
      puts "这是一条延迟消息"
    end

    # 手动确认消息
    msg.ack
    puts "消息已确认"
  rescue => e
    puts "处理消息时出错: #{e.message}"
    # 消息处理失败，否定确认
    msg.nack
  end
end

# 带重连机制的消费者示例
def start_consumer_with_reconnect
  opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://localhost:6650')
  consumer_opts = PulsarSdk::Options::Consumer.new(
    topic: 'persistent://public/default/test-topic',
    subscription_name: 'reconnect-subscription'
  )

  loop do
    begin
      client = PulsarSdk::Client.create(opts)
      consumer = client.subscribe(consumer_opts)

      puts "消费者连接成功，开始接收消息..."

      consumer.listen do |cmd, msg|
        puts "收到消息: #{msg.payload.inspect}"
        msg.ack
      end

    rescue => e
      puts "消费者连接出错: #{e.message}"
      puts "5秒后尝试重新连接..."
      sleep 5
      retry
    ensure
      consumer&.close
      client&.close
    end
  end
end

# 错误处理示例
begin
  # 尝试订阅不存在的主题
  bad_consumer_opts = PulsarSdk::Options::Consumer.new(
    topic: 'persistent://public/default/non-existent-topic',
    subscription_name: 'bad-subscription'
  )
  bad_consumer = client.subscribe(bad_consumer_opts)
rescue => e
  puts "订阅失败: #{e.message}"
end

# 关闭消费者和客户端
consumer.close
client.close

puts "Consumer示例执行完成"