# 创建客户端
opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://localhost:6650')
client = PulsarSdk::Client.create(opts)

# 获取分区主题信息
topic = 'persistent://public/default/partitioned-topic'
partitioned = PulsarSdk::Protocol::Partitioned.new(client, topic)

begin
  partitions = partitioned.partitions
  puts "主题 #{topic} 的分区列表:"
  partitions.each_with_index do |partition, index|
    puts "  分区 #{index}: #{partition}"
  end

  # 如果是分区主题，创建分区producer
  if partitions.size > 1
    puts "这是一个分区主题，共有 #{partitions.size} 个分区"

    # 创建分区producer管理器
    producer_opts = PulsarSdk::Options::Producer.new(topic: topic)
    producer_manager = PulsarSdk::Producer::Manager.new(client, producer_opts)

    # 发送消息到不同分区
    5.times do |i|
      message = PulsarSdk::Producer::Message.new(
        "分区消息 #{i}",
        nil,
        "key-#{i % 3}" # 使用不同的key来路由到不同分区
      )

      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::SEND,
        send: Pulsar::Proto::CommandSend.new(num_messages: 1)
      )

      result = producer_manager.execute(base_cmd, message)
      puts "发送消息到分区，结果: #{result.inspect}"
    end

    producer_manager.close
  end
rescue => e
  puts "获取分区信息失败: #{e.message}"
end

client.close
puts "分区主题示例执行完成"