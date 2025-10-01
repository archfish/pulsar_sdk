# 错误处理和重连机制示例

def create_robust_producer
  max_retries = 5
  retry_count = 0

  loop do
    begin
      client = PulsarSdk.create_client(logical_addr: 'pulsar://localhost:6650')
      producer_opts = PulsarSdk::Options::Producer.new(
        topic: 'persistent://public/default/robust-topic'
      )
      producer = client.create_producer(producer_opts)

      puts "Producer创建成功"
      return [client, producer]

    rescue => e
      retry_count += 1
      puts "创建Producer失败 (尝试 #{retry_count}/#{max_retries}): #{e.message}"

      if retry_count >= max_retries
        raise "无法创建Producer: #{e.message}"
      end

      sleep(2 ** retry_count) # 指数退避
      retry
    end
  end
end

# 创建健壮的producer
begin
  client, producer = create_robust_producer

  # 发送消息并处理可能的错误
  base_cmd = Pulsar::Proto::BaseCommand.new(
    type: Pulsar::Proto::BaseCommand::Type::SEND,
    send: Pulsar::Proto::CommandSend.new(num_messages: 1)
  )

  5.times do |i|
    begin
      msg = PulsarSdk::Producer::Message.new("消息 #{i}")
      result = producer.execute(base_cmd, msg)
      puts "消息 #{i} 发送成功"
    rescue => e
      puts "发送消息 #{i} 失败: #{e.message}"
    end
  end

  producer.close
  client.close

rescue => e
  puts "程序执行出错: #{e.message}"
  puts e.backtrace.join("\n")
end

puts "错误处理示例执行完成"