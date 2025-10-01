# 消息序列化示例
# 演示如何使用新的消息序列化功能和json_encoded?辅助方法

require 'bundler/setup'
require 'pulsar_sdk'

# 创建客户端
client_opts = PulsarSdk::Options::Connection.new(logical_addr: 'pulsar://localhost:6650')
client = PulsarSdk::Client.create(client_opts)

# 创建生产者
producer_opts = PulsarSdk::Options::Producer.new(
  topic: 'persistent://public/default/serialization-test'
)
producer = client.create_producer(producer_opts)

# 创建消费者
consumer_opts = PulsarSdk::Options::Consumer.new(
  topic: 'persistent://public/default/serialization-test',
  subscription_name: 'serialization-subscription'
)
consumer = client.subscribe(consumer_opts)

puts "=== 消息序列化示例 ==="

# 1. 发送字符串消息
puts "\n1. 发送字符串消息:"
string_message = "这是一条纯文本消息"
msg1 = PulsarSdk::Producer::Message.new(string_message)
puts "消息内容: #{msg1.message}"
puts "是否JSON编码: #{msg1.json_encoded?}"

# 发送消息
send_cmd = Pulsar::Proto::BaseCommand.new(
  type: Pulsar::Proto::BaseCommand::Type::SEND,
  send: Pulsar::Proto::CommandSend.new(num_messages: 1)
)
producer.execute(send_cmd, msg1)

# 2. 发送哈希消息
puts "\n2. 发送哈希消息:"
hash_message = { name: "张三", age: 30, city: "北京" }
msg2 = PulsarSdk::Producer::Message.new(hash_message)
puts "消息内容: #{msg2.message}"
puts "是否JSON编码: #{msg2.json_encoded?}"

producer.execute(send_cmd, msg2)

# 3. 发送数组消息
puts "\n3. 发送数组消息:"
array_message = [1, 2, 3, "four", "five"]
msg3 = PulsarSdk::Producer::Message.new(array_message)
puts "消息内容: #{msg3.message}"
puts "是否JSON编码: #{msg3.json_encoded?}"

producer.execute(send_cmd, msg3)

# 4. 发送自定义对象消息
puts "\n4. 发送自定义对象消息:"
class Person
  attr_accessor :name, :age

  def initialize(name, age)
    @name, @age = name, age
  end

  def to_json(*args)
    { name: @name, age: @age }.to_json
  end
end

person = Person.new("李四", 25)
msg4 = PulsarSdk::Producer::Message.new(person)
puts "消息内容: #{msg4.message}"
puts "是否JSON编码: #{msg4.json_encoded?}"

producer.execute(send_cmd, msg4)

puts "\n所有消息已发送，现在开始接收消息...\n"

# 接收并处理消息
received_count = 0
consumer.listen do |cmd, msg|
  received_count += 1
  puts "\n--- 接收到第 #{received_count} 条消息 ---"

  # 使用新的辅助方法判断消息是否需要JSON反序列化
  if msg.json_encoded?
    puts "消息是JSON编码的 (Content-Type: #{PulsarSdk::Producer::Message::CONTENT_TYPE_JSON})"
    begin
      # 尝试解析JSON内容
      parsed_data = JSON.parse(msg.payload)
      puts "解析后的数据: #{parsed_data}"
    rescue JSON::ParserError => e
      puts "JSON解析失败: #{e.message}"
      puts "原始内容: #{msg.payload}"
    end
  else
    puts "消息是纯文本 (Content-Type: #{PulsarSdk::Producer::Message::CONTENT_TYPE_TEXT})"
    puts "消息内容: #{msg.payload}"
  end

  # 确认消息
  msg.ack
  puts "消息已确认"

  # 接收4条消息后停止
  if received_count >= 4
    break
  end
end

# 清理资源
producer.close
consumer.close
client.close

puts "\n示例执行完成"
