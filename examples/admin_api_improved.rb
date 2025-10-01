require 'pulsar_admin'

# 创建管理客户端
api = PulsarAdmin.create_client(endpoint: 'http://localhost:8080', tenant: 'public')

begin
  # 列出所有命名空间
  puts "列出所有命名空间:"
  namespaces = api.list_namespaces
  namespaces.each { |ns| puts "  #{ns}" }

  # 创建新命名空间
  new_namespace = 'test-namespace'
  if api.create_namespace(new_namespace)
    puts "成功创建命名空间: #{new_namespace}"
  else
    puts "创建命名空间失败: #{new_namespace}"
  end

  # 在命名空间中创建主题
  topic_name = 'test-topic'
  if api.create_topic(new_namespace, topic_name)
    puts "成功创建主题: #{topic_name}"
  else
    puts "创建主题失败: #{topic_name}"
  end

  # 列出命名空间中的所有主题
  puts "命名空间 #{new_namespace} 中的主题:"
  topics = api.namespace_topics(new_namespace)
  topics.each { |topic| puts "  #{topic}" }

  # 删除主题
  if api.delete_topic(new_namespace, topic_name)
    puts "成功删除主题: #{topic_name}"
  else
    puts "删除主题失败: #{topic_name}"
  end

  # 删除命名空间
  if api.delete_namespace(new_namespace)
    puts "成功删除命名空间: #{new_namespace}"
  else
    puts "删除命名空间失败: #{new_namespace}"
  end

rescue => e
  puts "管理API操作出错: #{e.message}"
end

puts "管理API示例执行完成"