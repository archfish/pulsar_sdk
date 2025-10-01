require 'minitest/autorun'
require 'pulsar_sdk'

class TestClientRpc < Minitest::Test
  def setup
    # 创建模拟对象
    @opts = Minitest::Mock.new
    @opts.expect(:is_a?, true, [PulsarSdk::Options::Connection])

    # 创建Client::Rpc实例
    @client = PulsarSdk::Client::Rpc.new(@opts)
  end

  def test_initialization
    assert_instance_of(PulsarSdk::Client::Rpc, @client)
  end

  def test_connection_method
    # 测试connection方法
    logical_addr = 'pulsar://localhost:6650'
    physical_addr = 'pulsar://192.168.1.100:6650'

    # 模拟连接池对象
    connection_pool = Minitest::Mock.new
    connection_pool.expect(:fetch, nil, [NilClass, NilClass])
    connection_pool.expect(:fetch, nil, [logical_addr, physical_addr])

    # 使用stub替换@cnx实例变量
    @client.instance_variable_set(:@cnx, connection_pool)

    # 测试默认参数
    @client.connection
    # 测试指定参数
    @client.connection(logical_addr, physical_addr)

    # 验证mock对象的调用
    connection_pool.verify
  end

  def test_create_producer
    # 测试create_producer方法
    producer_opts = Minitest::Mock.new
    producer_opts.expect(:is_a?, true, [PulsarSdk::Options::Producer])

    # 模拟producer创建
    producer = Minitest::Mock.new

    # 使用stub替换相关方法
    @client.stub(:producer, producer) do
      result = @client.create_producer(producer_opts)
      assert_equal(producer, result)
    end

    producer_opts.verify
  end

  def test_subscribe
    # 测试subscribe方法
    consumer_opts = Minitest::Mock.new
    consumer_opts.expect(:is_a?, true, [PulsarSdk::Options::Consumer])

    # 模拟consumer创建
    consumer = Minitest::Mock.new

    # 使用stub替换相关方法
    @client.stub(:consumer, consumer) do
      result = @client.subscribe(consumer_opts)
      assert_equal(consumer, result)
    end

    consumer_opts.verify
  end

  def test_close
    # 测试close方法
    connection_pool = Minitest::Mock.new
    connection_pool.expect(:close, nil)

    @client.instance_variable_set(:@cnx, connection_pool)
    @client.close

    connection_pool.verify
  end

  def teardown
    @opts.verify
  end
end