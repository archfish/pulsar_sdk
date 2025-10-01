require 'minitest/autorun'
require 'pulsar_sdk'

class TestConsumerBase < Minitest::Test
  def setup
    # 创建模拟对象
    @client = Minitest::Mock.new
    @message_tracker = Minitest::Mock.new
    @opts = Minitest::Mock.new

    # 设置opts的必要属性
    @opts.expect(:topic, 'persistent://public/default/test-topic')
    @opts.expect(:prefetch, 100)
    @opts.expect(:name, 'test-consumer')
    @opts.expect(:subscription_name, 'test-subscription')
    @opts.expect(:subscription_type, Pulsar::Proto::CommandSubscribe::SubType::Exclusive)
    @opts.expect(:replicate_subscription_state, false)
    @opts.expect(:read_compacted, false)

    # 创建Consumer::Base实例
    @consumer = PulsarSdk::Consumer::Base.new(@client, @message_tracker, @opts)
  end

  def test_initialization
    assert_instance_of(PulsarSdk::Consumer::Base, @consumer)
    assert_equal('persistent://public/default/test-topic', @consumer.topic)
  end

  def test_subscription
    # 测试subscription方法
    @consumer.send(:grab_cnx) rescue nil # 忽略连接错误
    assert_equal('test-subscription', @consumer.subscription)
  end

  def test_increase_fetched
    # 测试increase_fetched方法
    assert_equal(0, @consumer.instance_variable_get(:@fetched))
    @consumer.increase_fetched
    assert_equal(1, @consumer.instance_variable_get(:@fetched))
    @consumer.increase_fetched(5)
    assert_equal(6, @consumer.instance_variable_get(:@fetched))
  end

  def test_disconnect?
    # 初始状态下应该断开连接
    assert(@consumer.disconnect?)

    # 模拟建立连接
    @consumer.instance_variable_set(:@established, true)
    refute(@consumer.disconnect?)
  end

  def teardown
    @opts.verify
  end
end