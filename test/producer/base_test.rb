require 'minitest/autorun'
require 'pulsar_sdk'

class TestProducerBase < Minitest::Test
  def setup
    # 创建模拟对象
    @client = Minitest::Mock.new
    @opts = Minitest::Mock.new

    # 设置opts的必要属性
    @opts.expect(:topic, 'persistent://public/default/test-topic')
    @opts.expect(:name, 'test-producer')

    # 创建Producer::Base实例
    @producer = PulsarSdk::Producer::Base.new(@client, @opts)
  end

  def test_initialization
    assert_instance_of(PulsarSdk::Producer::Base, @producer)
  end

  def test_disconnect?
    # 初始状态下应该断开连接
    assert(@producer.disconnect?)

    # 模拟建立连接
    @producer.instance_variable_set(:@established, true)
    refute(@producer.disconnect?)
  end

  def test_execute_with_invalid_message
    # 测试execute方法传入无效消息时的行为
    cmd = Minitar::Mock.new
    cmd.expect(:is_a?, true, [Pulsar::Proto::BaseCommand])

    # 应该抛出异常
    assert_raises(RuntimeError) do
      @producer.execute(cmd, "invalid message")
    end
  end

  def test_close_when_stopped
    # 测试当producer已停止时close方法的行为
    @producer.instance_variable_set(:@stoped, true)
    @producer.close
    # 不应该执行任何操作
  end

  def test_close_when_not_connected
    # 测试当producer未连接时close方法的行为
    @producer.instance_variable_set(:@stoped, false)
    @producer.instance_variable_set(:@established, false)

    # 模拟unbind_handler!方法
    @producer.stub(:unbind_handler!, true) do
      @producer.close
      assert(@producer.instance_variable_get(:@stoped))
    end
  end

  def teardown
    @opts.verify
    @client.verify if @client.respond_to?(:verify)
  end
end