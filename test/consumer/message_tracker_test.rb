require 'minitest/autorun'
require 'pulsar_sdk'

class TestMessageTracker < Minitest::Test
  def setup
    @redelivery_delay = 10
    @message_tracker = PulsarSdk::Consumer::MessageTracker.new(@redelivery_delay)
  end

  def test_initialization
    assert_instance_of(PulsarSdk::Consumer::MessageTracker, @message_tracker)
    assert_instance_of(PulsarSdk::Consumer::MessageTracker::AckHandler, @message_tracker.send(:ack_handler))
  end

  def test_ack_handler_has_call_method
    ack_handler = @message_tracker.send(:ack_handler)
    assert_respond_to(ack_handler, :call)
  end

  def test_ack_handler_has_call_sync_method
    ack_handler = @message_tracker.send(:ack_handler)
    assert_respond_to(ack_handler, :call_sync)
  end

  def test_ack_handler_call_sync_accepts_timeout_parameter
    ack_handler = @message_tracker.send(:ack_handler)

    # 创建一个模拟命令对象
    cmd = Minitest::Mock.new
    cmd.expect(:get_consumer_id, 1)

    # 创建一个模拟消费者
    consumer = Minitest::Mock.new
    consumer.expect(:execute, true, [cmd, 3])  # 验证超时参数被传递

    # 添加消费者到message_tracker
    @message_tracker.add_consumer(consumer)

    # 调用call_sync方法并传入超时参数
    result = ack_handler.call_sync(cmd, 3)

    assert_equal(true, result)

    # 验证mock对象
    cmd.verify
    consumer.verify
  end

  def test_add_consumer
    consumer = Minitest::Mock.new
    consumer.expect(:consumer_id, 1)

    @message_tracker.add_consumer(consumer)

    # 验证消费者已被添加
    consumers = @message_tracker.instance_variable_get(:@consumers)
    assert_equal(1, consumers.size)
    assert_equal(consumer, consumers[1])

    consumer.verify
  end

  def test_receive
    received_queue = Minitest::Mock.new
    received_queue.expect(:add, nil, ["test_arg"])

    # 替换实例变量以进行测试
    @message_tracker.instance_variable_set(:@received_message, received_queue)

    @message_tracker.receive("test_arg")

    received_queue.verify
  end

  def teardown
    # 清理资源
    @message_tracker.close rescue nil
  end
end