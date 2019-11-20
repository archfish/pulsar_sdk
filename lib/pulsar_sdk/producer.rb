module PulsarSdk
  class Producer
    def initialize(client, opts)
      raise "client expected a PulsarSdk::Client got #{client.class}" unless client.is_a?(PulsarSdk::Client)
      raise "opts expected a PulsarSdk::Options::Producer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Producer)

      @conn = client.establish(*client.lookup_service.lookup(opts.topic))
      @producer_id = client.new_producer_id
      @producer_name = [opts.name, @producer_id].join('.')
      @receipt_queue = ReceiptQueue.new

      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::PRODUCER,
        producer: Pulsar::Proto::CommandProducer.new(
          topic: opts.topic,
          producer_id: @producer_id,
          request_id: new_request_id,
          producer_name: @producer_name
        )
      )
      sync_request(base_cmd)
    end

    def set_handler!
      handler = Proc.new { |send_receipt| @receipt_queue.add(send_receipt) }
      @conn.producer_handlers.add(@producer_id, handler)
    end

    def remove_handler!
      @conn.producer_handlers.del(@producer_id)
      true
    end

    # 发送消息
    #   msg String
    def despatch(msg)
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::SEND,
        send: Pulsar::Proto::CommandSend.new(
          sequence_id: 0,
          num_messages: 1
        )
      )

      p_msg = PulsarSdk::ProducerMessage.new(
        msg,
        Pulsar::Proto::MessageMetadata.new(
          sequence_id: 0,
          publish_time: (Time.now.to_f * 1000).to_i
        )
      )

      despatch_ex(base_cmd, p_msg)
    end

    # 高级发送功能
    def despatch_ex(cmd, msg)
      raise "cmd expected a Pulsar::Proto::BaseCommand got #{cmd.class}" unless cmd.is_a?(Pulsar::Proto::BaseCommand)
      raise "msg expected a PulsarSdk::ProducerMessage got #{msg.class}" unless msg.is_a?(PulsarSdk::ProducerMessage)

      # only work with same producer_id, or get warn 「Producer had already been closed」
      cmd['send'].producer_id = @producer_id
      msg.metadata.producer_name = @producer_name if msg.metadata

      frame = PulsarSdk::Protocol::Frame.encode(cmd, msg)

      # FIXME 清空回执队列，等待新的回执，可能在回调阶段做更合适
      @receipt_queue.clear

      send_message(frame)
    end

    # 获取发送回执
    def receipt
      receipt_ = @receipt_queue.pop.first

      if block_given?
        yield receipt_
      end

      receipt_
    end

    def close
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::CLOSE_PRODUCER,
        close_producer: Pulsar::Proto::CommandCloseProducer.new(
          producer_id: @producer_id,
          request_id: new_request_id
        )
      )
      sync_request(base_cmd)

      remove_handler!
    end

    private
    def async_request(cmd)
      @conn.async_request(cmd)
    end

    def sync_request(cmd)
      @conn.sync_request(cmd)
    end

    def send_message(frame)
      @conn.write(frame)
    end

    def new_request_id
      @conn.new_request_id
    end

    class ReceiptQueue < ::PulsarSdk::Tweaks::TimeoutQueue; end
  end
end
