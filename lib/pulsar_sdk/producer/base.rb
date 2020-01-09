module PulsarSdk
  module Producer
    class Base
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(client, opts)
        @topic = opts.topic

        @conn = client.connection(*client.lookup(opts.topic))

        @seq_generator = SeqGenerator.new(@conn.seq_generator)
        @producer_id = @seq_generator.new_producer_id

        @producer_name = [opts.name, @producer_id].join('.')

        @receipt_queue = ReceiptQueue.new

        @stoped = false

        init_producer(@topic)
      end

      def execute(cmd, msg = nil, timeout = nil)
        unless msg.nil? || msg.is_a?(PulsarSdk::Producer::Message)
          raise "msg expected a PulsarSdk::Producer::Message got #{msg.class}"
        end

        write(cmd, msg, false, timeout)
      end

      def execute_async(cmd, msg = nil)
        unless msg.nil? || msg.is_a?(PulsarSdk::Producer::Message)
          raise "msg expected a PulsarSdk::Producer::Message got #{msg.class}"
        end

        write(cmd, msg, true)
      end

      # 获取发送回执
      # TODO get receipt by sequence_id
      def receipt
        receipt_ = @receipt_queue.pop.first
        return if receipt_.nil?

        if block_given?
          yield receipt_
        end

        receipt_
      end

      def close
        return if @stoped

        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::CLOSE_PRODUCER,
          close_producer: Pulsar::Proto::CommandCloseProducer.new
        )
        execute(base_cmd)

        unbind_handler!

        @stoped = true

        @receipt_queue.close
      end

      private
      def init_producer(topic)
        bind_handler!

        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::PRODUCER,
          producer: Pulsar::Proto::CommandProducer.new(
            topic: topic
          )
        )
        execute(base_cmd)
      end

      def write(cmd, msg, *args)
        cmd.seq_generator = @seq_generator

        unless msg.nil?
          msg.producer_name = @producer_name
          msg.sequence_id = @seq_generator.new_sequence_id
        end

        result = @conn.request(set_seq_generator(cmd), filling_message(msg), *args)

        # increase sequence_id when success
        @seq_generator.new_sequence_id(false) unless msg.nil?

        result
      end

      def set_seq_generator(cmd)
        cmd.seq_generator = @seq_generator
        cmd
      end

      def filling_message(msg)
        return if msg.nil?
        msg.producer_name = @producer_name
        msg
      end

      def bind_handler!
        handler = Proc.new { |send_receipt| @receipt_queue.add(send_receipt) }
        @conn.producer_handlers.add(@producer_id, handler)
      end

      def unbind_handler!
        @conn.producer_handlers.delete(@producer_id)
        true
      end
    end
    class ReceiptQueue < ::PulsarSdk::Tweaks::TimeoutQueue; end

    # NOTE keep producer_id and sequence_id static
    class SeqGenerator
      def initialize(seq_g)
        @seq_g = seq_g
        @producer_id = @seq_g.new_producer_id
        @sequence_id = @seq_g.new_sequence_id
      end

      def new_producer_id
        @producer_id
      end

      def new_sequence_id(cache = true)
        return @sequence_id if cache
        @sequence_id = @seq_g.new_sequence_id
      end

      def method_missing(method)
        @seq_g.public_send(method)
      end
    end
  end
end
