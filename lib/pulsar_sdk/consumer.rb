module PulsarSdk
  class Consumer
    def initialize(client, opts)
      raise "client expected a PulsarSdk::Client::Rpc got #{client.class}" unless client.is_a?(PulsarSdk::Client::Rpc)
      raise "opts expected a PulsarSdk::Options::Consumer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Consumer)

      @topic = opts.topic
      @conn = client.connection(*client.lookup(@topic))
      @seq_generator = SeqGenerator.new(@conn.seq_generator)

      @consumer_id = @seq_generator.new_consumer_id
      @consumer_name = opts.name
      @subscription_name = opts.subscription_name

      @received_message = ReceivedQueue.new

      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::SUBSCRIBE,
        subscribe: Pulsar::Proto::CommandSubscribe.new(
          topic: @topic,
          subscription: opts.subscription_name,
          subType: opts.subscription_type,
          consumer_name: @consumer_name
        )
      )
      sync_request(base_cmd)
    end

    def set_handler!
      handler = Proc.new { |cmd, meta_and_payload| @received_message.add(cmd, meta_and_payload) }
      @conn.consumer_handlers.add(@consumer_id, handler)
    end

    def remove_handler!
      @conn.consumer_handlers.delete(@consumer_id)
      true
    end

    def flow(batch = 1000)
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::FLOW,
        flow: Pulsar::Proto::CommandFlow.new(
          messagePermits: batch
        )
      )

      sync_request(base_cmd)
    end

    def subscription
      @subscription_name
    end

    def unsubscribe
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::UNSUBSCRIBE,
        unsubscribe: Pulsar::Proto::CommandUnsubscribe.new
      )
      async_request(base_cmd)
    end

    def receive
      cmd, meta_and_payload = @received_message.pop

      decoder = PulsarSdk::Protocol::Structure.new(meta_and_payload)
      message = decoder.decode
      message.assign_attributes(
        message_id: cmd.message&.message_id,
        consumer_id: cmd.message&.consumer_id,
        topic: @topic,
        command_handler: command_handler
      )

      [cmd, message]
    end

    def close
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::CLOSE_CONSUMER,
        close_consumer: Pulsar::Proto::CommandCloseConsumer.new(
          consumer_id: @consumer_id
        )
      )
      sync_request(base_cmd)

      remove_handler!
    end

    def command_handler
      Proc.new{|cmd| async_request(cmd)}
    end

    private
    def async_request(cmd)
      cmd.seq_generator = @seq_generator
      @received_message.clear if cmd.typeof_redeliver_unacknowledged_messages?

      @conn.request(cmd)
    end

    def sync_request(cmd)
      cmd.seq_generator = @seq_generator
      @conn.request(cmd, nil, true)
    end

    class ReceivedQueue < PulsarSdk::Tweaks::TimeoutQueue; end

    # NOTE keep consumer_id and sequence_id static
    class SeqGenerator
      def initialize(seq_g)
        @seq_g = seq_g
        @consumer_id = @seq_g.new_consumer_id
      end

      def new_consumer_id
        @consumer_id
      end

      def method_missing(method)
        @seq_g.public_send(method)
      end
    end
  end
end
