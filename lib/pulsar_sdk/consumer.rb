module PulsarSdk
  class Consumer
    def initialize(client, opts)
      raise "client expected a PulsarSdk::Client got #{client.class}" unless client.is_a?(PulsarSdk::Client)
      raise "opts expected a PulsarSdk::Options::Consumer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Consumer)

      @topic = opts.topic
      @conn = client.establish(*client.lookup_service.lookup(@topic))
      @consumer_id = client.new_consumer_id
      @consumer_name = opts.name
      @subscription_name = opts.subscription_name

      @received_message = ReceivedQueue.new

      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::SUBSCRIBE,
        subscribe: Pulsar::Proto::CommandSubscribe.new(
          topic: @topic,
          subscription: opts.subscription_name,
          subType: opts.subscription_type,
          consumer_id: @consumer_id,
          request_id: new_request_id,
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
          consumer_id: @consumer_id,
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
        unsubscribe: Pulsar::Proto::CommandUnsubscribe.new(
          consumer_id: @consumer_id,
          request_id: new_request_id
        )
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
          consumer_id: @consumer_id,
          request_id: new_request_id
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
      @received_message.clear if cmd.typeof_redeliver_unacknowledged_messages?

      @conn.async_request(cmd)
    end

    def sync_request(cmd)
      @conn.async_request(cmd)
    end

    def new_request_id
      @conn.new_request_id
    end

    class ReceivedQueue < PulsarSdk::Tweaks::TimeoutQueue; end
  end
end
