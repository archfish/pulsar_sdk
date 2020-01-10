module PulsarSdk
  module Consumer
    class Base
      attr_reader :consumer_id, :topic

      def initialize(client, message_tracker, opts)
        @topic = opts.topic

        @prefetch = opts.prefetch
        @fetched = 0
        @capacity = 0

        @listen_wait = opts.listen_wait

        @conn = client.connection(*client.lookup(@topic))
        @seq_generator = SeqGenerator.new(@conn.seq_generator)

        @consumer_id = @seq_generator.new_consumer_id
        @consumer_name = opts.name
        @subscription_name = opts.subscription_name

        @message_tracker = message_tracker

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
        handler = Proc.new { |cmd, meta_and_payload| @message_tracker.receive(cmd, meta_and_payload) }
        @conn.consumer_handlers.add(@consumer_id, handler)
      end

      def remove_handler!
        @conn.consumer_handlers.delete(@consumer_id)
        true
      end

      def increase_fetched(n = 1)
        @fetched += n
      end

      def flow
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::FLOW,
          flow: Pulsar::Proto::CommandFlow.new(
            messagePermits: @prefetch
          )
        )

        sync_request(base_cmd)

        @capacity += @prefetch
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

      def flow_if_need
        return if @capacity > 0 && [@prefetch / 2, 1].max.ceil < (@capacity - @fetched)
        flow
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

      def async_request(cmd)
        cmd.seq_generator = @seq_generator

        @conn.request(cmd)
      end

      def sync_request(cmd)
        cmd.seq_generator = @seq_generator

        @conn.request(cmd, nil, true)
      end

      private

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
end
