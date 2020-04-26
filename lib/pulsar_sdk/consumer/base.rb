module PulsarSdk
  module Consumer
    class Base
      attr_reader :consumer_id, :topic

      def initialize(client, message_tracker, opts)
        @opts = opts
        @topic = @opts.topic
        @message_tracker = message_tracker
        @client = client
      end

      def grab_cnx
        @prefetch = @opts.prefetch
        @fetched = 0
        @capacity = 0

        @conn = @client.connection(*@client.lookup(@topic))
        @established = true

        @seq_generator = SeqGenerator.new(@conn.seq_generator)

        @consumer_id = @seq_generator.new_consumer_id
        @consumer_name = @opts.name
        @subscription_name = @opts.subscription_name

        result = init_consumer
        @consumer_name = result.consumerName unless result.nil?
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

        execute(base_cmd)

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
        execute_async(base_cmd)
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
        execute(base_cmd) if @established

        remove_handler!
      end

      def execute(cmd)
        write(cmd)
      end

      def execute_async(cmd)
        write(cmd, nil, true)
      end

      private
      def write(cmd, *args)
        grab_cnx unless @established
        cmd.seq_generator = @seq_generator

        @conn.request(cmd, *args)
      end

      def bind_handler!
        handler = Proc.new do |cmd, meta_and_payload|
          cmd.nil? ? (@established = false) : @message_tracker.receive(cmd, meta_and_payload)
        end
        @conn.consumer_handlers.add(@consumer_id, handler)
      end

      def remove_handler!
        @conn.consumer_handlers.delete(@consumer_id)

        true
      end

      def init_consumer
        bind_handler!

        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::SUBSCRIBE,
          subscribe: Pulsar::Proto::CommandSubscribe.new(
            topic: @opts.topic,
            subscription: @opts.subscription_name,
            subType: @opts.subscription_type,
            consumer_name: @consumer_name,
            replicate_subscription_state: @opts.replicate_subscription_state,
            read_compacted: @opts.read_compacted
          )
        )
        result = execute(base_cmd)

        @message_tracker.add_consumer(self)

        result.consumerStatsResponse
      end

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
