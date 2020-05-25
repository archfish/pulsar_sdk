module PulsarSdk
  module Producer
    class Manager
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(client, opts)
        @topic = opts.topic
        @producers = init_producer_by(client, opts)
        @router = opts.router
      end

      def execute(cmd, msg = nil, timeout = nil)
        raise "cmd expected a Pulsar::Proto::BaseCommand got #{cmd.class}" unless cmd.is_a?(Pulsar::Proto::BaseCommand)
        real_producer(msg) do |producer|
          producer.execute(cmd, msg, timeout)
        end
      end

      def execute_async(cmd, msg = nil)
        raise "cmd expected a Pulsar::Proto::BaseCommand got #{cmd.class}" unless cmd.is_a?(Pulsar::Proto::BaseCommand)
        real_producer(msg) do |producer|
          producer.execute_async(cmd, msg)
        end
      end

      def real_producer(msg, &block)
        if @producers.size.zero?
          PulsarSdk.logger.warn(__method__){"There is no available producer for topic: 「#{@topic}」, skipping action!"}
          return
        end

        ensure_connection

        route_index = msg.nil? ? 0 : @router.route(msg.key, @producers.size)

        yield @producers[route_index]
      end

      def close
        @producers.each(&:close)
      end

      private
      def init_producer_by(client, opts)
        opts = opts.dup

        topics = client.partition_topics(@topic)
        topics.map do |topic|
          opts.topic = topic
          PulsarSdk::Producer::Base.new(client, opts).tap do |base|
            base.grab_cnx
          end
        end
      end

      def ensure_connection
        @producers.each do |producer|
          next unless producer.disconnect?
          PulsarSdk.logger.warn('PulsarSdk::Producer::Manager#ensure_connection'){
            "connection closed! reconnect now! #{producer.inspect}"
          }
          producer.grab_cnx
        end
      end
    end
  end
end
