module PulsarSdk
  module Producer
    class Manager
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(client, opts)
        @producers = init_producer_by(client, opts)
        @router = opts.router
      end

      def execute(cmd, msg = nil, timeout = nil)
        raise "cmd expected a Pulsar::Proto::BaseCommand got #{cmd.class}" unless cmd.is_a?(Pulsar::Proto::BaseCommand)
        real_producer(msg).execute(cmd, msg, timeout)
      end

      def execute_async(cmd, msg = nil)
        raise "cmd expected a Pulsar::Proto::BaseCommand got #{cmd.class}" unless cmd.is_a?(Pulsar::Proto::BaseCommand)
        real_producer(msg).execute_async(cmd, msg)
      end

      def real_producer(msg)
        return @producers[0] if msg.nil?
        @producers[@router.route(msg.key, @producers.size)]
      end

      def close
        @producers.each(&:close)
      end

      private
      def init_producer_by(client, opts)
        opts = opts.dup

        topics = PulsarSdk::Protocol::PartitionedTopic.new(client, opts.topic).partitions
        topics.map do |topic|
          opts.topic = topic
          PulsarSdk::Producer::Partition.new(client, opts)
        end
      end
    end
  end
end
