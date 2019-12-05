module PulsarSdk
  module Client
    class Rpc
      def initialize(opts)
        raise "opts expected a PulsarSdk::Options::Connection got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Connection)

        @opts = opts

        @cnx = ::PulsarSdk::Client::ConnectionPool.new(opts)

        @producer_id = 0
        @consumer_id = 0

        @lookup_service = ::PulsarSdk::Protocol::Lookup.new(self, opts.logical_addr)
      end

      def connection(logical_addr = nil, physical_addr = nil)
        logical_addr ||= @opts.logical_addr
        @cnx.fetch(logical_addr, physical_addr)
      end

      def lookup(topic)
        @lookup_service.lookup(topic)
      end

      def request(physical_addr, logical_addr, cmd)
        connection(physical_addr, logical_addr).request(cmd, nil, true)
      end

      def request_any_broker(cmd)
        connection.request(cmd)
      end

      def close
        @cnx.close
      end

      def create_producer(opts)
        raise "opts expected a PulsarSdk::Options::Producer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Producer)
        # FIXME check if connection ready
        PulsarSdk::Producer.create(self, opts)
      end

      def subscribe(opts)
        raise "opts expected a PulsarSdk::Options::Consumer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Consumer)
        # FIXME check if connection ready
        consumer = PulsarSdk::Consumer.new(self, opts)
        consumer.set_handler!
        consumer.flow

        consumer
      end

      def create_reader(opts = {})

      end

      def topic_partitions(topic)

      end
    end
  end
end
