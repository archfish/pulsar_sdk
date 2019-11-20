module PulsarSdk
  class Client
    attr_reader :conn

    def initialize(opts)
      raise "opts expected a PulsarSdk::Options::Client got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Client)

      # FIXME should use connection pool
      @conn = PulsarSdk::Connection.new(opts.url)
      @conn.connection_timeout = opts.connection_timeout
      @conn.operation_timeout = opts.operation_timeout
      @conn.start

      @request_id = 0
      @producer_id = 0
      @consumer_id = 0
    end

    def new_request_id
      @request_id += 1
    end

    def new_producer_id
      @producer_id += 1
    end

    def new_consumer_id
      @consumer_id += 1
    end

    def close
      @conn.close
    end

    def create_producer(opts)
      raise "opts expected a PulsarSdk::Options::Producer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Producer)
      # FIXME check if connection ready
      producer = PulsarSdk::Producer.new(self, opts)
      producer.set_handler!
      producer
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

    def lookup

    end

  end
end
