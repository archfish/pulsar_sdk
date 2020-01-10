module PulsarSdk
  module Consumer
    class Manager
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(client, opts)
        raise "client expected a PulsarSdk::Client::Rpc got #{client.class}" unless client.is_a?(PulsarSdk::Client::Rpc)
        raise "opts expected a PulsarSdk::Options::Consumer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Consumer)

        @topic = opts.topic

        @message_tracker = ::PulsarSdk::Consumer::MessageTracker.new(opts.redelivery_delay)

        @consumers = init_consumer_by(client, opts)

        @stoped = false
      end

      # NOTE some topic maybe have large permits if there is no message
      def flow
        @consumers.each(&:flow_if_need)
      end

      # NOTE all consumers has same name
      def subscription
        @consumers.find(&:subscription)
      end

      def unsubscribe
        @consumers.each(&:unsubscribe)
      end

      # if timeout is nil wait until get message
      def receive(timeout = nil)
        @message_tracker.shift(timeout)
      end

      def listen(autoack = false)
        raise 'listen require passing a block!!' if !block_given?

        loop do
          return if @stoped
          @consumers.each(&:flow_if_need)

          cmd, msg = receive(@listen_wait)
          return if msg.nil?

          result = yield cmd, msg

          if autoack && result == false
            msg.nack
            next
          end

          msg.ack if autoack
        end
      end

      def close
        return unless @stoped
        @consumers.each(&:close)
        @stoped = true

        @message_tracker.close
      end

      private
      def init_consumer_by(client, opts)
        topics = []

        case
        when !opts.topic.nil?
          PulsarSdk.logger.debug("#{__method__}:single topic"){opts.topic}

          topics << ::PulsarSdk::Protocol::Topic.parse(opts.topic).to_s
        when !Array(opts.topics).size.zero?
          PulsarSdk.logger.debug("#{__method__}:multiple topics"){opts.topics}

          opts.topics.each do |topic|
            topics << ::PulsarSdk::Protocol::Topic.parse(topic).to_s
          end
        when !opts.topics_pattern.nil?
          PulsarSdk.logger.debug("#{__method__}:pattern topic"){opts.topics_pattern}

          tn = ::PulsarSdk::Protocol::Topic.parse(opts.topics_pattern)
          pattern = Regexp.compile(tn.topic)
          client.namespace_topics(tn.namespace).each do |topic|
            topics << topic if pattern.match(topic)
          end
        else
          raise 'You must provide one topic by 「topic」or「topics」or「topics_pattern」'
        end

        PulsarSdk.logger.debug("#{__method__}:topics to initialize"){topics}

        topics.flat_map do |topic|
          partition_topics = client.partition_topics(topic)

          partition_topics.map do |x|
            opts_ = opts.dup

            opts_.topic = x
            PulsarSdk::Consumer::Base.new(client, @message_tracker, opts_).tap do |consumer|
              consumer.set_handler!
              @message_tracker.add_consumer(consumer)
            end
          end
        end
      end
    end
  end
end
