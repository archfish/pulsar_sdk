module PulsarSdk
  module Consumer
    class Manager
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(client, opts)
        raise "client expected a PulsarSdk::Client::Rpc got #{client.class}" unless client.is_a?(PulsarSdk::Client::Rpc)
        raise "opts expected a PulsarSdk::Options::Consumer got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Consumer)

        @topic = opts.topic

        @listen_wait = opts.listen_wait

        @message_tracker = ::PulsarSdk::Consumer::MessageTracker.new(opts.redelivery_delay)

        @consumers = init_consumer_by(client, opts)

        @stoped = false
      end

      # NOTE some topic maybe have large permits if there is no message
      def flow
        ensure_connection
        @consumers.each(&:flow_if_need)
      end

      # NOTE all consumers has same name
      def subscription
        ensure_connection
        @consumers.find(&:subscription)
      end

      def unsubscribe
        ensure_connection
        @consumers.each(&:unsubscribe)
      end

      # if timeout is nil wait until get message
      def receive(timeout = nil)
        ensure_connection
        @message_tracker.shift(timeout)
      end

      def listen(autoack = false)
        raise 'listen require passing a block!!' if !block_given?
        ensure_connection

        loop do
          begin
            # 检查是否应该停止监听
            return if @stoped

            # 请求更多消息
            flow

            # 接收消息
            cmd, msg = receive(@listen_wait)

            # 处理超时情况 - 如果设置了等待时间但没有收到消息，继续循环
            if msg.nil? && !@listen_wait.nil?
              next
            end

            # 如果没有设置等待时间且没有消息，或者消费者被停止，则退出
            return if msg.nil? || @stoped

            begin
              result = yield cmd, msg
            rescue => e
              PulsarSdk.logger.error("Consumer::Manager#listen") { "Error in listen block: #{e}" }
              # 即使处理消息时出错，也要继续监听
              next
            end

            if autoack && result == false
              msg.nack
              next
            end

            msg.ack if autoack
          rescue => e
            PulsarSdk.logger.error("Consumer::Manager#listen") { "Error in listen loop: #{e}" }
            # 在主循环中捕获异常，确保监听可以继续
            # 可以选择短暂休眠以避免忙等待
            sleep(0.1) rescue nil
            # 确保连接有效
            ensure_connection
          end
        end
      end

      def close
        PulsarSdk.logger.debug(__method__){"current @stoped #{@stoped} close now!"}
        return if @stoped

        # 设置停止标志，使listen循环能够正常退出
        @stoped = true

        @consumers.each(&:close)
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
          pattern = Regexp.compile(tn.topic == '*' ? '^*' : tn.topic)
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
              consumer.grab_cnx
            end
          end
        end
      end

      def ensure_connection
        @consumers.each do |consumer|
          next unless consumer.disconnect?
          PulsarSdk.logger.warn('PulsarSdk::Consumer::Manager#ensure_connection'){
            "connection closed! reconnect now! #{consumer.inspect}"
          }
          begin
            consumer.grab_cnx
          rescue => e
            PulsarSdk.logger.error('PulsarSdk::Consumer::Manager#ensure_connection') {
              "Failed to reconnect consumer: #{e}"
            }
          end
        end
      end
    end
  end
end