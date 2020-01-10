module PulsarSdk
  module Consumer
    class MessageTracker

      class ReceivedQueue < PulsarSdk::Tweaks::TimeoutQueue; end
      class NackQueue < PulsarSdk::Tweaks::BinaryHeap; end

      def initialize(redelivery_delay)
        @redelivery_delay = redelivery_delay
        @received_message = ReceivedQueue.new
        @acknowledge_message = NackQueue.new {|parent, child| child[:ack_at] <=> parent[:ack_at] }
        @consumers = {}

        @tracker = track
      end

      def add_consumer(consumer)
        @consumers[consumer.consumer_id] = consumer
      end

      def receive(*args)
        @received_message.add(*args)
      end

      def shift(timeout)
        cmd, meta_and_payload = @received_message.pop(timeout)

        return if cmd.nil?

        message = PulsarSdk::Protocol::Structure.new(meta_and_payload).decode

        consumer_id = cmd.message&.consumer_id
        real_consumer = @consumers[consumer_id]

        message.assign_attributes(
          message_id: cmd.message&.message_id,
          consumer_id: consumer_id,
          topic: real_consumer&.topic,
          ack_handler: ack_handler
        )

        real_consumer&.increase_fetched

        [cmd, message]
      end

      def close
        @received_message.close
      end

      private
      def track
        Thread.new do
          loop do
            while item = @acknowledge_message.top
              break if item[:ack_at] > Process.clock_gettime(Process::CLOCK_MONOTONIC)
              begin
                PulsarSdk.logger.debug('acknowledge message'){"#{Process.clock_gettime(Process::CLOCK_MONOTONIC)}: #{item[:cmd].type} --> #{item[:ack_at]}"}
                async_request(item[:cmd])
                @acknowledge_message.shift
              rescue => exp
                PulsarSdk.logger.error('Error occur when acknowledge message'){exp}
                PulsarSdk.logger.error('Error occur when acknowledge message'){item}
              end
            end
            sleep(1)
          end
        end
      end

      def ack_handler
        Proc.new do |cmd|
          current_clock = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          ack_at = cmd.typeof_ack? ? (current_clock - 1) : (current_clock + @redelivery_delay.to_i)
          @acknowledge_message.insert(cmd: cmd, ack_at: ack_at)
        end
      end

      def async_request(cmd)
        consumer = @consumers[cmd.get_consumer_id]

        consumer.async_request(cmd)
      end
    end
  end
end
