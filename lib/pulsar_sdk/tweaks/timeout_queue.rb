module PulsarSdk
  module Tweaks
    class TimeoutQueue
      def initialize
        @mutex = Mutex.new
        @receive_queue = Queue.new
        @received = ConditionVariable.new
      end

      def add(*args)
        @mutex.synchronize do
          @receive_queue << args
          @received.signal
        end
      end

      def clear
        @mutex.synchronize do
          @receive_queue.clear
        end
      end

      # timeout 数字，单位秒
      def pop(timeout = nil)
        @mutex.synchronize do
          if timeout.nil?
            while @receive_queue.empty?
              @received.wait(@mutex)
            end
          elsif @receive_queue.empty? && timeout != 0
            timeout_at = Time.now.to_f + timeout
            while @receive_queue.empty? && (res = timeout_at - Time.now.to_f) > 0
              @received.wait(@mutex, res)
            end
          end
          @receive_queue.pop
        end
      end
    end
  end
end
