module PulsarSdk
  module Tweaks
    class TimeoutQueue
      def initialize
        @mutex = Mutex.new
        @queue = []
        @received = ConditionVariable.new
        @closed = false
      end

      def add(*args)
        @mutex.synchronize do
          @queue << args
          @received.signal
        end
      end

      def clear
        @mutex.synchronize do
          @queue.clear
        end
      end

      def size
        @queue.size
      end

      # timeout 数字，单位秒
      def pop(timeout = nil)
        @mutex.synchronize do
          if timeout.nil?
            while !@closed && @queue.empty?
              @received.wait(@mutex)
            end
          elsif @queue.empty? && timeout != 0
            timeout_at = TimeX.now.to_f + timeout
            while !@closed && @queue.empty? && (res = timeout_at - TimeX.now.to_f) > 0
              @received.wait(@mutex, res)
            end
          end

          @queue.pop
        end
      end

      def close
        @closed = true
        @received.broadcast
      end
    end
  end
end
