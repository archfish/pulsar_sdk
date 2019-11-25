module PulsarSdk
  module Tweaks
    class WaitMap
      def initialize
        @mutex = Mutex.new
        @responses = {}

        @wait = {}
      end

      def add(id, state)
        @mutex.synchronize do
          @responses[id] = state
          _, signal = @wait[id]
          signal.signal unless signal.nil?
        end
      end

      def clear
        @mutex.synchronize do
          @responses = {}
        end
      end

      # 不会删除元素
      def find(id)
        @mutex.synchronize do
          @responses[id]
        end
      end

      def delete(id, timeout = nil)
        mutex, signal = []

        @mutex.synchronize do
          return @responses.delete(id) if @responses.has_key?(id)

          @wait[id] ||= [Mutex.new, ConditionVariable.new]
          mutex, signal = @wait[id]
        end

        mutex.synchronize do
          if timeout.nil?
            while @responses.empty?
              signal.wait(mutex)
            end
          elsif @responses.empty? && timeout != 0
            timeout_at = TimeX.now.to_f + timeout
            while @responses.empty? && (res = timeout_at - TimeX.now.to_f) > 0
              signal.wait(mutex, res)
            end
          end
        end

        @mutex.synchronize do
          @responses.delete id
        end
      end
    end
  end
end
