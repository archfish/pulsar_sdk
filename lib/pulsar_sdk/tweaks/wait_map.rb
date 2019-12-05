module PulsarSdk
  module Tweaks
    class WaitMap
      def initialize
        @mutex = Mutex.new
        @map = {}

        @wait = {}
      end

      def add(id, value)
        @mutex.synchronize do
          @map[id] = value
          _, signal = @wait[id]
          signal.signal unless signal.nil?
        end
        value
      end

      def clear
        @mutex.synchronize do
          @map.each {|k, v| yield k, v} if block_given?

          @map = {}
        end
        true
      end

      def each(&block)
        @mutex.synchronize do
          @map.each {|k, v| yield k, v}
        end
      end

      # 不会删除元素
      def find(id)
        @mutex.synchronize do
          @map[id]
        end
      end

      def delete(id, timeout = nil)
        mutex, signal = []

        @mutex.synchronize do
          return @map.delete(id) if @map.has_key?(id)

          @wait[id] ||= [Mutex.new, ConditionVariable.new]
          mutex, signal = @wait[id]
        end

        mutex.synchronize do
          if timeout.nil?
            while @map.empty?
              signal.wait(mutex)
            end
          elsif @map.empty? && timeout != 0
            timeout_at = TimeX.now.to_f + timeout
            while @map.empty? && (res = timeout_at - TimeX.now.to_f) > 0
              signal.wait(mutex, res)
            end
          end
        end

        @mutex.synchronize do
          @map.delete id
        end
      end
    end
  end
end
