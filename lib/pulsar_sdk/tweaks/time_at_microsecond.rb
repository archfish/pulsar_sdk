module PulsarSdk
  module Tweaks
    module TimeAtMicrosecond
      def self.prepended(base)
        class << base
          prepend ClassMethods
        end
      end

      def timestamp
        (self.to_f * 1000).floor
      end

      module ClassMethods
        def at_timestamp(v)
          second, micro = v.divmod(1000)
          self.at(second, micro * 1000)
        end
      end
    end
  end
end

# 扩展默认时间方法，增加毫秒时间戳相关处理
class TimeX < Time
  prepend PulsarSdk::Tweaks::TimeAtMicrosecond
end
