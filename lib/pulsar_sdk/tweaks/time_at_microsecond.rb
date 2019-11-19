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
          Time.at(second, micro * 1000)
        end
      end
    end
  end
end
