module PulsarSdk
  module Tweaks
    module AssignAttributes
      def initialize(opts = {})
        set_default

        assign_attributes(opts)
      end

      def assign_attributes(opts)
        opts.each do |k, v|
          method = "#{k}="
          next unless self.respond_to?(method)
          self.public_send method, v
        end
      end

      private
      def set_default; end
    end
  end
end
