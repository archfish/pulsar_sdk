module PulsarSdk
  module Tweaks
    module AssignAttributes
      def initialize(opts = {})
        set_default

        assign_attributes(opts)

        remove_empty_instance_variables!
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

      def remove_empty_instance_variables!
        instance_variables.each do |x|
          remove_instance_variable(x) if instance_variable_get(x).nil?
        end
      end
    end
  end
end
