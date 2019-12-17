module PulsarSdk
  module Tweaks
    module BaseCommand
      attr_accessor :seq_generator

      # add some helper
      #   def set_request_id
      #   def set_consumer_id
      #   def set_producer_id
      #   def set_sequence_id
      #   def get_request_id
      #   def get_consumer_id
      #   def get_producer_id
      #   def get_sequence_id
      # NOTE before use setter must set `seq_generator`
      [:request_id, :consumer_id, :producer_id, :sequence_id].each do |x|
        define_method "set_#{x}" do
          if self.seq_generator.nil?
            PulsarSdk.logger.warn(__method__){"seq_generator was not set!! action has no effect"}
            return
          end

          method_ = "#{x}="
          current_id = nil
          attribute_names.each do |name|
            next unless self[name].respond_to?(method_)

            # NOTE keep same id name with same value, like sequence_id
            current_id ||= self.seq_generator.public_send("new_#{x}")

            self[name].public_send(method_, current_id)
          end

          current_id
        end

        define_method "get_#{x}" do
          attribute_names.each do |name|
            next unless self[name].respond_to?(x)
            return self[name][x.to_s]
          end
          nil
        end
      end

      Pulsar::Proto::BaseCommand::Type.constants.each do |x|
        method = "typeof_#{x.to_s.downcase}?"
        define_method method do
          self.type == x
        end
      end

      private
      def attribute_names
        self.class.descriptor.map(&:name)
      end
    end
  end
end
