module PulsarSdk
  module Tweaks
    module BaseCommand
      Pulsar::Proto::BaseCommand::Type.constants.each do |x|
        method = "typeof_#{x.to_s.downcase}?"
        define_method method do
          self.type == x
        end
      end

      def get_request_id
        case
        when typeof_success?
          self.success.request_id
        when typeof_producer_success?
          self.producer_success.request_id
        when typeof_lookup_response?
          self.lookupTopicResponse.request_id
        when typeof_get_last_message_id_response?
          self.getLastMessageIdResponse.request_id
        end
      end

      def get_consumer_id
        return unless typeof_message?
        message.consumer_id
      end
    end
  end
end
