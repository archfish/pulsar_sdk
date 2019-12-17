module PulsarSdk
  module Producer
    class Message
      prepend ::PulsarSdk::Tweaks::CleanInspect

      attr_reader :metadata, :message, :key

      def initialize(msg, metadata = nil)
        # TODO check metadata type
        @message, @metadata = msg, metadata
        @metadata ||= Pulsar::Proto::MessageMetadata.new

        # msg must convet to string
        json_encode! unless @message.is_a?(String)

        publish_time = @metadata.publish_time
        @metadata.publish_time = publish_time.zero? ? TimeX.now.timestamp : publish_time
      end

      def producer_name=(v)
        @metadata.producer_name = v
      end

      def sequence_id=(v)
        @metadata.sequence_id = v
      end

      def binary_string
        @message.bytes.pack('C*')
      end

      private
      def json_encode!
        @message = @message.respond_to?(:to_json) ? @message.to_json : JSON.dump(@message)
        @metadata.properties << Pulsar::Proto::KeyValue.new(key: 'Content-Type', value: 'application/json; charset=utf-8')
      end
    end
  end
end
