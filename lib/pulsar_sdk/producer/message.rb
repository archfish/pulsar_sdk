module PulsarSdk
  module Producer
    class Message
      attr_reader :metadata, :message, :key
      def initialize(msg, metadata = nil)
        # TODO check metadata type
        @message, @metadata = msg, metadata

        @metadata ||= Pulsar::Proto::MessageMetadata.new
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
    end
  end
end
