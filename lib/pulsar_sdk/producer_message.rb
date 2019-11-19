module PulsarSdk
  class ProducerMessage
    attr_reader :metadata, :message
    def initialize(msg, metadata)
      # TODO check metadata type
      @message, @metadata = msg, metadata
    end

    def binary_string
      @message.bytes.pack('C*')
    end
  end
end
