module PulsarSdk
  module Protocol
    class Structure
      # [MAGIC_NUMBER] [CHECKSUM] [METADATA_SIZE] [METADATA] [PAYLOAD]
      MAGIC_NUMBER = [0x0e, 0x01].pack('C*').freeze
      MAGIC_NUMBER_LEN = MAGIC_NUMBER.size
      CHECKSUM_LEN = 4
      METADATA_SIZE_LEN = 4

      def initialize(buff)
        @buff = buff
        rewind
      end

      def decode
        metadata = nil

        message = PulsarSdk::Protocol::Message.new

        mn_bytes = read_magic_number
        if mn_bytes == MAGIC_NUMBER
          _checksum = read_checksum
          # TODO 可能需要校验一下，防止错误消息
          metadata = read_metadata
        else
          rewind(MAGIC_NUMBER_LEN)
          metadata = read_metadata
        end

        message.assign_attributes(
          publish_time: metadata.publish_time,
          event_time: metadata.event_time,
          partition_key: metadata.partition_key,
          properties: metadata.properties,
          payload: read_remaining
        )

        message
      end

      # 回退若干字节，方便处理非连续段
      def rewind(x = nil)
        return @readed = 0 if x.nil?

        @readed -= x
      end

      def read_magic_number
        read(MAGIC_NUMBER_LEN)
      end

      # crc32
      def read_checksum
        read(CHECKSUM_LEN)
      end

      def read_metadata
        metadata_size = read(METADATA_SIZE_LEN, 'N')
        metadata_bytes = read(metadata_size)
        Pulsar::Proto::MessageMetadata.decode(metadata_bytes)
      end

      def read_remaining
        payload_size = @buff.size - @readed
        return if payload_size <= 0
        read(payload_size)
      end

      private
      def read(size, unpack = nil)
        bytes = @buff[@readed..(@readed + size - 1)]
        @readed += size

        return bytes if unpack.nil? || bytes.nil?

        bytes.unpack(unpack).first
      end
    end
  end
end
