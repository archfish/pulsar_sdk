module PulsarSdk
  module Protocol
    class Structure
      prepend ::PulsarSdk::Tweaks::CleanInspect

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
          checksum_bytes = read_checksum
          metadata = read_metadata
          payload = read_remaining || ""

          # 校验和验证
          meta_payload = [@buff[@readed - (metadata.to_proto.size + payload.size + METADATA_SIZE_LEN)..-1]].pack('a*')
          calculated_checksum = calculate_checksum(meta_payload)
          expected_checksum = checksum_bytes.unpack('N').first

          if calculated_checksum != expected_checksum
            raise Pulsar::Proto::CommandError.new(
              error: Pulsar::Proto::ServerError::ChecksumError,
              message: "Checksum mismatch: expected #{expected_checksum}, got #{calculated_checksum}"
            )
          end
        else
          rewind(MAGIC_NUMBER_LEN)
          metadata = read_metadata
        end

        msg = read_remaining

        # NOTE 同为Ruby SDK时可以根据Content-Type预先还原
        # 复杂类型依旧为string，需要特别注意
        metadata.properties.each do |x|
          next unless x.key.to_s =~ /Content-Type/i
          next unless x.value.to_s =~ /json/i
          PulsarSdk.logger.info("#{self.class}::#{__method__}"){"Found json encode remark, parse JSON mesaage!"}
          msg = JSON.parse(msg)
        end

        message.assign_attributes(
          publish_time: metadata.publish_time,
          event_time: metadata.event_time,
          partition_key: metadata.partition_key,
          properties: metadata.properties,
          payload: msg
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

      def calculate_checksum(bytes)
        crc = Digest::CRC32c.new
        crc << bytes
        crc.checksum
      end

      def read(size, unpack = nil)
        bytes = @buff[@readed..(@readed + size - 1)]
        @readed += size

        return bytes if unpack.nil? || bytes.nil?

        bytes.unpack(unpack).first
      end
    end
  end
end