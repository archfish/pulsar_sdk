require 'digest/crc32c'

module PulsarSdk
  module Protocol
    class Frame
      # 预留4byte存放帧长度
      PREPENDED_SIZE = 4
      CHECKSUM_SIZE = 4
      MAGIC_NUMBER = [0x0e, 0x01].pack('C*').freeze

      def self.encode(command, message = nil)
        raise "command MUST be Pulsar::Proto::BaseCommand but got #{command.class}" unless command.is_a?(Pulsar::Proto::BaseCommand)

        pb_cmd = command.to_proto

        # 非发送消息帧
        return encode_command(pb_cmd) if message.nil?

        # 消息发送帧
        # [TOTAL_SIZE] [CMD_SIZE] [CMD] [MAGIC_NUMBER] [CHECKSUM] [METADATA_SIZE] [METADATA] [PAYLOAD]
        raise "message MUST be PulsarSdk::Producer::Message but got #{message.class}" unless message.is_a?(PulsarSdk::Producer::Message)

        metadata = message.metadata
        pb_meta = metadata.to_proto

        meta_payload = binary(pb_meta.size) + pb_meta + message.binary_string
        checksum = crc32(meta_payload)

        total_size = PREPENDED_SIZE + pb_cmd.size + MAGIC_NUMBER.size + CHECKSUM_SIZE + meta_payload.size

        binary(total_size, pb_cmd.size) + pb_cmd + MAGIC_NUMBER + binary(checksum) + meta_payload
      end

      def self.encode_command(pb_cmd)
        binary(pb_cmd.size + PREPENDED_SIZE, pb_cmd.size) + pb_cmd
      end

      def self.decode(byte)
        Pulsar::Proto::BaseCommand.decode(byte)
      end

      def self.binary(*obj)
        obj.map { |x| Array(x).pack('N') }.join
      end

      def self.crc32(bytes)
        crc = Digest::CRC32c.new
        crc << bytes
        crc.checksum
      end
    end
  end
end
