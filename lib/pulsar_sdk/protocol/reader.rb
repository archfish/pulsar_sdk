module PulsarSdk
  module Protocol
    class Reader
      FRAME_SIZE_LEN = 4
      CMD_SIZE_LEN = 4

      def initialize(io)
        ensure_interface_implemented!(io)
        @io = io

        @readed = 0
      end

      # TODO add timeout?
      def read_fully
        frame_szie = read_frame_size
        raise "IO reader is empty! maybe server error, please check server log for help." if frame_szie.nil?

        base_cmd = read_command

        buffer = read_remaining(frame_szie)

        [base_cmd, buffer]
      end

      def read_frame_size
        frame_size = read(FRAME_SIZE_LEN, 'N')
        # reset cursor! let's read the frame
        @readed = 0
        frame_size
      end

      def read_command
        cmd_size = read(CMD_SIZE_LEN, 'N')
        cmd_bytes = read(cmd_size)
        Pulsar::Proto::BaseCommand.decode(cmd_bytes)
      end

      def read_remaining(frame_szie)
        meta_and_payload_size = frame_szie - @readed
        return if meta_and_payload_size <= 0
        read(meta_and_payload_size)
      end

      private
      def ensure_interface_implemented!(io)
        [:read, :closed?].each do |x|
          raise "io must implement method: #{x}" unless io.respond_to?(x)
        end
      end

      def read(size, unpack = nil)
        raise Errno::ECONNRESET if @io.closed?
        raise Errno::ETIMEDOUT unless IO.select([@io], nil)

        bytes = @io.read(size)
        @readed = @readed.to_i + size.to_i

        return bytes if unpack.nil? || bytes.nil?

        bytes.unpack(unpack).first
      end
    end
  end
end
