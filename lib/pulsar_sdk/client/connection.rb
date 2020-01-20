require 'socket'

module PulsarSdk
  module Client
    class Connection
      prepend ::PulsarSdk::Tweaks::CleanInspect

      CLIENT_NAME = "pulsar-client-#{PulsarSdk::VERSION}".freeze
      PROTOCOL_VER = Pulsar::Proto::ProtocolVersion::V13

      attr_reader :consumer_handlers
      attr_reader :producer_handlers
      attr_reader :response_container # 用于处理状态回调
      attr_reader :seq_generator

      # opts PulsarSdk::Options::Connection
      def initialize(opts)
        @conn_options = opts

        @socket = nil
        @state = Status.new

        @seq_generator = SeqGenerator.new

        @consumer_handlers = ConsumerHandler.new
        @producer_handlers = ProducerHandler.new
        @response_container = ResponseContainer.new
      end

      def start
        unless connect && do_hand_shake && listen
          @state.closed!
        end
      end

      def self.establish(opts)
        conn = new(opts).tap do |c|
          c.start
        end
        # TODO check connection ready
        conn
      end

      def close
        @state.closed!
        Timeout::timeout(2) {@pong&.join} rescue @pong&.kill
        @pong&.join

        consumer_handlers.each{|_k, v| v.call}
        producer_handlers.each{|_k, v| v.call}
      ensure
        @socket.close
      end

      def closed?
        @state.closed?
      end

      def ping
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::PING,
          ping: Pulsar::Proto::CommandPing.new
        )

        request(base_cmd, nil, true)

        @state.ping!
      end

      def active_status
        [@state.last_ping_at, @state.last_received_at]
      end

      def request(cmd, msg = nil, async = false, timeout = nil)
        raise 'connection was closed!' if closed?

        cmd.seq_generator ||= @seq_generator

        # NOTE try to auto set *_id
        cmd.handle_ids

        frame = PulsarSdk::Protocol::Frame.encode(cmd, msg)
        write(frame)
        return true if async

        if request_id = cmd.get_request_id
          return @response_container.delete(request_id, timeout)
        end

        true
      end

      private
      def reader
        @reader ||= PulsarSdk::Protocol::Reader.new(@socket)
      end

      def write(bytes)
        begin
          @socket.write_nonblock(bytes)
        rescue IO::WaitWritable
          IO.select(nil, [@socket], nil, @conn_options.operation_timeout)
          retry
        end
      end

      def listen
        @pong = Thread.new do
          loop do
            break if closed?

            begin
              @state.ready? ? read_from_connection : @state.wait
            rescue Errno::ETIMEDOUT
              # read timeout, do nothing
            rescue => e
              PulsarSdk.logger.error("reader error") {e}
              close
            end
          end
        end

        true
      end

      def connect
        return true if (@socket && !closed?)

        @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        @socket.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_KEEPALIVE, true)

        host_port = @conn_options.port_and_host_from(:logical_addr)

        sockaddr = Socket.sockaddr_in(*host_port)
        begin
          # Initiate the socket connection in the background. If it doesn't fail
          # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
          # indicating the connection is in progress.
          @socket.connect_nonblock(sockaddr)
        rescue IO::WaitWritable
          # IO.select will block until the socket is writable or the timeout
          # is exceeded, whichever comes first.
          unless IO.select(nil, [@socket], nil, @conn_options.connection_timeout)
            # IO.select returns nil when the socket is not ready before timeout
            # seconds have elapsed
            @socket.close
            return false
          end

          begin
            # Verify there is now a good connection.
            @socket.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
            # The socket is connected, we're good!
          end
        end

        @state.tcp_connected!

        true
      end

      def do_hand_shake
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::CONNECT,
          connect: Pulsar::Proto::CommandConnect.new(
            client_version: CLIENT_NAME,
            protocol_version: PROTOCOL_VER,
            proxy_to_broker_url: @conn_options.proxy_to_broker_url
          )
        )

        request(base_cmd)

        @state.ready!
        true
      end

      def read_from_connection
        base_cmd, meta_and_payload = reader.read_fully
        return if base_cmd.nil?

        @state.received!

        handle_base_command(base_cmd, meta_and_payload)
      end

      def handle_base_command(cmd, payload)
        PulsarSdk.logger.debug(__method__){cmd.type} unless cmd.typeof_ping?
        case
        when cmd.typeof_success?
          handle_response(cmd)
        when cmd.typeof_connected?
          PulsarSdk.logger.info(__method__){"#{cmd.type}: #{cmd.connected}"}
        when cmd.typeof_producer_success?
          handle_response(cmd)
        when cmd.typeof_lookup_response?
          handle_response(cmd)
        when cmd.typeof_get_last_message_id_response?
          handle_response(cmd)
        when cmd.typeof_consumer_stats_response?
          handle_response(cmd)
        when cmd.typeof_get_topics_of_namespace_response?
          handle_response(cmd)
        when cmd.typeof_get_schema_response?
        when cmd.typeof_partitioned_metadata_response?
          handle_response(cmd)
        when cmd.typeof_error?
          PulsarSdk.logger.error(__method__){"#{cmd.error}: #{cmd.message}"}
        when cmd.typeof_close_producer?
          producer_id = cmd.close_producer.producer_id
          producer_handlers.find(producer_id)&.call
        when cmd.typeof_close_consumer?
          consumer_id = cmd.close_consumer.consumer_id
          consumer_handlers.find(consumer_id)&.call
        when cmd.typeof_active_consumer_change?
        when cmd.typeof_message?
          handle_message(cmd, payload)
        when cmd.typeof_send_receipt?
          handle_send_receipt(cmd)
        when cmd.typeof_ping?
          handle_ping
        when cmd.typeof_pong?
        else
          close
          raise "Received invalid command type: #{cmd.type}"
        end

        true
      end

      def handle_response(cmd)
        request_id = cmd.get_request_id
        return if request_id.nil?
        @response_container.add(request_id, cmd)
      end

      def handle_message(cmd, payload)
        consumer_id = cmd.get_consumer_id
        return if consumer_id.nil?
        handler = consumer_handlers.find(consumer_id)
        return if handler.nil?
        handler.call(cmd, payload)
      end

      def handle_send_receipt(cmd)
        send_receipt = cmd.send_receipt
        producer_id = send_receipt.producer_id
        handler = producer_handlers.find(producer_id)
        return if handler.nil?
        handler.call(send_receipt)
      end

      def handle_ping
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::PONG,
          pong: Pulsar::Proto::CommandPong.new
        )

        request(base_cmd, nil, true)
      end

      class Status
        attr_reader :last_received_at, :last_ping_at

        STATUS = %w[
          init
          connecting
          tcp_connected
          ready
          closed
        ].freeze

        def initialize
          @state = 'init'
          @lock = Mutex.new
          @signal = ConditionVariable.new
          @last_received_at = 0
          @last_ping_at = 0
        end

        def wait
          @lock.synchronize do
            @signal.wait(@lock)
          end
        end

        def received!
          @lock.synchronize do
            @last_received_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          end
        end

        def ping!
          @lock.synchronize do
            @last_ping_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          end
        end

        STATUS.each do |x|
          define_method "#{x.to_s.downcase}?" do
            @state == x
          end

          define_method "#{x.to_s.downcase}!" do
            @lock.synchronize do
              @state = x
              @signal.broadcast
            end
          end
        end
      end

      class SeqGenerator
        def initialize
          @mutex = Mutex.new
          @seq = {}
        end

        # def new_request_id
        # def new_producer_id
        # def new_consumer_id
        # def new_sequence_id
        [:request_id, :producer_id, :consumer_id, :sequence_id].each do |k|
          define_method "new_#{k}" do
            next!(k)
          end
        end

        def next!(key)
          @mutex.synchronize do
            @seq[key] ||= 0
            @seq[key] += 1
          end
        end
      end

      class ConsumerHandler < ::PulsarSdk::Tweaks::WaitMap; end

      class ProducerHandler < ::PulsarSdk::Tweaks::WaitMap; end

      class ResponseContainer < ::PulsarSdk::Tweaks::WaitMap; end
    end
  end
end
