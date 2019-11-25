require 'socket'

module PulsarSdk
  class Connection
    CLIENT_NAME = "pulsar-client-#{PulsarSdk::VERSION}".freeze
    PROTOCOL_VER = Pulsar::Proto::ProtocolVersion::V13
    DEFAULT_PORT = 6650

    attr_accessor :operation_timeout, :connection_timeout
    attr_reader :consumer_handlers
    attr_reader :producer_handlers
    attr_reader :response_container # 用于处理状态回调
    attr_reader :seq_generator

    def initialize(proxy_addr, broker_addr = nil, tls_options = nil, auth_provider = nil)
      @proxy_addr = proxy_addr
      @broker_addr = broker_addr || @proxy_addr
      @tls_options = tls_options&.dup
      @auth_provider = auth_provider
      @socket = nil
      @last_data_received_at = TimeX.now
      @state = Status.new
      self.operation_timeout = 30
      self.connection_timeout = 5

      @seq_generator = SeqGenerator.new

      @mutex = Mutex.new
      @receive_queue = Queue.new
      @received = ConditionVariable.new

      @consumer_handlers = ConsumerHandler.new
      @producer_handlers = ProducerHandler.new
      @response_container = ResponseContainer.new
    end

    def start
      unless connect && do_hand_shake && run
        @state.closed!
      end
    end

    def connect
      return true if (@socket && !closed?)

      @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      @socket.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_KEEPALIVE, true)

			sockaddr = Socket.sockaddr_in(*extract_port_and_host)
      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        @socket.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        # IO.select will block until the socket is writable or the timeout
        # is exceeded, whichever comes first.
        unless IO.select(nil, [@socket], nil, connection_timeout)
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
          protocol_version: PROTOCOL_VER
        )
      )

      sync_request(base_cmd)

      @state.ready!
      true
    end

    def run
      @pong = Thread.new do
        loop do
          break unless @state.ready?
          begin
            read_from_connection
          rescue Errno::ETIMEDOUT
            # read timeout, do nothing
          rescue => e
            puts "ERROR: reader exist, cause by #{e}"
            puts "BACKTRACE: #{e.backtrace.join("\n")}"
            @state.closed!
          end
        end
      end

      true
    end

    def close
      @state.closed!
      Timeout::timeout(5) {@pong&.join} rescue @pong&.kill
      @pong&.join
    ensure
      @socket.close
    end

    def closed?
      @socket.closed?
    end

    def request(cmd, msg = nil, async = false, timeout = nil)
      cmd.seq_generator ||= @seq_generator

      # NOTE try to auto set *_id
      cmd.set_request_id
      cmd.set_consumer_id
      cmd.set_producer_id
      cmd.set_sequence_id

      frame = PulsarSdk::Protocol::Frame.encode(cmd, msg)
      write(frame)
      return true if async

      if request_id = cmd.get_request_id
        return @response_container.delete(request_id, timeout)
      end

      true
    end

    def write(bytes)
      begin
        @socket.write_nonblock(bytes)
      rescue IO::WaitWritable
        IO.select(nil, [@socket], nil, self.operation_timeout)
        retry
      end
    end

    # 发送消息或命令
    def async_request(cmd)
      write(
        PulsarSdk::Protocol::Frame.encode(cmd)
      )
    end

    def sync_request(cmd, timeout = nil)
      async_request(cmd)
      if request_id = cmd.get_request_id
        return @response_container.delete(request_id, timeout)
      end
      true
    end

    def read_from_connection
      base_cmd, meta_and_payload = reader.read_fully

      set_last_data_received

      handle_base_command(base_cmd, meta_and_payload)
    end

    def new_request_id
      @seq_generator.new_request_id
    end

    private
    def reader
      @reader ||= PulsarSdk::Protocol::Reader.new(@socket)
    end

    def extract_port_and_host
      url = URI.parse(@broker_addr)
      [url.port || DEFAULT_PORT, url.host]
    end

    def handle_base_command(cmd, payload)
      puts "INFO: handle_base_command: #{cmd.type}"
      case
      when cmd.typeof_success?
        handle_response(cmd)
      when cmd.typeof_connected?
        puts "#{cmd.type}: #{cmd.connected}"
      when cmd.typeof_producer_success?
        handle_response(cmd)
      when cmd.typeof_lookup_response?
        handle_response(cmd)
      when cmd.typeof_get_last_message_id_response?
        handle_response(cmd)
      when cmd.typeof_consumer_stats_response?
      when cmd.typeof_get_topics_of_namespace_response?
      when cmd.typeof_get_schema_response?
      when cmd.typeof_partitioned_metadata_response?
        handle_response(cmd)
      when cmd.typeof_error?
        puts "ERROR: #{cmd.error} \n #{cmd.message}"
      when cmd.typeof_close_producer?
      when cmd.typeof_close_consumer?
      when cmd.typeof_message?
        handle_message(cmd, payload)
      when cmd.typeof_send_receipt?
        handle_send_receipt(cmd.send_receipt)
      when cmd.typeof_ping?
        handle_ping
      when cmd.typeof_pong?
      else
        @socket.close
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
      handler = @consumer_handlers.find(consumer_id)
      return if handler.nil?
      handler.call(cmd, payload)
    end

    def handle_send_receipt(send_receipt)
      producer_id = send_receipt.producer_id
      handler = @producer_handlers.find(producer_id)
      return if handler.nil?
      handler.call(send_receipt)
    end

    def set_last_data_received
      @last_data_received_at = TimeX.now
    end

    def handle_ping
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::PONG,
        pong: Pulsar::Proto::CommandPong.new
      )

      async_request(base_cmd)
    end

    def send_ping
      base_cmd = Pulsar::Proto::BaseCommand.new(
        type: Pulsar::Proto::BaseCommand::Type::PING,
        ping: Pulsar::Proto::CommandPing.new
      )

      async_request(base_cmd)
    end

    class Status
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
      end

      STATUS.each do |x|
        define_method "#{x.to_s.downcase}?" do
          @state == x
        end

        define_method "#{x.to_s.downcase}!" do
          @lock.synchronize do
            @state = x
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

    class EventHandler < ::PulsarSdk::Tweaks::WaitMap; end

    class ConsumerHandler < EventHandler; end

    class ProducerHandler < EventHandler; end

    class ResponseContainer < ::PulsarSdk::Tweaks::WaitMap; end
  end
end
