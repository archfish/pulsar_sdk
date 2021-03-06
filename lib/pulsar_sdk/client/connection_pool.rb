module PulsarSdk
  module Client
    class ConnectionPool
      prepend ::PulsarSdk::Tweaks::CleanInspect

      def initialize(opts)
        raise "opts expected a PulsarSdk::Options::Connection got #{opts.class}" unless opts.is_a?(PulsarSdk::Options::Connection)

        @mutex = Mutex.new
        @pool = ::PulsarSdk::Tweaks::WaitMap.new

        @options = opts
        @keepalive = opts.keepalive
        @connection_timeout = opts.connection_timeout

        @authentication = opts.auth_provider
        @tls_options = opts.tls_options

        instance_variables.each do |x|
          remove_instance_variable(x) if instance_variable_get(x).nil?
        end
      end

      def fetch(logical_addr, physical_addr)
        id = (logical_addr || physical_addr).to_s
        raise 'logical_addr and physical_addr both empty!' if id.empty?

        conn = nil
        @mutex.synchronize do
          conn = @pool.find(id)

          if conn.nil? || conn.closed?
            # REMOVE closed conncetion from pool
            @pool.delete(id, 0.01) unless conn.nil?

            opts = @options.dup
            opts.assign_attributes(
              logical_addr: logical_addr,
              physical_addr: physical_addr
            )

            conn = @pool.add(id, ::PulsarSdk::Client::Connection.establish(opts))
          end
        end

        conn
      end

      def run_checker
        Thread.new do
          loop do
            begin
              @pool.each do |_k, v|
                last_ping_at, last_received_at = v.active_status

                case
                when last_ping_at - last_received_at >= @keepalive * 2
                  v.close
                when last_ping_at - last_received_at > @keepalive
                  v.ping
                end
              end
            rescue => exp
              PulsarSdk.logger.error(exp)
            end

            sleep(1)
          end
        end
      end

      def close
        @pool.clear do |_, v|
          v.close
        end
      end
    end
  end
end
