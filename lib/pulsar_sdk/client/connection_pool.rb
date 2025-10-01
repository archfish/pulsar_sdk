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
            # REMOVE closed connection from pool
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
              # 使用临时数组避免在迭代过程中修改池
              connections_to_check = []
              @pool.each do |k, v|
                connections_to_check << [k, v]
              end

              connections_to_check.each do |id, conn|
                begin
                  last_ping_at, last_received_at = conn.active_status

                  case
                  when last_ping_at - last_received_at >= @keepalive * 2
                    PulsarSdk.logger.warn("ConnectionPool#run_checker") { "Closing stale connection: #{id}" }
                    @mutex.synchronize do
                      # 确保连接仍然在池中再删除
                      if @pool.find(id) == conn
                        @pool.delete(id, 0.01)
                        conn.close
                      end
                    end
                  when last_ping_at - last_received_at > @keepalive
                    conn.ping
                  end
                rescue => exp
                  PulsarSdk.logger.error("ConnectionPool#run_checker") { "Error checking connection #{id}: #{exp}" }
                  # 如果检查连接时出错，从池中移除该连接
                  @mutex.synchronize do
                    if @pool.find(id) == conn
                      @pool.delete(id, 0.01)
                      conn.close rescue nil
                    end
                  end
                end
              end
            rescue => exp
              PulsarSdk.logger.error("ConnectionPool#run_checker") { "Error in connection checker loop: #{exp}" }
            end

            sleep(1)
          end
        end
      end

      def close
        @mutex.synchronize do
          @pool.clear do |_, v|
            v.close rescue nil
          end
        end
      end
    end
  end
end