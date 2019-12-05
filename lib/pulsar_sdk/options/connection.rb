module PulsarSdk
  module Options
    class Connection < Base
      DEFAULT_PORT = 6650

      attr_accessor :logical_addr, :physical_addr,
                    :connection_timeout,
                    :operation_timeout,
                    :auth_provider,
                    :tls_options,
                    :keepalive

      [:logical_addr, :physical_addr].each do |x|
        define_method "#{x}=" do |v|
          return instance_variable_set("@#{x}", v) if v.nil?
          v = v.is_a?(URI) ? v : URI.parse(v)
          v.port = DEFAULT_PORT if v.port.nil?
          instance_variable_set("@#{x}", v)
        end
      end

      def connecting_through_proxy?
        logical_addr == physical_addr
      end

      def proxy_to_broker_url
        connecting_through_proxy? ? logical_addr : nil
      end

      def port_and_host_from(name)
        v = instance_variable_get("@#{name}")
        return if v.nil?
        [v.port, v.host]
      end

      private
      def set_default
        self.logical_addr = ENV.fetch("PULSAR_BROKER_URL", nil)

        # 连接5秒超时
        self.connection_timeout = 5

        # Set the operation timeout (default: 30 seconds)
        # Producer-create, subscribe and unsubscribe operations will be retried until this interval
        self.operation_timeout = 30

        self.keepalive = 30
      end
    end
  end
end
