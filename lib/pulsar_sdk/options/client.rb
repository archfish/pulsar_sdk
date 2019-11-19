module PulsarSdk
  module Options
    class Client < Base
      attr_accessor :url,
                    :connection_timeout,
                    :operation_timeout,
                    :authentication,
                    :tls_trust_certs_file_path,
                    :tls_allow_insecure_connection,
                    :tls_validate_hostname

      private
      def set_default
        self.url = ENV.fetch("PULSAR_BROKER_URL", nil)

        # 连接5秒超时
        self.connection_timeout = 5

        # Set the operation timeout (default: 30 seconds)
        # Producer-create, subscribe and unsubscribe operations will be retried until this interval
        self.operation_timeout = 30
      end
    end
  end
end
