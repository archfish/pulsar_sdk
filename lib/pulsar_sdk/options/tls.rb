module PulsarSdk
  module Options
    class Tls < Base
      attr_accessor :trust_certs_file_path, :allow_insecure_connection,
                    :validate_hostname
    end
  end
end
