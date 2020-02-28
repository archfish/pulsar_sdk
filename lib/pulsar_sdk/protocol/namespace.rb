module PulsarSdk
  module Protocol
    class Namespace
      def initialize(client)
        @client = client
      end

      def topics(namespace)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::GET_TOPICS_OF_NAMESPACE,
          getTopicsOfNamespace: Pulsar::Proto::CommandGetTopicsOfNamespace.new(
            namespace: namespace,
            mode: Pulsar::Proto::CommandGetTopicsOfNamespace::Mode.resolve(:ALL)
          )
        )
        resp = @client.request_any_broker(base_cmd)

        resp.getTopicsOfNamespaceResponse&.topics
      end
    end
  end
end
