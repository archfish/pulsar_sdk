module PulsarSdk
  module Protocol
    class Namespace
      def initialize(client)
        @client = client
      end

      def topics(namespace)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::GET_TOPICS_OF_NAMESPACE_RESPONSE,
          lookupTopic: Pulsar::Proto::CommandGetTopicsOfNamespace.new(
            namespace: namespace,
            mode: false
          )
        )
        @client.request_any_broker(base_cmd)&.topics
      end
    end
  end
end
