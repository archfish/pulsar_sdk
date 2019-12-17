module PulsarSdk
  module Protocol
    class Lookup
      MAX_LOOKUP_TIMES = 20

      def initialize(client, service_url)
        @client = client
        @service_url = service_url
      end

      # output
      #   [logical_addr, physical_addr]
      def lookup(topic)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
          lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
            topic: topic,
            authoritative: false
          )
        )
        resp = @client.request_any_broker(base_cmd).lookupTopicResponse

        # 最多查找这么多次
        MAX_LOOKUP_TIMES.times do
          case Pulsar::Proto::CommandLookupTopicResponse::LookupType.resolve(resp.response)
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Failed
            PulsarSdk.logger.error(__method__){"Failed to lookup topic 「#{topic}」, #{resp.error}"}
            break
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Redirect
            logical_addr, physical_addr = extract_addr(resp)
            base_cmd = Pulsar::Proto::BaseCommand.new(
              type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
              lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
                topic: topic,
                authoritative: resp.authoritative
              )
            )
            # NOTE 从连接池拿
            resp = @client.request(logical_addr, physical_addr, base_cmd).lookupTopicResponse
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Connect
            return extract_addr(resp)
          end
        end
      end

      private
      def extract_addr(resp)
        logical_addr = resp.brokerServiceUrl
        physical_addr = resp.proxy_through_service_url ? @service_url : logical_addr

        [logical_addr, physical_addr]
      end
    end
  end
end
