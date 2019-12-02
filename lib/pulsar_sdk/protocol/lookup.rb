module PulsarSdk
  module Protocol
    class Lookup
      MAX_LOOKUP_TIMES = 20

      def initialize(client, service_url)
        @client = client
        @service_url = service_url
      end

      # output
      #   [proxy_addr, broker_addr]
      def lookup(topic)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
          lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
            topic: topic,
            authoritative: false
          )
        )
        resp = sync_request(base_cmd).lookupTopicResponse

        # 最多查找这么多次
        MAX_LOOKUP_TIMES.times do
          case Pulsar::Proto::CommandLookupTopicResponse::LookupType.resolve(resp.response)
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Failed
            puts "ERROR: Failed to lookup topic 「#{topic}」, #{resp.error}"
            break
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Redirect
            proxy_addr, broker_addr = extract_addr(resp)
            base_cmd = Pulsar::Proto::BaseCommand.new(
              type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
              lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
                topic: topic,
                authoritative: resp.authoritative
              )
            )
            resp = @client.sync_request(proxy_addr, broker_addr, base_cmd).lookupTopicResponse
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Connect
            return extract_addr(resp)
          end
        end
      end

      private
      def sync_request(cmd)
        @client.conn.request(cmd)
      end

      def extract_addr(resp)
        proxy_addr = resp.brokerServiceUrl
        broker_addr = resp.proxy_through_service_url ? @service_url : proxy_addr

        [proxy_addr, broker_addr]
      end
    end
  end
end
