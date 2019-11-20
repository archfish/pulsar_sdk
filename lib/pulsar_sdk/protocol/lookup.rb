module PulsarSdk
  module Protocol
    class Lookup
      MAX_LOOKUP_TIMES = 20

      def initialize(client, service_url)
        @client = client
        @service_url = service_url
      end

      def lookup(topic)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
          lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
            topic: topic,
            request_id: new_request_id,
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
            broker_addr, proxy_addr = extract_addr(resp)
            base_cmd = Pulsar::Proto::BaseCommand.new(
              type: Pulsar::Proto::BaseCommand::Type::LOOKUP,
              lookupTopic: Pulsar::Proto::CommandLookupTopic.new(
                topic: topic,
                request_id: new_request_id,
                authoritative: resp.authoritative
              )
            )
            resp = @client.sync_request(broker_addr, proxy_addr, base_cmd).lookupTopicResponse
          when Pulsar::Proto::CommandLookupTopicResponse::LookupType::Connect
            return extract_addr(resp)
          end
        end
      end

      private
      def new_request_id
        @client.conn.new_request_id
      end

      def sync_request(cmd)
        @client.conn.sync_request(cmd)
      end

      def extract_addr(resp)
        broker_addr = resp.brokerServiceUrl
        proxy_addr = resp.proxy_through_service_url ? @service_url : broker_addr

        [broker_addr, proxy_addr]
      end
    end
  end
end
