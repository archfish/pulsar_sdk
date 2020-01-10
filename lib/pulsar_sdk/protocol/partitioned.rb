module PulsarSdk
  module Protocol
    class Partitioned
      def initialize(client, topic)
        @client = client
        @tn = ::PulsarSdk::Protocol::Topic.parse(topic)
      end

      def partitions
        pmr = topic_metadata

        if !success_response?(pmr)
          PulsarSdk.logger.error(__method__){"Get topic partitioned metadata failed, #{pmr.error}: #{pmr.message}"}
          return []
        end

        return [@tn.to_s] if pmr.partitions.zero?

        tn = @tn.dup
        (0..pmr.partitions).map do |i|
          tn.partition = i
          tn.to_s
        end
      end

      # 当前topic是否是分区topic
      def partitioned?
        topic_metadata.partitions > 0
      end

      private
      def success_response?(pmr)
        result = false
        Pulsar::Proto::CommandPartitionedTopicMetadataResponse::LookupType.tap do |x|
          result = x.resolve(pmr.response) == x.const_get(:Success)
        end

        result
      end

      def topic_metadata
        return @topic_metadata_ unless @topic_metadata_.nil?

        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::PARTITIONED_METADATA,
          partitionMetadata: Pulsar::Proto::CommandPartitionedTopicMetadata.new(
            topic: @tn.to_s
          )
        )
        @topic_metadata_ = @client.request_any_broker(base_cmd).partitionMetadataResponse
      end
    end
  end
end
