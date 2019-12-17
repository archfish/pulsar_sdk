module PulsarSdk
  module Protocol
    class PartitionedTopic
      def initialize(client, topic)
        @client = client
        @tn = TopicName.parse(topic)
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

    class TopicName
      PUBLIC_TENANT = 'public'.freeze
      DEFAULT_NAMESPACE = 'default'.freeze
      PARTITIONED_TOPIC_SUFFIX = '-partition-'.freeze

      prepend ::PulsarSdk::Tweaks::AssignAttributes

      attr_accessor :domain, :tenant, :namespace, :topic, :partition

      def to_s
        [
          mk_domain,
          self.tenant,
          self.namespace,
          mk_topic
        ].join('/')
      end

      private
      def mk_topic
        return self.topic if self.partition.nil?

        "#{self.topic}#{PARTITIONED_TOPIC_SUFFIX}#{self.partition}"
      end

      def mk_domain
        return if domain.nil?
        "#{self.domain}:/"
      end

      # new:    persistent://tenant/namespace/topic
      # legacy: persistent://tenant/cluster/namespace/topic
      def self.parse(topic)
        if !topic.include?('://')
          parts = topic.split('/')
          if parts.size == 3 || parts.size == 4
            topic = "persistent://#{topic}"
          elsif parts.size == 1
            topic = "persistent://#{PUBLIC_TENANT}/#{DEFAULT_NAMESPACE}/" + parts[0]
          else
            raise "Invalid short topic name: #{topic}, it should be in the format of <tenant>/<namespace>/<topic> or <topic>"
          end
        end

        domain, rest = topic.split('://', 2)
        unless ['persistent', 'non-persistent'].include?(domain)
          raise "Invalid topic domain: #{domain}"
        end

        tn = new(domain: domain)
        topic_with_partition = nil

        case rest.count('/')
        when 2
          tn.tenant, tn.namespace, topic_with_partition = rest.split('/', 3)
        when 3
          tn.tenant, cluster, namespace, topic_with_partition = rest.split('/', 4)
          tn.namespace = [cluster, namespace].join('/')
        else
          raise "Invalid topic name: #{topic}"
        end

        tn.topic, partition = topic_with_partition.split(PARTITIONED_TOPIC_SUFFIX, 2)

        tn.partition = partition&.to_i

        tn
      end
    end
  end
end
