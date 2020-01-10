module PulsarSdk
  module Protocol
    class Topic
      PUBLIC_TENANT = 'public'.freeze
      DEFAULT_NAMESPACE = 'default'.freeze
      PARTITIONED_TOPIC_SUFFIX = '-partition-'.freeze

      prepend ::PulsarSdk::Tweaks::AssignAttributes

      attr_accessor :domain, :namespace, :topic, :partition

      def to_s
        [
          mk_domain,
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
          tenant, namespace, topic_with_partition = rest.split('/', 3)
          tn.namespace = [tenant, namespace].join('/')
        when 3
          tenant, cluster, namespace, topic_with_partition = rest.split('/', 4)
          tn.namespace = [tenant, cluster, namespace].join('/')
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
