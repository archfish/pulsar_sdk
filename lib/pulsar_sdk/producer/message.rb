module PulsarSdk
  module Producer
    class Message
      prepend ::PulsarSdk::Tweaks::CleanInspect

      # Content-Type常量定义
      CONTENT_TYPE_JSON = 'application/json; charset=utf-8'.freeze
      CONTENT_TYPE_TEXT = 'text/plain; charset=utf-8'.freeze

      attr_reader :metadata, :message, :key

      def initialize(msg, metadata = nil, key = nil)
        # TODO check metadata type
        @message, @metadata = msg, metadata
        @metadata ||= Pulsar::Proto::MessageMetadata.new

        # 统一进行消息序列化处理
        serialize_message!

        publish_time = @metadata.publish_time
        @metadata.publish_time = publish_time.zero? ? TimeX.now.timestamp : publish_time

        self.key = key
      end

      def producer_name=(v)
        @metadata.producer_name = v
      end

      def sequence_id=(v)
        @metadata.sequence_id = v
      end

      def binary_string
        @message.bytes.pack('C*')
      end

      def key=(v, b64 = false)
        @metadata.partition_key = v.to_s
        @metadata.partition_key_b64_encoded = b64
      end

      # 辅助方法：判断消息是否需要JSON反序列化
      def json_encoded?
        # 检查metadata中是否有标记为JSON的内容类型
        @metadata.properties.each do |prop|
          if prop.key == 'Content-Type' && prop.value.include?('application/json')
            return true
          end
        end
        false
      end

      private
      # 统一的消息序列化方法
      def serialize_message!
        case @message
        when String
          # 字符串类型也添加Content-Type
          @metadata.properties << Pulsar::Proto::KeyValue.new(key: 'Content-Type', value: CONTENT_TYPE_TEXT)
        when Hash, Array
          # 对哈希和数组进行JSON序列化
          @message = @message.to_json
          @metadata.properties << Pulsar::Proto::KeyValue.new(key: 'Content-Type', value: CONTENT_TYPE_JSON)
        else
          # 其他对象类型也进行JSON序列化
          if @message.respond_to?(:to_json)
            @message = @message.to_json
            @metadata.properties << Pulsar::Proto::KeyValue.new(key: 'Content-Type', value: CONTENT_TYPE_JSON)
          else
            # 如果对象不能转换为JSON，则转换为字符串
            @message = @message.to_s
            @metadata.properties << Pulsar::Proto::KeyValue.new(key: 'Content-Type', value: CONTENT_TYPE_TEXT)
          end
        end
      end
    end
  end
end