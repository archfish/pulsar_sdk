module PulsarSdk
  module Protocol
    class Message
      prepend ::PulsarSdk::Tweaks::AssignAttributes
      prepend ::PulsarSdk::Tweaks::CleanInspect

      attr_accessor :publish_time, :event_time, :partition_key, :payload,
                    :message_id, :properties, :consumer_id, :topic

      attr_accessor :ack_handler

      # def publish_at
      # def event_at
      [:publish, :event].each do |x|
        define_method "#{x}_at" do
          v = self.public_send("#{x}_time").to_i
          return if v.zero?
          TimeX.at_timestamp(v)
        end
      end

      # 辅助方法：判断消息是否需要JSON反序列化
      def json_encoded?
        # 检查properties中是否有标记为JSON的内容类型
        self.properties.each do |prop|
          if prop.key == 'Content-Type' && prop.value.include?('application/json')
            return true
          end
        end
        false
      end

      # 异步确认消息（原有方法保持不变）
      def ack(type = Pulsar::Proto::CommandAck::AckType::Individual)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::ACK,
          ack: Pulsar::Proto::CommandAck.new(
            consumer_id: self.consumer_id,
            message_id: [self.message_id],
            ack_type: type
          )
        )

        ack_handler.call(base_cmd)
        @confirmed = true
      end

      # 同步确认消息（新增方法）
      def ack!(type = Pulsar::Proto::CommandAck::AckType::Individual, timeout: 5)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::ACK,
          ack: Pulsar::Proto::CommandAck.new(
            consumer_id: self.consumer_id,
            message_id: [self.message_id],
            ack_type: type
          )
        )

        # 如果ack_handler支持同步调用，则等待确认结果
        result = if ack_handler.respond_to?(:call_sync)
                   ack_handler.call_sync(base_cmd, timeout)
                 else
                   ack_handler.call(base_cmd)
                   true # 默认返回成功
                 end

        @confirmed = true if result
        result
      end

      # 异步否定确认消息（原有方法保持不变）
      def nack
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::REDELIVER_UNACKNOWLEDGED_MESSAGES,
          redeliverUnacknowledgedMessages: Pulsar::Proto::CommandRedeliverUnacknowledgedMessages.new(
            consumer_id: self.consumer_id,
            message_ids: [self.message_id]
          )
        )

        ack_handler.call(base_cmd)
        @confirmed = true
      end

      # 同步否定确认消息（新增方法）
      def nack!(timeout: 5)
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::REDELIVER_UNACKNOWLEDGED_MESSAGES,
          redeliverUnacknowledgedMessages: Pulsar::Proto::CommandRedeliverUnacknowledgedMessages.new(
            consumer_id: self.consumer_id,
            message_ids: [self.message_id]
          )
        )

        # 如果ack_handler支持同步调用，则等待确认结果
        result = if ack_handler.respond_to?(:call_sync)
                   ack_handler.call_sync(base_cmd, timeout)
                 else
                   ack_handler.call(base_cmd)
                   true # 默认返回成功
                 end

        @confirmed = true if result
        result
      end

      # 检查是否有确认，无论是ack还是nack都算是确认
      def confirmed?
        !!@confirmed
      end

      # 获取确认状态（新增方法）
      def confirmation_status
        return :unconfirmed unless @confirmed
        # 可以扩展为返回更详细的状态信息
        :confirmed
      end
    end
  end
end
