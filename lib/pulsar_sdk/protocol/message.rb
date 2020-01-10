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

      # 检查是否有确认，无论是ack还是nack都算是确认
      def confirmed?
        !!@confirmed
      end

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
    end
  end
end
