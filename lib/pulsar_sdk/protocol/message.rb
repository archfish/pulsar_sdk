module PulsarSdk
  module Protocol
    class Message
      prepend ::PulsarSdk::Tweaks::AssignAttributes

      attr_accessor :publish_time, :event_time, :partition_key, :payload,
                    :message_id, :properties, :consumer_id, :topic

      attr_accessor :command_handler

      # def publish_at
      # def event_at
      [:publish, :event].each do |x|
        define_method "#{x}_at" do
          v = self.public_send("#{x}_time").to_i
          return if v.zero?
          Time.at_timestamp(v)
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

        command_handler.call(base_cmd)
      end

      # NOTE 这个消息之后的所有消息都会重新发送回来，导致consumer中消息队列内容重复了
      # NOTE 这应该是保证消息顺序的feature，所以发送nack时要清空消息队列
      def nack
        base_cmd = Pulsar::Proto::BaseCommand.new(
          type: Pulsar::Proto::BaseCommand::Type::REDELIVER_UNACKNOWLEDGED_MESSAGES,
          redeliverUnacknowledgedMessages: Pulsar::Proto::CommandRedeliverUnacknowledgedMessages.new(
            consumer_id: self.consumer_id,
            message_ids: [self.message_id]
          )
        )

        command_handler.call(base_cmd)
      end
    end
  end
end
