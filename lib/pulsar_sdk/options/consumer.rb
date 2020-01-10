module PulsarSdk
  module Options
    class Consumer < Base
      attr_accessor :topic, :topics, :topics_pattern,
                    :name, :subscription_name, :subscription_type,
                    :prefetch, :redelivery_delay,
                    :listen_wait

      def subscription_type
        sub_type = @subscription_type.to_sym
        if Pulsar::Proto::CommandSubscribe::SubType.constants.include?(sub_type)
          return Pulsar::Proto::CommandSubscribe::SubType.resolve(sub_type)
        end

        raise "subscription_type mismatch! available is #{Pulsar::Proto::CommandSubscribe::SubType.constants}, got 「#{@subscription_type}」"
      end

      private
      def set_default
        self.name = 'ruby-consumer.' + SecureRandom.urlsafe_base64(10)
        # 相同名字的subscription与订阅模式有关
        self.subscription_name = 'ruby-subscription'
        self.subscription_type = :Exclusive
        # 延迟消息重发，默认60秒
        self.redelivery_delay = 60
        # 记录预取数量
        self.prefetch = 1000
      end
    end
  end
end
