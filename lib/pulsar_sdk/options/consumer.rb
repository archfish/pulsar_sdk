module PulsarSdk
  module Options
    class Consumer < Base
      attr_accessor :topic, :name, :subscription_name, :subscription_type, :prefetch,
                    :listen_wait

      private
      def set_default
        self.name = 'ruby-consumer.' + SecureRandom.urlsafe_base64(10)
        # 相同名字的subscription与订阅模式有关
        self.subscription_name = 'ruby-subscription'
        self.subscription_type = Pulsar::Proto::CommandSubscribe::SubType::Exclusive
        # 记录预取数量
        self.prefetch = 1000
      end
    end
  end
end
