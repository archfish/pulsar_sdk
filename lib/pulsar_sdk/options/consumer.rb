module PulsarSdk
  module Options
    class Consumer < Base
      attr_accessor :topic, :name, :subscription_name, :subscription_type

      private
      def set_default
        self.name = 'ruby-consumer.' + SecureRandom.urlsafe_base64(10)
        # 相同名字的subscription与订阅模式有关
        self.subscription_name = 'ruby-subscription'
        self.subscription_type = Pulsar::Proto::CommandSubscribe::SubType::Exclusive
      end
    end
  end
end
