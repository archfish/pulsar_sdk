module PulsarSdk
  module Options
    class Producer < Base
      attr_accessor :topic, :name

      private
      def set_default
        self.name = 'ruby-producer.' + SecureRandom.urlsafe_base64(10)
      end
    end
  end
end
