module PulsarSdk
  module Options
    class Producer < Base
      attr_accessor :topic, :name, :router

      private
      def set_default
        self.name = 'ruby-producer.' + SecureRandom.urlsafe_base64(10)
        self.router = PulsarSdk::Producer::Router.new
      end
    end
  end
end
