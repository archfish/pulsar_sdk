require 'pulsar_sdk/consumer/base'
require 'pulsar_sdk/consumer/message_tracker'
require 'pulsar_sdk/consumer/manager'

module PulsarSdk
  module Consumer
    extend self

    def create(client, opts)
      PulsarSdk::Consumer::Manager.new(client, opts)
    end
  end
end
