require 'pulsar_sdk/producer/base'
require 'pulsar_sdk/producer/message'
require 'pulsar_sdk/producer/router'
require 'pulsar_sdk/producer/manager'

module PulsarSdk
  module Producer
    extend self

    def create(client, opts)
      PulsarSdk::Producer::Manager.new(client, opts)
    end
  end
end
