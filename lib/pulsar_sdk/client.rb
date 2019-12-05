require 'pulsar_sdk/client/rpc'
require 'pulsar_sdk/client/connection'
require 'pulsar_sdk/client/connection_pool'

module PulsarSdk
  module Client
    extend self

    def create(opts)
      PulsarSdk::Client::Rpc.new(opts)
    end
  end
end
