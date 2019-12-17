require 'logger'
require 'json'
require 'protobuf/pulsar_api.pb'
require "pulsar_sdk/version"
require 'pulsar_sdk/tweaks'
require 'pulsar_sdk/protocol'
require 'pulsar_sdk/options'
require 'pulsar_sdk/consumer'
require 'pulsar_sdk/producer'
require 'pulsar_sdk/client'

module PulsarSdk
  extend self

  def logger
    @logger ||= Logger.new(STDOUT).tap do |logger|
                  logger.formatter = Formatter.new
                end
  end

  def logger=(v)
    @logger = v
  end

  class Formatter < ::Logger::Formatter
    def call(severity, timestamp, progname, msg)
      case msg
      when ::StandardError
        msg = [msg.message, msg&.backtrace].join(":\n")
      end

      super
    end
  end
end
