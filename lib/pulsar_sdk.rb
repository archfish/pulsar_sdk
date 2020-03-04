require 'logger'
require 'json'
require 'uri'
require 'protobuf/validate'
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

  # options Hash see PulsarSdk::Options::Connection for detail
  def create_client(options)
    opts = ::PulsarSdk::Options::Connection.new(options)
    ::PulsarSdk::Client.create(opts)
  end

  # options Hash see PulsarSdk::Options::Producer for detail
  def create_producer(client, options)
    opts = ::PulsarSdk::Options::Producer.new(options)
    client.create_producer(opts)
  end

  # options Hash see PulsarSdk::Options::Consumer for detail
  def create_consumer(client, options)
    opts = ::PulsarSdk::Options::Consumer.new(options)
    client.subscribe(opts)
  end

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
