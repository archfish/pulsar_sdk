require 'net/http'
require 'pulsar_admin/api'

module PulsarAdmin
  extend self

  # options
  #   endpoint
  #   tenant
  #   persistent
  def create_client(options)
    PulsarAdmin::Api.new(options)
  end
end
