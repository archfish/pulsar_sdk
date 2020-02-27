module PulsarAdmin
  class Api
    PublishTimeHeader = /^X-Pulsar-Publish-Time$/i
    BatchHeader       = /^X-Pulsar-Num-Batch-Message$/i
    PropertyPrefix    = /^X-Pulsar-PROPERTY-/i

    # opts
    #   endpoint
    #   tenant
    #   persistent
    def initialize(opts)
      @endpoint = URI.parse(opts[:endpoint])
      @tenant = opts[:tenant]
      @persistent = opts[:persistent] == false ? 'non-persistent' : 'persistent'
    end

    def list_namespaces
      get('/admin/v2/namespaces/:tenant')
    end

    def create_namespace(name)
      put('/admin/v2/namespaces/:tenant/:namespace', namespace: name)
    end

    def namespace_topics(namespace)
      result = {}
      ['', '/partitioned'].flat_map do |pd|
        resp = get("/admin/v2/:persistent/:tenant/:namespace#{pd}", namespace: namespace)
        result[pd.empty? ? 'non-partitioned' : 'partitioned'] = resp
      end
      result
    end

    def create_topic(namespace, topic, partitions = 0)
      put("/admin/v2/:persistent/:tenant/:namespace/:topic#{partitions.zero? ? '' : '/partitions'}",
            {namespace: namespace, topic: topic}, partitions
          )
    end

    def delete_topic(namespace, topic)
      res1 = delete('/admin/v2/:persistent/:tenant/:namespace/:topic',
              namespace: namespace, topic: topic
            )

      res2 = delete('/admin/v2/:persistent/:tenant/:namespace/:topic/partitions',
        namespace: namespace, topic: topic
      )

      res1 || res2
    end

    # options
    #   namespace
    #   topic
    #   sub_name
    #   message_position
    #   count
    def peek_messages(options)
      opts = options.dup
      (opts[:count] || 1).times.map do |x|
        opts[:message_position] = (opts[:message_position].to_i + x + 1).to_s
        peek_message(opts)
      end.compact
    end

    private
    def put(path, payload = {}, body = nil)
      uri = @endpoint.dup
      uri.path, payload = handle_restful_path(path, payload)

      req = Net::HTTP::Put.new(uri)

      if payload.empty?
        req.body = body.to_s
        req.content_type = 'text/plain'
      else
        req.body = payload.to_json
        req.content_type = 'application/json'
      end

      res = Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req)
      end

      case res
      when Net::HTTPSuccess, Net::HTTPNoContent
        return true
      else
        puts "status: #{res.code} - body: #{res.body} - #{res.inspect}"
        return false
      end
    end

    def get(path, params = {})
      uri = @endpoint.dup
      uri.path, params = handle_restful_path(path, params)

      req = Net::HTTP::Get.new(uri)
      req.set_form_data(params) unless params.empty?

      resp = Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req)
      end

      try_decode_body(resp)
    end

    def delete(path, payload = {})
      uri = @endpoint.dup
      uri.path, payload = handle_restful_path(path, payload)

      req = Net::HTTP::Delete.new(uri)
      req.body = payload.to_json
      req.content_type = 'application/json'

      res = Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req)
      end

      case res
      when Net::HTTPSuccess, Net::HTTPNoContent
        return true
      else
        return false
      end
    end

    # options
    #   namespace
    #   topic
    #   sub_name
    #   message_position
    def peek_message(options)
      options[:message_position] = options[:message_position].to_s
      resp = get('/admin/v2/:persistent/:tenant/:namespace/:topic/subscription/:sub_name/position/:message_position', options)
      unless request_ok?(resp)
        puts resp.body
        return
      end

      payload = resp.body

      msg_id = nil
      properties = {}
      resp.each_header do |header|
        case header
        when PublishTimeHeader
          properties['publish-time'] = resp.header[header]
        when BatchHeader
          properties['pulsar-num-batch-message'] = resp.header[header]
        when PropertyPrefix
          properties[header] = resp.header[header]
        when /^X-Pulsar-Message-ID$/i
          msg_id = resp.header[header]
        end
      end
      [
        msg_id,
        properties,
        payload
      ]
    end

    def handle_restful_path(path, options)
      return path unless path.include?(':')

      opts = combine_default_value(options || {})
      opts.keys.sort.reverse.each do |k|
        remark = ":#{k}"
        next unless path.include?(remark)
        path.gsub!(remark, opts[k])
        options.delete(k)
      end

      return [path, options]
    end

    def combine_default_value(opts)
      opts.merge(
        tenant: @tenant,
        persistent: @persistent
      )
    end

    def request_ok?(resp)
      case resp
      when Net::HTTPSuccess
        true
      when Net::HTTPRedirection
        false
      else
        false
      end
    end

    def try_decode_body(resp)
      unless request_ok?(resp)
        puts resp.body
        return
      end
      return resp.body unless resp.content_type =~ /application\/json/

      JSON.parse(resp.body) rescue resp.body
    end
  end
end
