require 'typhoeus'
require 'loggable'
require 'base64'
require 'json'

class TyphoeusClient
  include Loggable

  DEFAULT_HEADERS = {'Content-Type' => 'application/json', 'Accept' => '*/*'}
  TIMEOUT = 10000

  def initialize(host='sandbox.fluidinfo.com', username='test', password='test')
    @host = host
    @username = username
    @password = password

    @bulk = false
  end

  def parallel
    if in_parallel?
      yield
    else
      @bulk = true
      yield
      hydra.run
      @bulk = false
    end
  end

  [ :get, :post, :delete ].each do |method|
    class_eval %{
      def #{method}(uri, params=nil, payload=nil, headers={}, &block)
        request(:#{method}, uri, params, payload, headers, &block)
      end
    }, __FILE__, __LINE__
  end

  def put(uri, params=nil, payload=nil, headers={}, &block)
    if payload && payload.respond_to?(:read) && payload.respond_to?(:size) && headers['Content-Type']
      streaming_put(uri, payload, headers, &block)
    else
      request(:put, uri, params, payload, headers, &block)
    end
  end

  def streaming_post(uri, payload, headers, &block)
    raise NotImplementedError
  end

  def request(method, path, params=nil, payload=nil, headers={}, &block)
    headers = DEFAULT_HEADERS.merge(headers)
    headers["Authorization"] = "Basic #{Base64.encode64("#{@username}:#{@password}")}".strip

    opts = {
      :method => method,
      :params => params,
      :headers => headers,
      :timeout => TIMEOUT
    }

    opts[:params] ||= {} if method == :post

    if payload
      # We already merged the Content-Type into the headers
      if headers['Content-Type'] == 'application/json'
        opts[:body] = payload.to_json
      else
        opts[:body] = payload.to_s
      end
    end

    url = 'http://' + @host + path
    req = Typhoeus::Request.new(url, opts)

    if in_parallel?
      req.on_complete {|r| response(req, r, &block) }
      hydra.queue req
      req
    else
      hydra.queue req
      hydra.run
      response(req, req.response, &block)
    end
  end

  def response(request, response)
    raise "Failed to connect to #{request.url}" if response.code == 0

    headers = {}
    response.headers.split("\r\n").each {|header|
      if header =~ /^(.+?): (.*)$/
        headers[$1] = $2
      end
    }
    response.instance_variable_set("@headers", headers)

    unless (200..299).include?(response.code)
      logger.warn "(#{response.code}) #{request.method.to_s.capitalize} #{request.url} -- #{response.headers.inspect}"
    end

    body = if headers['Content-Type'] == "application/json"
      JSON.parse(response.body)
    elsif headers['Content-Type'] == "application/vnd.fluiddb.value+json"
      JSON.parse('[' + response.body + ']').first
    else
      response.body
    end
    response.instance_variable_set("@body", body)

    if block_given?
      yield response
    else
      response
    end
  end

  def in_parallel?
    @bulk == true
  end

  def hydra
    @hydra ||= Typhoeus::Hydra.new
  end
end

