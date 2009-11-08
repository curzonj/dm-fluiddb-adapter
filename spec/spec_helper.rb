$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'rubygems'
require 'spec'
require 'spec/autorun'
require 'fluiddb_adapter'
require 'memcached'

DATABASE_URL = "fluiddb://test:test@sandbox.fluidinfo.com/test"
CACHE = Memcached.new 'localhost:11211', :prefix_key => 'fluiddb-test:'

Loggable.logger = Logger.new(nil)
DataMapper.logger = Loggable.logger

Spec::Runner.configure {|config|}

Typhoeus::Hydra.class_eval do
  def stub_response(method, url, args={})
    args = { :code => 204, :headers => "", :body => '', :time => 0.5 }.merge(args)
    response = Typhoeus::Response.new(args)
    stub(method, 'http://sandbox.fluidinfo.com' + url).and_return(response)
  end
end
