require 'spec/spec_helper'


describe TyphoeusClient do 
  before(:each) do
    adapter = DataMapper.setup(:default, DATABASE_URL)
    # This is the easiest way to get the config data out
    # of the DATABASE_URL for the tests
    @handler = adapter.instance_variable_get("@http")
  end

  describe :request do
    it 'should work' do
      @handler.hydra.stub_response(:get, '/objects')
      @handler.request(:get, '/objects')
    end
  end

  [ :get, :post, :put, :delete ]. each do |method|
    describe method do 
      it "should respond to #{method}" do
        @handler.respond_to?(method).should == true
      end

      it 'should work' do
        @handler.hydra.stub_response(method, '/objects')

        resp = @handler.send(method, '/objects')
        resp.code.should == 204
      end
    end
  end
end
