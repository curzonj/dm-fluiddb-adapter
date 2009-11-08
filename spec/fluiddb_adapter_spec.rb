require 'spec/spec_helper'

describe DataMapper::Adapters::FluidDBAdapter do
  before :all do
    @adapter = DataMapper.setup(:default, DATABASE_URL)
    @repository = DataMapper.repository(:default)

    @http = mock('http client')
    @adapter.instance_variable_set("@http", @http)

    @cache = mock('memcache client')
    DataMapper::Adapters::FluidDBAdapter.cache = @cache

    class Flinstone
      include DataMapper::Resource

      property :id, String, :key => true
      property :name, String
      property :age, Integer
    end
  end

  def tag(name)
    prefix = @adapter.send(:tag_prefix) + '/flinstones'
    name.nil? ? prefix : (prefix + '/' + name)
  end

  after :all do
    DataMapper::Adapters::FluidDBAdapter.cache = CACHE
  end

  describe :create do

  end

  describe :read do

  end

  describe :delete do

  end

  describe :check_tag do

  end

  describe :check_namespace do

  end

  describe :paged_query do

  end

  describe :update do

  end

  describe :conditions_statement do
    def test(c)
      @adapter.send(:conditions_statement, c)
    end

    before(:all) do
      @id = Flinstone.properties[:id]
      @name = Flinstone.properties[:name]
      @age = Flinstone.properties[:age]
    end

    it 'should extract ids' do
      cond = DataMapper::Query::Conditions::EqualToComparison.new(@id, 'wilma')
      test(cond).should == '|id:wilma|'
    end

    it 'should handle inclusion comparisons' do
      andc = DataMapper::Query::Conditions::AndOperation.new
      andc << DataMapper::Query::Conditions::InclusionComparison.new(@name, 1...6)

      test(andc).should == "(#{tag('name')} >= 1 and #{tag('name')} < 6)"
    end

    it 'should handle negative operators' do
      notc = DataMapper::Query::Conditions::NotOperation.new
      notc << DataMapper::Query::Conditions::EqualToComparison.new(@age, 78)
      andc = DataMapper::Query::Conditions::AndOperation.new
      andc << "has #{tag(nil)}"
      andc << notc

      test(andc).should == "has #{tag(nil)} except #{tag('age')} = 78"
    end
  end

  describe :query_ids do
    it 'should parse extracted ids' do
      #@adapter.stub(:blocking_request)

      q = DataMapper::Query.new(@repository, Flinstone, :id => 'wilma')
      @adapter.send(:query_ids, q, false).should == [ 'wilma' ]
    end

    it 'should add the identity query' do
      @adapter.should_receive(:fluiddb_query).with("has #{tag(nil)}").and_yield(['list', 'of', 'ids'])

      q = DataMapper::Query.new(@repository, Flinstone)
      @adapter.send(:query_ids, q).should == [ 'list', 'of', 'ids' ]
    end
  end

  describe :fetch_fields do
    def mock_multi_get(keys, values=nil)
      result = keys.map_hash do |k|
        value =values.nil? ? nil : values[keys.index(k)]
        [ k, value ]
      end

      @cache.should_receive(:get) {|args|
        args.sort.should == keys.sort
        result
      }
    end

    before(:each) do
      @ids = [ 'id1', 'id2' ]
      @fields = [ 'description', 'path' ]
      @tags = @fields.map_hash {|f| [ f, 'fluiddb/tags/'+f] }
      @cached = @ids.map_hash do |id|
        [ @adapter.send(:object_cache_key, id, @tags.values),
          @fields.map_hash.merge("id"=>id) ]
      end

      @http.should_receive(:parallel).and_yield
    end

    describe 'when objects are cached' do
      it 'should not fetch any fields' do
        mock_multi_get(@cached.keys, @cached.values)
        @adapter.send(:fetch_fields, @ids, @tags)
      end
    end

    describe 'when objects are not cached' do
      before(:each) do
        mock_multi_get(@cached.keys)

        @ids.each do |id|
          key = @adapter.send(:object_cache_key, id, @tags.values)
          @cache.should_receive(:set).with(key, @fields.map_hash.merge("id"=>id), 600)
        end
      end

      it 'should fetch fields when not cached' do
        @ids.each do |id|
          mock_multi_get @fields.map {|f| id+'/fluiddb/tags/'+f }

          @fields.each do |field|
            resp = mock(:code => 200, :body => field)
            @http.should_receive(:get).with("/objects/#{id}/fluiddb/tags/#{field}").and_yield(resp)
            @cache.should_receive(:set).with("#{id}/fluiddb/tags/#{field}", field, 600)
          end
        end

        @adapter.send(:fetch_fields, @ids, @tags)
      end

      it 'should not fetch fields when cached' do
        @ids.each do |id|
          values = @fields.map_hash {|f| [ id+'/fluiddb/tags/'+f, f ] }
          mock_multi_get(values.keys, values.values)
        end

        @adapter.send(:fetch_fields, @ids, @tags)
      end
    end
  end

  describe :save do

  end

  describe :serialize do

  end

  describe :tags do

  end

end
