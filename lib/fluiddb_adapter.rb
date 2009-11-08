require 'dm-core'
require 'dm-core/adapters/abstract_adapter'

require 'forwardable'
require 'digest/sha1'

require 'dm-fluiddb-adapter/typhoeus_client'
require 'dm-fluiddb-adapter/fixes'

module DataMapper
  module Adapters
  class FluidDBAdapter  < AbstractAdapter
    extend Forwardable

    PRIMITIVE_HEADERS = { 'Content-Type' => 'application/vnd.fluiddb.value+json' }
    # TODO split this into a TTL for tag values, and a TTL for objects
    CACHE_TTL = 600

    attr_reader :http
    def_delegators :http, :get, :post, :put, :parallel

    def initialize(*args)
      super(*args)

      raise "Authentication required" if @options['user'].nil? || @options['password'].nil?
      @http = TyphoeusClient.new(@options['host'], @options['user'], @options['password'])
    end

    def logger
      DataMapper.logger
    end

    class << self
      attr_writer :cache

      def cache
        return nil unless (@cache || defined? CACHE)

        @cache ||= CACHE
      end
    end

    def cache
      self.class.cache
    end

    def create(resources)
      parallel {
        resources.each {|resource|
          # TODO add about support
          post("/objects", {}) {|resp|
            if resp.code == 201
              resource.id = resp.body['id']
              save(resource)
            else
              raise "Failed to create object: #{resp.code}"
            end
          }
        }
      }
    end

    def update(attributes, collection)
      attributes = attributes.map_hash do |property, value|
        [ tag_name(property), value ]
      end
      list = query_ids(collection.query)

      parallel do
        list.each do |id|
          attributes.each do |tag, value|
            set_primitive_tag(id, tag, value)
          end
        end
      end

      list.each {|id| expire id }.size
    end

    # TODO is there a way we can leave the objects
    # around?
    def delete(collection)
      model = collection.query.model
      count = 0

      parallel do
        # It's unlikely that this method will receive invalid
        # ids, so don't check them. It's not a big deal if they're
        # wrong because we're not returning resource objects.
        query_ids(collection.query, false) do |ids|
          count = ids.size
          ids.each do |id|
            set_primitive_tag(id, deleted_tag(model))

            identity_tag = identity_tag(model)
            remove_tag(id, identity_tag, false) if identity_tag

            # We don't set the existance false, because the delete
            # may not mean anything if model doesn't use an identity
            # tag
            expire "existance:#{id}"
          end
        end
      end

      count
    end

    # TODO move this to the given resource
    def remove_tag(id, tag, expire=true)
      key = "#{id}/#{tag}"
      @http.delete "/objects/#{key}"
      expire(key) if expire
    end

    def read(query)
      blocking_request
      ids = query_ids(query)
      fields = query_fields(query)

      raise "Blank ids" if ids.any? {|id| id == '' || id.nil? }

      list = fetch_fields(ids, fields, !query.reload?)

      query.filter_records(list.values)
    end

    module Migration

      def storage_exists?(storage_name)
        check_namespace "#{tag_prefix}/#{storage_name}"
        # TODO error handling in here

        true
      end

      def create_model_storage(model)
        identity_tag = identity_tag(model)
        check_tag(identity_tag) if identity_tag

        check_tag(deleted_tag(model))
        model.properties.each {|field| check_dm_field(field) unless field.name == :id }
      end
      alias upgrade_model_storage create_model_storage

      def check_tag(tag, description=nil, indexed=true)
        blocking_request
        description ||= "DataMapper Created Namespace"
        match = tag.match(/^(.+)\/([^\/]+)/)
        space = match[1]
        tag_name = match[2]
        check_namespace(space)

        url = "/tags/#{tag}"
        cached = cache_get('check:'+url)

        if cached != description
          if cached.nil?
            resp = get(url, :returnDescription => true)
            if resp.code != 200
              post("/tags/#{space}", nil, :name => tag_name, :description => description, :indexed => indexed)
            end
          else
            # An error here is unimportant, it's just the description
            put(url, nil, :description => description)
          end

          cache_set('check:'+url, description, 3600)
        end
      end

      def check_namespace(namespace, description=nil)
        blocking_request

        description ||= "DataMapper Created Namespace"
        spaces = namespace.split('/')
        spaces.inject('') do |prefix, name|
          url = "/namespaces#{prefix}/#{name}"
          cached = cache_get('check:'+url)

          if cached != description
            if cached.nil?
              resp = get(url, :returnDescription => true)
              if resp.code != 200
                resp = post("/namespaces#{prefix}", nil, :name => name, :description => description)
                raise "Failed (#{resp.code}) to create namespace #{prefix}" if resp.code != 201
              end
            elsif prefix != ''
              # Don't try to change the description on top level namespaces
              # An error here is unimportant, it's just the description
              put(url, nil, :description => description)
            end

            cache_set('check:'+url, description, 3600)
          end
          
          prefix + '/' + name
        end
      end

      module SQL
        def supports_serial?
          false
        end
      end
                      
      include SQL
                              
    end # module Migration
          
    include Migration

    private

    def query_ids(query, validate_ids=true, &block)
      conditions = identity_conditions(query)
      query_string = conditions_statement(conditions)

      # It's not a good idea to mix ids in your query with other conditions
      unless (list = valid_id_list(query.model, query_string, validate_ids))
        raise "Can't run an empty query" if query_string.strip == ''
        fluiddb_query query_string do |list|
          preturn(apply_limits(list, query), &block)
        end
      else
        preturn(list, &block)
      end
    end

    def valid_id_list(model, query_string, validate=true)
      list = query_string.scan(/\|id:(.+?)\|/).flatten
      unless list.empty?
        # Sometimes we don't need to validate the ids
        return list unless validate
        # We can't do joins so, we have to go into this
        # synchronously
        blocking_request

        parallel do
          list.each_with_index do |id, index|
            is_valid_object_id?(model, id) do |valid|
              # we can't modify the array itself until
              # we are synchronous again, so just nil the
              # invalid index and we'll compact it later
              list[index] = nil unless valid
            end
          end
        end

        list.compact
      else
        false
      end
    end

    def is_valid_object_id?(model, id, &block)
      # We need an alternate key to cache existance lookups because
      # we are not caching objects which require the included fields
      # to be specified
      alt_key = "existance:#{id}"

      exists = cache_get(alt_key)
      if !exists.nil?
        preturn(exists, &block)
      else
        get "/objects/#{id}" do |resp|
          identity_tag = identity_tag(model)
          valid = (resp.code == 200 && (identity_tag.nil? || resp.body['tagPaths'].include?(identity_tag)))

          cache_set(alt_key, valid)
          preturn(valid, &block)
        end
      end
    end

    # TODO is there a way to include the identity query in the generated conditions by default?
    def identity_conditions(query)
      identity_query = identity_query(query.model)
      return query.conditions if identity_query.nil?

      andc = DataMapper::Query::Conditions::AndOperation.new
      andc << identity_query(query.model)
      andc << query.conditions unless query.conditions.nil?

      andc
    end

    # TODO be able to change the identity query
    def identity_query(model)
      identity_tag = identity_tag(model)
      "has #{identity_tag}" if identity_tag
    end

    def apply_limits(list, query)
      if query.limit
        list.slice(query.offset, query.limit)
      else
        list
      end
    end

    # TODO limit to the actual query fields
    # TODO support links
    def query_fields(query)
      tags(query.model)
    end

    # TODO be more efficient
    # TODO make sure the key column is a single string
    # TODO collect fields for conditions that fluiddb doesn't support and then build a query that loads just those fields and use the filter_records method on that and then load all the other requested fields only for the ids that matched the query
    def conditions_statement(conditions)
      case conditions
        when Query::Conditions::AbstractOperation
          operation_statement(conditions)

        when Query::Conditions::AbstractComparison
          comparison_statement(conditions)

        when String
          conditions # handle raw conditions
      end
    end

    # TODO add support for set matching with the inclusion operator
    def comparison_statement(comparison)
      value = comparison.value

      if comparison.subject && comparison.subject.name == :id
        # This gets scanned out at the query level
        return "|id:#{value}|"
      end

      # break exclusive Range queries up into two comparisons ANDed together
      if value.kind_of?(Range)
        operation = Query::Conditions::Operation.new(:and,
          Query::Conditions::Comparison.new(:gte, comparison.subject, value.first),
          Query::Conditions::Comparison.new((value.exclude_end? ? :lt : :lte),  comparison.subject, value.last)
        )

        return conditions_statement(operation)
      elsif comparison.relationship?
        #return conditions_statement(comparison.foreign_key_mapping, qualify)
        # I think this is for joins which we don't support in queries
        raise NotImplementedError.new("Joins not supported in object lookups")
      end

      operator = case comparison
        when Query::Conditions::EqualToComparison              then equality_operator(comparison.subject, value)
#        when Query::Conditions::InclusionComparison            then include_operator(comparison.subject, value)
#        when Query::Conditions::RegexpComparison               then regexp_operator(value)
#        when Query::Conditions::LikeComparison                 then like_operator(value)
        when Query::Conditions::GreaterThanComparison          then '>'
        when Query::Conditions::LessThanComparison             then '<'
        when Query::Conditions::GreaterThanOrEqualToComparison then '>='
        when Query::Conditions::LessThanOrEqualToComparison    then '<='
      end

      subject = comparison.subject.nil? ? '' : tag_name(comparison.subject)
      operator.nil? ? '' : "#{subject} #{operator} #{value}"
    end

    def equality_operator(subject, value)
      if value.is_a?(Numeric)
        '=' 
      else
        # TODO they don't support text matching yet
      end
    end

    def operation_statement(operation)
      operands = operation.operands
      statements  = []

      if operands.any? {|x| x.is_a?(Query::Conditions::NotOperation) }
        case operation
          when Query::Conditions::AndOperation
            affirmatives = operands.select {|x| ! x.is_a?(Query::Conditions::NotOperation) }
            statements << multipart_operation(Query::Conditions::AndOperation.new(*affirmatives))

            negatives = operands - affirmatives
            statements << multipart_operation(Query::Conditions::AndOperation.new(*(negatives.map(&:operands).flatten)))

            join_with = 'except'
          when Query::Conditions::OrOperation then 'or'
            raise "Query builder can't build a negative-or, please write the query by hand"
        end
      else
        operands.each do |operand|
          statements << multipart_operation(operand)
        end

        join_with = case operation
          when Query::Conditions::AndOperation then 'and'
          when Query::Conditions::OrOperation then 'or'
        end
      end

      statements.delete_if {|x| x.nil? || x.strip == '' }

      joined = statements.join(" #{join_with} ")
      joined.strip == '' ? '' : joined
    end

    def multipart_operation(operand)
      statement = conditions_statement(operand)

      if (operand.respond_to?(:operands) && operand.operands.size > 1) || operand.kind_of?(Query::Conditions::InclusionComparison) &&
         statement.strip != ''
        statement = "(#{statement})"
      end

      statement
    end

    def fluiddb_query(query, &block)
      get '/objects', :query => query do |resp|
        if resp.code != 200
          raise "Failed to run query: #{query}"
        else
          preturn(resp.body['ids'], &block)
        end
      end
    end

    # Set use_cache = false to reload all the data from FluidDB. The
    # resulting values will still be stored in memcache when available.
    def fetch_fields(ids, fields, use_cache=true)
      ids = ids.dup
      dataset = {}

      if use_cache
        keys = ids.map_hash do |id|
          [ object_cache_key(id,fields), id ]
        end

        results = cache_get(keys.keys) || []
        results.each do |key, attrs|
          if attrs
            id = keys[key]
            ids.delete(id)
            dataset[id] = attrs
          end
        end
      end

      parallel do
        ids.each do |id|
          row = { 'id' => id }
          dataset[id] = row

          requests = fields.map_hash do |field, tag|
            [ "#{id}/#{tag}", field ]
          end

          if use_cache
            ret = cache_get(requests.keys) || []
            ret.each do |key,value|
              row[requests.delete(key)] = value if value
            end
          end

          requests.each do |key, field|
            get("/objects/#{key}") do |resp|
              if [ 200, 404 ].include?(resp.code)
                cache_set key, resp.body
                row[field] = resp.body
              else
                # TODO raise a fetch error, or try again
              end
            end
          end
        end
      end

      ids.each do |id|
        cache_set object_cache_key(id,fields), dataset[id]
      end

      dataset
    end

    # TODO some peice of code is passing the id in here
    def object_cache_key(id, fields)
      fields = fields.values if fields.is_a?(Hash)
      fields.compact!
      fields.sort!

      raise "don't use the id in the cache key" if fields.any? {|x| x[/\/id$/] }
      Digest::SHA1.hexdigest("id:#{id}:fields:#{fields.join(',')}")
    end

    def save(resource)
      identity_tag = identity_tag(resource.model)
      set_primitive_tag(resource, identity_tag) if identity_tag

      serialized = serialize(resource)
      serialized.each do |tag, value|
        set_primitive_tag(resource, tag, value)
      end

      cache_key = object_cache_key(resource.id, serialized.keys)
      cache_set cache_key, serialized.merge(:id => resource.id)
    end

    def serialize(resource)
      hash = {}

      resource.model.properties.each do |property|
        if resource.model.public_method_defined?(name = property.name) && property.name != :id
          tag = tag_name(property)
          hash[tag] = primitive_value(resource, name)
        end
      end

      hash
    end

    # TODO better type handling, like dates
    def primitive_value(resource, name)
      resource.send(name)
    end

    def check_dm_field(property)
      tag = tag_name(property)
      description = property.options[:description]
      check_tag(tag, description)
    end

    def set_primitive_tag(id, tag, value=nil)
      id = id.id if id.is_a?(Resource)

      key = "#{id}/#{tag}"
      cache_set key, value
      encoded = value.nil? ? "null" : value.to_json

      put "/objects/#{key}",
          nil, encoded, PRIMITIVE_HEADERS
      # TODO raise a persistance error on failure
    end

    # TODO return nil if the model doesn't have an identity tag
    def identity_tag(model)
      "#{tag_prefix}/#{model.storage_name(name)}"
    end

    def deleted_tag(model)
      "#{tag_prefix}/#{model.storage_name(name)}/deleted"
    end

    # TODO change the field name strategy to automatically map to tag names
    def tag_name(property)
      field = property.field

      if field.index('/')
        field
      else
        "#{tag_prefix}/#{property.model.storage_name(name)}/#{field}"
      end
    end

    # TODO change the field name strategy to automatically map to tag names
    def tags(model)
      @tags ||= model.properties.map_hash do |property|
        next if property.name == :id
        [ property.field, tag_name(property) ]
      end
    end

    def preturn(value, &block)
      if block_given?
        yield value
      else
        value
      end
    end

    def blocking_request
      raise "Tried to make blocking request in parallel block" if @http.in_parallel?

      yield if block_given?
    end

    def tag_prefix
      @tag_prefix ||= @options['user']+'/'+@options['path'][1..-1]
    end

    def cache_get(key)
      return nil if key.empty?
      cache.get key rescue nil
    end

    def cache_set(key, value, ttl=CACHE_TTL)
      raise "Failed to store empty memcache key" if key.empty?
      cache.set(key, value, ttl) rescue nil
    end

    def expire(key)
      cache.delete key rescue nil
    end
  end

  FluiddbAdapter = FluidDBAdapter

  DataMapper.extend(Migrations::SingletonMethods)
  [ :Repository, :Model ].each do |name|
    DataMapper.const_get(name).send(:include, Migrations.const_get(name))
  end

  const_added(:FluiddbAdapter)
  end
end
