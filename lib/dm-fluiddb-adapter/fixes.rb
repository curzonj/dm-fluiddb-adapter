module DataMapper
  module Resource
    def original_attributes
      if frozen?
        {}
      else
        @original_attributes ||= {}
      end
    end
  end
end

module Enumerable
  def map_hash(&block)
    hash = {}

    list = map(&block)

    list.each do |result|
      if result.is_a?(Array)
        hash[result.first] = result.last
      elsif result.is_a?(Hash)
        hash.merge!(result)
      else
        hash[result] = result
      end
    end
    
    hash
  end
end
