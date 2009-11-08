require 'logger'

module Loggable
  def self.logger=(global)
    @logger = global
  end

  def self.logger
    @logger ||= Logger.new(STDERR)
  end

  def self.included(base)
    base.class_eval do
      def logger=(l)
        @logger = l
      end

      def logger
        @logger ||= Loggable.logger
      end
    end
  end

  def logger
    self.class.logger
  end
end
