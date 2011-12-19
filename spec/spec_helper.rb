require 'rspec'
require 'bundler/setup'
require 'logger'

$: << File.expand_path(File.join(File.dirname(__FILE__),'..','lib'))

require 'rz/version'
require 'rz/pull_worker'
require 'rz/client'
require 'rz/service'

# Overriding RZ::Context#log to get log messages

module RZ::Context
  def logger
    @logger ||= Logger.new $stderr
  end
  def log(level,&block)
    logger.send level,&block
  end
end
