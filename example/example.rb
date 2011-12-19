#!/usr/bin/env ruby

$: << File.expand_path(File.join(File.dirname(__FILE__),'..','lib'))

require 'rz/client'
require 'rz/service'
require 'rz/pull_worker'
require 'rz/service/statistics'

class Client
  include RZ::Client

  # overriding log noop log, this interface needs to improve
  def rz_log(level,&block)
    puts "#{level}: #{block.call}"
  end

  def run
    yield self
  ensure
    rz_cleanup
  end

  def initialize(options)
    options.merge!(:rz_identity => "client-#{Process.pid}")
    initialize_client(options)
  end
end


class Service
  include RZ::Service
  include RZ::Service::Statistics

  # overriding log noop log, this interface needs to improve
  def rz_log(level,&block)
    puts "#{level}: #{block.call}"
  end

  def initialize(options)
    options.merge!(:rz_identity => "service-#{Process.pid}")
    initialize_service(options)
  end
end


class Worker
  include RZ::PullWorker

  def initialize(options)
    options.merge!(:rz_identity => "worker-#{Process.pid}")
    initialize_worker(options)
  end

  # overriding log noop log, this interface needs to improve
  def rz_log(level,&block)
    puts "#{level}: #{block.call}"
  end

  register :eval do |string|
    eval string
  end
end

module Example
  def self.addresses
    { 
      :a => {
        :response_address  => 'tcp://127.0.0.1:8001',
        :request_address_a => 'tcp://127.0.0.1:8002',
        :request_address_b => 'tcp://127.0.0.1:8003',
        :frontend_address  => 'tcp://127.0.0.1:8000'
      },
      :b => {
        :response_address  => 'tcp://127.0.0.1:8010',
        :request_address_a => 'tcp://127.0.0.1:8020',
        :request_address_b => 'tcp://127.0.0.1:8030',
        :frontend_address  => 'tcp://127.0.0.1:8000'
      },
      :c => {
        :response_address  => 'tcp://127.0.0.1:8100',
        :request_address_a => 'tcp://127.0.0.1:8200',
        :request_address_b => 'tcp://127.0.0.1:8300',
        :frontend_address  => 'tcp://127.0.0.1:8000'
      },
    }
  end

  def self.options_for(type,name)
    addresses = self.addresses.fetch(name) do 
      raise ArgumentError,"address for: #{name} does not exist"
    end
    addresses.merge :identity => "#{type}-#{name}-#{Process.pid}" 
  end
end

