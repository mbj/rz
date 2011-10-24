require 'rspec'
require 'bundler/setup'

$: << File.expand_path(File.join(File.dirname(__FILE__),'..','lib'))

require 'rz/version'
require 'rz/worker'
require 'rz/client'
require 'rz/service'
