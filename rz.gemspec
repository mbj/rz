# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
 
require 'rz/version'
 
Gem::Specification.new do |s|
  s.name        = 'rz'
  s.version     = RZ::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = 'Markus Schirp'
  s.email       = 'mbj@seonic.net'
  s.homepage    = 'http://github.com/mbj/rz'
  s.summary     = 'jobserver build on top of zmq'
 
  s.required_rubygems_version = '>= 1.3.6'
  #s.rubyforge_project         = 'bundler'
 
  s.add_runtime_dependency 'rz', '~> 2.1.4'
  s.add_development_dependency 'rspec'
 
  s.files        = Dir.glob("{bin,lib}/**/*") + %w(LICENSE README.md)
  s.require_path = 'lib'
end
