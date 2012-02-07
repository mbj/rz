# -*- encoding: utf-8 -*-
require File.expand_path('../lib/rz/version', __FILE__)

Gem::Specification.new do |s|
  s.name = 'rz'
  s.version = RZ::VERSION.dup

  s.authors  = ['Markus Schirp']
  s.email    = 'mbj@seonic.net'
  s.date     = '2011-11-16'
  s.summary  = 'zmq jobserver'
  s.homepage = 'http://github.com/mbj/rz'

  s.files            = `git ls-files`.split("\n")
  s.test_files       = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths    = %w(lib)
  s.extra_rdoc_files = %w(LICENSE) 

  s.add_runtime_dependency('zmq',         '~> 2.1.4')

  s.add_development_dependency('rspec',   '~> 2.7.0')
  s.add_development_dependency('rake',      '~> 0.9.2')
  s.add_development_dependency('backports', '~> 2.3.0')
  s.add_development_dependency('rspec',     '~> 2.7.0')
  s.add_development_dependency('reek',     '~> 2.7.0')
end
