# -*- encoding: utf-8 -*-
require File.expand_path('../lib/rz/version', __FILE__)

Gem::Specification.new do |s|
  s.name = 'rz'
  s.version = RZ::VERSION

  s.authors  = ['Markus Schirp']
  s.email    = 'mbj@seonic.net'
  s.date     = '2011-11-16'
  s.summary  = 'zmq jobserver'
  s.homepage = 'http://github.com/mbj/rz'

  s.files            = `git ls-files`.split("\n")
  s.test_files       = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths    = [ 'lib' ]
  s.extra_rdoc_files = [ 'LICENSE', 'README.rdoc', 'TODO' ]

  s.add_runtime_dependency(%q<zmq>,         ["~> 1.4.0"])
  s.add_runtime_dependency(%q<dm-core>,       ["~> 1.2.0.rc2"])
  s.add_runtime_dependency(%q<dm-migrations>, ["~> 1.2.0.rc2"])
  s.add_runtime_dependency(%q<dm-aggregates>, ["~> 1.2.0.rc2"])

  s.add_development_dependency(%q<rake>,      ["~> 0.8.7"])
  s.add_development_dependency(%q<rspec>,     ["~> 1.3.1"])
end
