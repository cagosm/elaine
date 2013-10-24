# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "elaine/version"

Gem::Specification.new do |s|
  s.name        = "elaine"
  s.version     = Elaine::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Jeremy Blackburn"]
  s.email       = ["jeremy.blackburn@gmail.com"]
  s.homepage    = "http://bitbucket.org/worst/elaine"
  s.summary     = "Distribtuted implementation of Google's Pregel framework for large-scale graph processing. Forked from http://github.com/igrigorik/pregel"
  s.description = s.summary
  s.rubyforge_project = "elaine"

  s.add_development_dependency "rspec"
  s.add_dependency "celluloid-io"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
