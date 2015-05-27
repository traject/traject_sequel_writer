# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'traject_sequel_writer/version'

Gem::Specification.new do |spec|
  spec.name          = "traject_sequel_writer"
  spec.version       = TrajectSequelWriter::VERSION
  spec.authors       = ["Jonathan Rochkind"]
  spec.email         = ["jonathan@dnil.net"]

  spec.summary       = %q{Plug-in for traject, write to an rdbms with Sequel gem}
  spec.homepage      = "TODO: Put your gem's website or public repo URL here."
  spec.license       = "MIT"


  spec.files = Dir["{lib,bin}/**/*", "LICENSE.txt", "Rakefile", "README.md"]
  spec.test_files = Dir["test/**/*"]

  spec.require_paths = ["lib"]

  spec.add_dependency "traject", "~> 2.0"
  spec.add_dependency "sequel", "~> 4.22"

  spec.add_development_dependency "minitest"
  spec.add_development_dependency "bundler", "~> 1.9"
  spec.add_development_dependency "rake", "~> 10.0"
end
