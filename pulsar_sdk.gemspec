
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "pulsar_sdk/version"

Gem::Specification.new do |spec|
  spec.name          = "pulsar_sdk"
  spec.version       = PulsarSdk::VERSION
  spec.authors       = 'archfish'
  spec.email         = ["weihailang@gmail.com"]
  spec.license       = 'Apache License 2.0'

  spec.summary       = %q{A pure ruby client for Apache Pulsar}
  spec.description   = %q{A pure ruby client for Apache Pulsar}
  spec.homepage      = "https://github.com/archfish/pulsar_sdk"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features|examples)/}) }
  end

  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'google-protobuf', '~> 3.10'
  spec.add_dependency 'digest-crc', '~> 0.4'

  spec.add_development_dependency "bundler", "> 1.17"
end
