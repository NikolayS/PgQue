# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "lib/pgque/version"

Gem::Specification.new do |spec|
  spec.name        = "pgque"
  spec.version     = Pgque::VERSION
  spec.authors     = ["Nikolay Samokhvalov", "Dalto Curvelano Jr"]
  spec.email       = ["nik@postgres.ai", "daltojr@gmail.com"]

  spec.summary     = "Ruby client for PgQue -- PgQ Universal Edition"
  spec.description = "Thin Ruby wrapper over the pgque SQL API: send, " \
                     "send_batch, receive, ack, nack, force_next_tick, " \
                     "plus a polling Consumer with LISTEN/NOTIFY wakeup."
  spec.homepage    = "https://github.com/NikolayS/pgque"
  spec.license     = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"]      = spec.homepage
  spec.metadata["source_code_uri"]   = spec.homepage
  spec.metadata["bug_tracker_uri"]   = "#{spec.homepage}/issues"
  spec.metadata["documentation_uri"] = "#{spec.homepage}/blob/main/docs/reference.md"
  spec.metadata["changelog_uri"]     = "#{spec.homepage}/releases"

  spec.files = Dir.glob("lib/**/*.rb") +
               %w[README.md LICENSE].select { |f| File.exist?(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "pg", ">= 1.5", "< 2.0"

  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "rake",     "~> 13.0"
end
