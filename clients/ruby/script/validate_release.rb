# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "rubygems"
require "rubygems/package"
require_relative "../lib/pgque/version"

abort "usage: validate_release.rb VERSION TAG_NAME [GEM_PATH]" unless [2, 3].include?(ARGV.length)

version, tag_name, gem_path = ARGV

unless version.match?(/\A\d+\.\d+\.\d+(?:\.[0-9A-Za-z]+)*\z/) &&
       Gem::Version.correct?(version) && Gem::Version.new(version).to_s == version
  abort "invalid canonical RubyGems version: #{version.inspect}"
end

expected_tag = "ruby/v#{version}"
unless tag_name == expected_tag
  abort "tag must be #{expected_tag.inspect}, got #{tag_name.inspect}"
end
unless Pgque::VERSION == version
  abort "version input #{version.inspect} != Pgque::VERSION #{Pgque::VERSION.inspect}"
end

if gem_path
  expected_file = "pgque-#{version}.gem"
  unless File.basename(gem_path) == expected_file
    abort "gem filename must be #{expected_file.inspect}, got #{File.basename(gem_path).inspect}"
  end
  abort "gem artifact not found: #{gem_path}" unless File.file?(gem_path)

  spec = Gem::Package.new(gem_path).spec
  abort "gem name must be \"pgque\", got #{spec.name.inspect}" unless spec.name == "pgque"
  abort "gem version #{spec.version} != expected #{version}" unless spec.version.to_s == version
end

source = gem_path ? " from #{gem_path}" : ""
puts "release candidate verified: pgque #{version} (#{expected_tag})#{source}"
