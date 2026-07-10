# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "json"
require "rubygems"

module PgqueRelease
  module RubyGemsVersionCheck
    module_function

    VERSION_PATTERN = /\A\d+\.\d+\.\d+(?:\.[0-9A-Za-z]+)*\z/

    def check!(version, response)
      unless VERSION_PATTERN.match?(version) &&
             Gem::Version.correct?(version) && Gem::Version.new(version).to_s == version
        raise ArgumentError, "invalid canonical RubyGems version: #{version.inspect}"
      end

      versions = JSON.parse(response)
      unless versions.is_a?(Array)
        raise ArgumentError, "invalid RubyGems versions response: expected a JSON array"
      end

      published_versions = versions.each_with_index.map do |entry, index|
        number = entry["number"] if entry.is_a?(Hash)
        unless number.is_a?(String) && !number.empty?
          raise ArgumentError,
                "invalid RubyGems versions response: entry #{index} has no string number"
        end
        number
      end

      if published_versions.include?(version)
        raise ArgumentError,
              "pgque #{version} is already published on RubyGems and immutable; choose a new version"
      end

      true
    rescue JSON::ParserError => e
      raise ArgumentError, "invalid RubyGems versions response: #{e.message}"
    end
  end
end

if $PROGRAM_NAME == __FILE__
  abort "usage: check_rubygems_version.rb VERSION [RESPONSE_PATH]" unless [1, 2].include?(ARGV.length)

  version, response_path = ARGV
  response = response_path ? File.read(response_path) : $stdin.read
  begin
    PgqueRelease::RubyGemsVersionCheck.check!(version, response)
  rescue ArgumentError => e
    abort e.message
  end
  puts "RubyGems version available: pgque #{version}"
end
