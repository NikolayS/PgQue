# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "minitest/autorun"
require "open3"
require "rbconfig"
require_relative "../lib/pgque/version"

class TestValidateRelease < Minitest::Test
  SCRIPT = File.expand_path("../script/validate_release.rb", __dir__)

  def run_validator(*arguments)
    Open3.capture3(RbConfig.ruby, SCRIPT, *arguments)
  end

  def test_accepts_matching_version_and_namespaced_tag
    output, error, status = run_validator(
      Pgque::VERSION, "ruby/v#{Pgque::VERSION}"
    )

    assert status.success?, error
    assert_includes output, "release candidate verified: pgque #{Pgque::VERSION}"
  end

  def test_rejects_unnamespaced_tag
    _output, error, status = run_validator(Pgque::VERSION, "v#{Pgque::VERSION}")

    refute status.success?
    assert_includes error, "tag must be \"ruby/v#{Pgque::VERSION}\""
  end

  def test_rejects_noncanonical_version
    _output, error, status = run_validator("0.3.0-rc.1", "ruby/v0.3.0-rc.1")

    refute status.success?
    assert_includes error, "invalid canonical RubyGems version"
  end

  def test_rejects_version_that_differs_from_library
    _output, error, status = run_validator("9.9.9", "ruby/v9.9.9")

    refute status.success?
    assert_includes error, "!= Pgque::VERSION"
  end

  def test_rejects_missing_artifact
    missing = "pgque-#{Pgque::VERSION}.gem"
    _output, error, status = run_validator(
      Pgque::VERSION, "ruby/v#{Pgque::VERSION}", missing
    )

    refute status.success?
    assert_includes error, "gem artifact not found"
  end
end
