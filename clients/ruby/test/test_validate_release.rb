# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "minitest/autorun"
require "open3"
require "rbconfig"
require "yaml"
require_relative "../lib/pgque/version"

class TestValidateRelease < Minitest::Test
  SCRIPT = File.expand_path("../script/validate_release.rb", __dir__)
  WORKFLOW = File.expand_path("../../../.github/workflows/release-ruby.yml", __dir__)

  def run_validator(*arguments)
    Open3.capture3(RbConfig.ruby, SCRIPT, *arguments)
  end

  def workflow_steps
    YAML.safe_load_file(WORKFLOW).fetch("jobs").values.flat_map do |job|
      job.fetch("steps")
    end
  end

  def test_release_workflow_pins_privileged_gem_tooling
    install = workflow_steps.find { |step| step["name"] == "Install release tooling" }
    await_step = workflow_steps.find { |step| step["name"] == "Wait for release to propagate" }

    assert_equal "gem install rubygems-await --version 0.5.4 --no-document", install&.fetch("run")
    assert_equal "gem exec --version 0.5.4 rubygems-await \"./pgque-${VERSION}.gem\"", await_step.fetch("run")
  end

  def test_release_workflow_shell_contract
    scripts = workflow_steps.filter_map { |step| step["run"] }
    scripts.grep(/set -Eeuo pipefail/).each do |script|
      assert_match(/\Aset -Eeuo pipefail\nIFS=\$'\\n\\t'\n/, script)
    end

    combined = scripts.join("\n")
    refute_includes combined, "$(seq "
    if combined.match?(/\bpsql\b/)
      assert_includes combined, "PAGER=cat"
      assert_includes combined, "psql --no-psqlrc --set=ON_ERROR_STOP=1"
      refute_match(/\bpsql\b[^\n]*\s-v(?:\s|$)/, combined)
    end
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
