# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
require "minitest/autorun"
require "open3"
require "yaml"
require "tmpdir"
require "fileutils"
require "digest"
require "rubygems/package"
require_relative "../lib/pgque/version"

class TestReleaseArtifact < Minitest::Test
  ROOT = File.expand_path("..", __dir__)
  WORKFLOW = File.expand_path("../../../.github/workflows/release-ruby.yml", __dir__)

  def step(job, name)
    YAML.safe_load_file(WORKFLOW).fetch("jobs").fetch(job).fetch("steps").find { |s| s["name"] == name }.fetch("run")
  end

  def with_artifact
    Dir.mktmpdir("pgque-release-test") do |dir|
      FileUtils.mkdir_p("#{dir}/script")
      FileUtils.mkdir_p("#{dir}/lib/pgque")
      FileUtils.cp("#{ROOT}/script/validate_release.rb", "#{dir}/script")
      FileUtils.cp("#{ROOT}/lib/pgque/version.rb", "#{dir}/lib/pgque")
      spec = Gem::Specification.new do |s|
        s.name = "pgque"
        s.version = Pgque::VERSION
        s.summary = "release gate fixture"
        s.authors = ["PgQue tests"]
        s.license = "Apache-2.0"
        s.files = []
      end
      File.write("#{dir}/fixture.gemspec", spec.to_ruby)
      out, err, status = Open3.capture3("gem", "build", "fixture.gemspec", chdir: dir)
      assert status.success?, out + err
      FileUtils.mkdir_p("#{dir}/bin")
      File.write("#{dir}/bin/git", <<~SH)
        #!/bin/sh
        echo "$*" >> "$GIT_LOG"
        if [ "$1" = ls-remote ]; then exit "${REMOTE_STATUS:-2}"; fi
      SH
      FileUtils.chmod(0o755, "#{dir}/bin/git")
      gem = "#{dir}/pgque-#{Pgque::VERSION}.gem"
      env = {"VERSION" => Pgque::VERSION, "TAG_NAME" => "ruby/v#{Pgque::VERSION}",
             "EXPECTED_GEM_SHA256" => Digest::SHA256.file(gem).hexdigest,
             "PATH" => "#{dir}/bin:#{ENV.fetch('PATH')}", "GIT_LOG" => "#{dir}/git.log"}
      yield dir, gem, env
    end
  end

  def test_publish_revalidation_accepts_exact_tested_artifact
    with_artifact do |dir, _gem, env|
      out, err, status = Open3.capture3(env, "bash", "-c", step("publish-rubygems", "Revalidate artifact and tag"), chdir: dir)
      assert status.success?, out + err
      assert_includes out, ".gem: OK"
    end
  end

  def test_publish_revalidation_rejects_tampered_artifact_before_tag_check
    with_artifact do |dir, gem, env|
      File.open(gem, "ab") { |f| f.write("changed after test") }
      _out, _err, status = Open3.capture3(env, "bash", "-c", step("publish-rubygems", "Revalidate artifact and tag"), chdir: dir)
      refute status.success?
      refute_includes File.read(env.fetch("GIT_LOG")), "ls-remote"
    end
  end

  def test_publish_revalidation_rejects_missing_digest
    with_artifact do |dir, _gem, env|
      env["EXPECTED_GEM_SHA256"] = ""
      _out, _err, status = Open3.capture3(env, "bash", "-c", step("publish-rubygems", "Revalidate artifact and tag"), chdir: dir)
      refute status.success?
    end
  end

  def test_remote_tag_occupied_or_lookup_failed_is_not_available
    with_artifact do |dir, _gem, env|
      ["0", "128"].each do |code|
        env["REMOTE_STATUS"] = code
        _out, _err, status = Open3.capture3(env, "bash", "-c", step("publish-rubygems", "Revalidate artifact and tag"), chdir: dir)
        refute status.success?, "remote exit #{code} must fail closed"
      end
    end
  end

  def test_registry_request_errors_fail_closed
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p("#{dir}/script")
      FileUtils.cp("#{ROOT}/script/check_rubygems_version.rb", "#{dir}/script")
      File.write("#{dir}/curl", <<~SH)
        #!/bin/sh
        echo called >> "$CURL_LOG"
        if [ "$CURL_STATUS" != 0 ]; then exit "$CURL_STATUS"; fi
        while [ "$#" -gt 0 ]; do
          if [ "$1" = -o ]; then
            printf '%s' '[{"number":"0.2.2"}]' > "$2"
            exit 0
          fi
          shift
        done
        exit 2
      SH
      FileUtils.chmod(0o755, "#{dir}/curl")
      env = {"PATH" => "#{dir}:#{ENV.fetch('PATH')}", "VERSION" => Pgque::VERSION,
             "CURL_LOG" => "#{dir}/curl.log", "CURL_STATUS" => "0"}
      ["build", "publish-rubygems"].each do |job|
        name = job == "build" ? "Verify RubyGems version is available" : "Recheck RubyGems version availability"
        env["CURL_STATUS"] = "0"
        out, err, status = Open3.capture3(env, "bash", "-c", step(job, name), chdir: dir)
        assert status.success?, out + err
        before = File.readlines(env.fetch("CURL_LOG")).length
        env["CURL_STATUS"] = "22"
        _out, _err, status = Open3.capture3(env, "bash", "-c", step(job, name), chdir: dir)
        assert_equal 22, status.exitstatus
        assert_equal before + 1, File.readlines(env.fetch("CURL_LOG")).length
      end
    end
  end

  def test_artifact_metadata_must_be_valid_before_download
    steps = YAML.safe_load_file(WORKFLOW).fetch("jobs").fetch("publish-rubygems").fetch("steps")
    guard = steps.index { |s| s["name"] == "Verify artifact metadata" }
    download = steps.index { |s| s["uses"] == "actions/download-artifact@v4" }
    refute_nil guard, "artifact metadata needs a pre-download guard"
    assert_operator guard, :<, download
    script = step("publish-rubygems", "Verify artifact metadata")
    digest = "a" * 64
    out, err, status = Open3.capture3({"EXPECTED_ARTIFACT_ID" => "12345", "EXPECTED_GEM_SHA256" => digest}, "bash", "-c", script)
    assert status.success?, out + err
    [nil, "", " ", "0", "-1", "1,2", "abc"].each do |id|
      _out, _err, status = Open3.capture3({"EXPECTED_ARTIFACT_ID" => id, "EXPECTED_GEM_SHA256" => digest}, "bash", "-c", script)
      refute status.success?, "invalid artifact ID #{id.inspect} must fail"
    end
    [nil, "", " ", "xyz"].each do |value|
      _out, _err, status = Open3.capture3({"EXPECTED_ARTIFACT_ID" => "12345", "EXPECTED_GEM_SHA256" => value}, "bash", "-c", script)
      refute status.success?, "invalid digest must fail"
    end
  end
end
