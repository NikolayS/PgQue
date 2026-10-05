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
      File.write("#{dir}/curl", "#!/bin/sh\nexit 22\n")
      FileUtils.chmod(0o755, "#{dir}/curl")
      env = {"PATH" => "#{dir}:#{ENV.fetch('PATH')}", "VERSION" => Pgque::VERSION}
      ["build", "publish-rubygems"].each do |job|
        name = job == "build" ? "Verify RubyGems version is available" : "Recheck RubyGems version availability"
        _out, _err, status = Open3.capture3(env, "bash", "-c", step(job, name), chdir: dir)
        refute status.success?
      end
    end
  end
end
