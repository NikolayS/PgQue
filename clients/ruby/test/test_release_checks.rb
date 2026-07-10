# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "minitest/autorun"
require "open3"
require "rbconfig"
require_relative "../script/assert_no_test_skips"
require_relative "../script/check_rubygems_version"

class TestReleaseChecks < Minitest::Test
  FIXTURE_DIR = File.expand_path("fixtures/rubygems_versions", __dir__)
  RUBYGEMS_CHECKER = File.expand_path("../script/check_rubygems_version.rb", __dir__)

  def fixture(name)
    File.read(File.join(FIXTURE_DIR, name))
  end

  def test_accepts_an_available_exact_version
    assert PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.2", fixture("available.json"))
  end

  def test_exact_matching_does_not_confuse_rc_1_and_rc_10
    assert PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.1", <<~JSON)
      [{"number":"0.3.0.rc.10"}]
    JSON
  end

  def test_rejects_an_already_published_version
    error = assert_raises(ArgumentError) do
      PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.1", fixture("published.json"))
    end
    assert_match(/already published.*immutable.*new version/, error.message)
  end

  def test_rejects_invalid_json
    error = assert_raises(ArgumentError) do
      PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.2", fixture("invalid.json"))
    end
    assert_match(/invalid RubyGems versions response/, error.message)
  end

  def test_rejects_an_invalid_top_level_schema
    error = assert_raises(ArgumentError) do
      PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.2", fixture("invalid_schema.json"))
    end
    assert_match(/expected a JSON array/, error.message)
  end

  def test_rejects_an_entry_without_a_version_number
    error = assert_raises(ArgumentError) do
      PgqueRelease::RubyGemsVersionCheck.check!("0.3.0.rc.2", fixture("missing_number.json"))
    end
    assert_match(/entry 0 has no string number/, error.message)
  end

  def test_cli_rejects_a_published_version
    _stdout, stderr, status = Open3.capture3(
      RbConfig.ruby,
      RUBYGEMS_CHECKER,
      "0.3.0.rc.1",
      File.join(FIXTURE_DIR, "published.json"),
    )

    refute status.success?
    assert_match(/already published.*immutable/, stderr)
  end

  def test_accepts_a_zero_skip_minitest_summary
    output = "10 runs, 20 assertions, 0 failures, 0 errors, 0 skips\n"
    assert PgqueRelease::TestSkipCheck.check!(output)
  end

  def test_rejects_a_nonzero_skip_minitest_summary
    error = assert_raises(ArgumentError) do
      PgqueRelease::TestSkipCheck.check!(
        "10 runs, 20 assertions, 0 failures, 0 errors, 2 skips\n",
      )
    end
    assert_match(/reported 2 skipped test/, error.message)
  end

  def test_rejects_output_without_a_minitest_summary
    error = assert_raises(ArgumentError) do
      PgqueRelease::TestSkipCheck.check!("no tests ran\n")
    end
    assert_match(/summary not found/, error.message)
  end
end
