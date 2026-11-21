# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module PgqueRelease
  module TestSkipCheck
    module_function

    SUMMARY_PATTERN = /^(\d+) runs, \d+ assertions, (\d+) failures, (\d+) errors, (\d+) skips$/

    def check!(output)
      summaries = output.scan(SUMMARY_PATTERN)
      raise ArgumentError, "Minitest summary not found" if summaries.empty?

      summaries.each do |summary|
        runs, failures, errors, skips = summary.map { |value| Integer(value, 10) }
        raise ArgumentError, "Ruby release test suite ran no tests" if runs.zero?
        raise ArgumentError, "Ruby release test suite reported failures or errors" unless failures.zero? && errors.zero?
        if skips.positive?
          raise ArgumentError, "Ruby release test suite reported #{skips} skipped test(s)"
        end
      end

      true
    end
  end
end

if $PROGRAM_NAME == __FILE__
  abort "usage: assert_no_test_skips.rb TEST_LOG" unless ARGV.length == 1

  begin
    PgqueRelease::TestSkipCheck.check!(File.read(ARGV.fetch(0)))
  rescue ArgumentError => e
    abort e.message
  end
  puts "Ruby release test suite reported zero skips"
end
