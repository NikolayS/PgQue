# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module PgqueRelease
  module TestSkipCheck
    module_function

    SUMMARY_PATTERN = /^\d+ runs, \d+ assertions, \d+ failures, \d+ errors, (\d+) skips$/

    def check!(output)
      summaries = output.scan(SUMMARY_PATTERN).flatten
      raise ArgumentError, "Minitest summary not found" if summaries.empty?

      skip_count = Integer(summaries.last, 10)
      if skip_count.positive?
        raise ArgumentError, "Ruby release test suite reported #{skip_count} skipped test(s)"
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
