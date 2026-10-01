require_relative "test_helper"

class PageFakeResult
  include Enumerable
  def initialize(rows); @rows = rows; end
  def ntuples; @rows.length; end
  def [](index); @rows[index]; end
  def filter_map(&block); @rows.filter_map(&block); end
  def each(&block); @rows.each(&block); end
  def getvalue(row, col); @rows[row].values[col]; end
end

class PageFakeConnection
  attr_reader :calls
  def initialize(results); @results = results; @calls = []; end
  def exec_params(sql, params); @calls << [sql, params]; @results.shift; end
end

class TestPageUnit < Minitest::Test
  def row
    {"status"=>"page", "page_batch_id"=>"9223372036854775807",
     "page_token"=>"token", "page_number"=>"1", "is_last"=>"t",
     "lease_until"=>nil, "fence_epoch"=>nil,
     "msg_id"=>"9223372036854775806", "batch_id"=>"9223372036854775807",
     "type"=>"x", "payload"=>"{}", "retry_count"=>nil,
     "created_at"=>nil, "extra1"=>nil, "extra2"=>nil, "extra3"=>nil,
     "extra4"=>nil}
  end

  def test_typed_expansion_preserves_bigints
    conn = PageFakeConnection.new([PageFakeResult.new([row])])
    page = Pgque::Client.new(conn).receive_page("q", "c", "w")
    assert_includes conn.calls[0][0], "left join lateral unnest(p.messages)"
    assert_equal 9_223_372_036_854_775_807, page.batch_id
    assert_equal 9_223_372_036_854_775_806, page.messages[0].msg_id
  end

  def test_handler_error_does_not_ack
    conn = PageFakeConnection.new([PageFakeResult.new([row])])
    assert_raises(RuntimeError) do
      Pgque::Client.new(conn).process_page("q", "c", "w") { raise "boom" }
    end
    assert_equal 1, conn.calls.length
  end
end
