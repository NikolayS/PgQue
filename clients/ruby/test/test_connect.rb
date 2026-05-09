# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestConnect < Minitest::Test
  include PgqueTest::Helpers

  def test_connect_returns_client
    client = Pgque.connect(dsn)
    assert_instance_of Pgque::Client, client
    refute client.conn.finished?
    client.close
    assert client.conn.finished?
  end

  def test_connect_block_form_closes_on_exit
    captured = nil
    Pgque.connect(dsn) do |client|
      captured = client
      refute client.conn.finished?
    end
    assert captured.conn.finished?
  end
end

class TestConnectBadDsn < Minitest::Test
  def test_connect_bad_dsn_raises_pgque_connection_error
    assert_raises(Pgque::ConnectionError) do
      Pgque.connect("postgresql://nobody:wrong@localhost:1/nonexistent_db_xyz")
    end
  end
end
