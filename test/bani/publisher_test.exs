defmodule Bani.PublisherTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  test "initializes" do
    test_pid = self()
    ref = make_ref()

    stream_name = "publisher-initializes"
    conn = self()
    publisher_id = 0

    expect(Bani.MockBroker, :create_publisher, fn (conn_, stream_name_, publisher_id_, publisher_name_) ->
      assert conn_ == conn
      assert stream_name_ == stream_name
      assert publisher_id_ == publisher_id
      assert publisher_name_

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      stream_name: stream_name,
      publisher_id: publisher_id
    ]

    start_supervised!({Bani.Publisher, opts})

    assert_receive {:expect_called, ^ref}
  end

  test "cleans up on exit" do
    raise "pending"

    test_pid = self()
    ref = make_ref()

    stream_name = "publisher-cleans-up-on-exit"
    conn = self()
    publisher_id = 1

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockBroker, :delete_publisher, fn (conn_, publisher_id_) ->
      assert conn_ == conn
      assert publisher_id_ == publisher_id

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      stream_name: stream_name,
      publisher_id: publisher_id
    ]

    start_supervised!({Bani.Publisher, opts})

    assert_receive {:expect_called, ^ref}
  end

  test "publishes" do
    stream_name = "publisher-publishes"
    conn = self()
    publisher_id = 0
    message = "some message"

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockBroker, :publish, fn (conn_, publisher_id_, message_, publishing_id_) ->
      assert conn_ == conn
      assert publisher_id_ == publisher_id
      assert message_ == message
      assert publishing_id_ == 0

      :ok
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      stream_name: stream_name,
      publisher_id: publisher_id
    ]

    start_supervised!({Bani.Publisher, opts})

    assert :ok = Bani.Publisher.publish_sync(stream_name, message)
  end

  test "increments publishing id" do
    stream_name = "publisher-increments-publishing-id"
    conn = self()
    publisher_id = 0

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :publish, fn (_, _, _, _) -> :ok end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      stream_name: stream_name,
      publisher_id: publisher_id
    ]

    pid = start_supervised!({Bani.Publisher, opts})

    :ok = Bani.Publisher.publish_sync(stream_name, "message 1")
    :ok = Bani.Publisher.publish_sync(stream_name, "message 2")

    assert get_publishing_id(pid) == 2
  end

  defp get_publishing_id(pid) do
    pid
    |> :sys.get_state()
    |> Map.get(:publishing_id)
  end
end
