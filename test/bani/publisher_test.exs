defmodule Bani.PublisherTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  setup do
    conn = self()
    connection_id = "some-connection-id"

    stub(Bani.MockConnectionManager, :conn, fn (_) -> conn end)

    {:ok, %{conn: conn, connection_id: connection_id}}
  end

  test "initializes", %{conn: conn, connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-1"
    stream_name = "publisher-initializes"
    publisher_id = 0

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      stream_name: stream_name,
      publisher_id: publisher_id,
      tenant: tenant
    ]

    expect(Bani.MockBroker, :create_publisher, fn (conn_, stream_name_, publisher_id_, publisher_name_) ->
      assert conn_ == conn
      assert stream_name_ == stream_name
      assert publisher_id_ == publisher_id
      assert publisher_name_

      Process.send(test_pid, {:expect_create_publisher_called, ref}, [])

      :ok
    end)

    expect(Bani.MockBroker, :query_publisher_sequence, fn (conn_, publisher_name_, stream_name_) ->
      assert conn_ == conn
      assert publisher_name_
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_query_publisher_sequence_called, ref}, [])

      {:ok, 0}
    end)

    assert {:ok, _} = start_supervised({Bani.Publisher, opts})

    assert_receive {:expect_create_publisher_called, ^ref}
    assert_receive {:expect_query_publisher_sequence_called, ^ref}
  end

  test "cleans up on exit", %{conn: conn, connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    stream_name = "publisher-cleans-up-on-exit"
    publisher_id = 1
    tenant = "tenant-123"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      stream_name: stream_name,
      publisher_id: publisher_id,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :query_publisher_sequence, fn (_, _, _) -> {:ok, 1} end)

    expect(Bani.MockBroker, :delete_publisher, fn (conn_, publisher_id_) ->
      assert conn_ == conn
      assert publisher_id_ == publisher_id

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    {:ok, pid} = start_supervised({Bani.Publisher, opts})
    :ok = GenServer.stop(pid)

    assert_receive {:expect_called, ^ref}
  end

  test "publishes single message",  %{conn: conn, connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-1"
    stream_name = "publisher-publishes"
    publisher_id = 0
    message = "some message"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      stream_name: stream_name,
      publisher_id: publisher_id,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :query_publisher_sequence, fn (_, _, _) -> {:ok, 0} end)

    expect(Bani.MockBroker, :publish, fn (conn_, publisher_id_, messages_) ->
      assert conn_ == conn
      assert publisher_id_ == publisher_id
      assert messages_ == [{0, message}]

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    pid = start_supervised!({Bani.Publisher, opts})

    assert :ok = Bani.Publisher.publish_sync(tenant, stream_name, message)
    assert_receive {:expect_called, ^ref}
    assert get_publishing_id(pid) == 1
  end

  test "publishes list of messages",  %{conn: conn, connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-1"
    stream_name = "publisher-publishes-list-of-messages"
    publisher_id = 0
    message_1 = "some message 1"
    message_2 = "some message 2"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      stream_name: stream_name,
      publisher_id: publisher_id,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :create_publisher, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :query_publisher_sequence, fn (_, _, _) -> {:ok, 0} end)

    expect(Bani.MockBroker, :publish, fn (conn_, publisher_id_, messages_) ->
      assert conn_ == conn
      assert publisher_id_ == publisher_id
      assert messages_ == [{0, message_1}, {1, message_2}]

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    pid = start_supervised!({Bani.Publisher, opts})

    assert :ok = Bani.Publisher.publish_sync(tenant, stream_name, [message_1, message_2])
    assert_receive {:expect_called, ^ref}
    assert get_publishing_id(pid) == 2
  end

  defp get_publishing_id(pid) do
    pid
    |> :sys.get_state()
    |> Map.get(:publishing_id)
  end
end
