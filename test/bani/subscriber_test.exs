defmodule Bani.SubscriberTest do
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

    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-initializes"
    subscription_id = 10
    subscription_name = "subscriber-initializes-sink"
    tenant = "tenant-123"
    offset = 0

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    expect(Bani.MockBroker, :subscribe, fn (conn_, stream_name_, subscription_id_, offset_) ->
      assert conn_ == conn
      assert stream_name_ == stream_name
      assert subscription_id_ == subscription_id
      assert offset_ == offset

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    start_subscriber_storage!(opts, offset, %{})
    start_supervised!({Bani.Subscriber, opts})

    assert_receive {:expect_called, ^ref}
  end

  test "cleans up on exit", %{conn: conn, connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-cleans-up-on-exit"
    subscription_id = 10
    subscription_name = "subscriber-cleans-up-on-exit-sink"
    tenant = "tenant-123"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockBroker, :unsubscribe, fn (conn_, subscription_id_) ->
      assert conn_ == conn
      assert subscription_id_ == subscription_id

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    start_subscriber_storage!(opts)
    {:ok, pid} = start_supervised({Bani.Subscriber, opts})
    :ok = GenServer.stop(pid)

    assert_receive {:expect_called, ^ref}
  end

  test "handles message delivery", %{connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-handles-message-delivery"
    subscription_id = 10
    subscription_name = "subscriber-handles-message-delivery-sink"
    tenant = "tenant-123"
    acc = %{a: "a"}
    offset = 1
    chunk = "some chunk"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)

    expect(Bani.MockMessageProcessor, :process, fn (parser_fn_, processing_fn_, chunk_, acc_) ->
      # assert parser_fn_ == &Bani.MockBroker.chunk_to_messages/1
      assert parser_fn_
      assert processing_fn_ == handler
      assert chunk_ == chunk
      assert acc_ == acc

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, acc}
    end)

    start_subscriber_storage!(opts, offset, acc)
    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, chunk})

    assert_receive {:expect_called, ^ref}
  end

  test "updates state on deliver success", %{connection_id: connection_id} do
    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-updates-state-on-deliver-success"
    subscription_id = 10
    subscription_name = "subscriber-updates-state-on-deliver-success-sink"
    tenant = "tenant-123"

    offset = 10
    acc = %{a: "a 1"}
    new_acc = %{a: "a 2", b: "b 1"}

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)
    stub(Bani.MockMessageProcessor, :process, fn (_, _, _, _) -> {:ok, new_acc} end)

    start_subscriber_storage!(opts, offset, acc)
    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, "some chunk"})

    wait_for_passing(_2_seconds = 2_000, fn ->
      new_state = Bani.SubscriberStorage.values(tenant, stream_name, subscription_name)

      assert new_state.acc == new_acc
      assert new_state.offset == offset + 1
      assert new_state.poisoned == false
    end)
  end

  test "updates state on deliver error", %{connection_id: connection_id} do
    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-updates-state-on-deliver-error"
    subscription_id = 10
    subscription_name = "subscriber-updates-state-on-deliver-error-sink"
    tenant = "tenant-123"

    offset = 10
    acc = %{a: "a 1"}
    poisoned_err = "some error"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)
    stub(Bani.MockMessageProcessor, :process, fn (_, _, _, _) -> {:error, poisoned_err} end)

    start_subscriber_storage!(opts, offset, acc)
    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, "some chunk"})

    wait_for_passing(_2_seconds = 2_000, fn ->
      new_state = Bani.SubscriberStorage.values(tenant, stream_name, subscription_name)

      assert new_state.offset == offset
      assert new_state.poisoned == true
      assert new_state.poisoned_err == poisoned_err
    end)
  end

  defp start_subscriber_storage!(subscriber_opts, offset \\ 0, acc \\ nil, poisoned \\ false) do
    opts =
      subscriber_opts
      |> Keyword.take([:stream_name, :subscription_name, :tenant])
      |> Keyword.merge([offset: offset, acc: acc, poisoned: poisoned])

    start_supervised!({Bani.SubscriberStorage, opts})
  end
end
