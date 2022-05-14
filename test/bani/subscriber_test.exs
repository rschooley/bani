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
    offset = 0
    stream_name = "subscriber-initializes"
    subscription_id = 10
    subscription_name = "subscriber-initializes-sink"
    tenant = "tenant-123"

    subscriber_key = Bani.KeyRing.subscriber_name(tenant, stream_name, subscription_name)

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      store: Bani.MockSubscriberStore,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    expect(Bani.MockSubscriberStore, :get_subscriber, fn (tenant_, subscriber_key_) ->
      assert tenant_ == tenant
      assert subscriber_key_ == subscriber_key

      Process.send(test_pid, {:expect_get_subscriber_called, ref}, [])

      %Bani.Store.SubscriberState{offset: offset}
    end)

    expect(Bani.MockBroker, :subscribe, fn (conn_, stream_name_, subscription_id_, offset_) ->
      assert conn_ == conn
      assert stream_name_ == stream_name
      assert subscription_id_ == subscription_id
      assert offset_ == offset

      Process.send(test_pid, {:expect_subscribe_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Subscriber, opts})

    assert_receive {:expect_get_subscriber_called, ^ref}
    assert_receive {:expect_subscribe_called, ^ref}
  end

  test "initializes with locked stream", %{connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    handler = fn (_prev, curr) -> {:ok, curr} end
    stream_name = "subscriber-initializes-with-locked-stream"
    subscription_id = 10
    subscription_name = "subscriber-initializes-with-locked-stream-sink"
    tenant = "tenant-123"

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      store: Bani.MockSubscriberStore,
      strategy: :exactly_once,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockSubscriberStore, :get_subscriber, fn (_, _) ->
      %Bani.Store.SubscriberState{locked: true}
    end)

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) ->
      Process.send(test_pid, {:expect_subscribe_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Subscriber, opts})

    refute_receive {:expect_subscribe_called, ^ref}
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
      store: Bani.MockSubscriberStore,
      stream_name: stream_name,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockSubscriberStore, :get_subscriber, fn (_, _) -> %Bani.Store.SubscriberState{} end)
    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockBroker, :unsubscribe, fn (conn_, subscription_id_) ->
      assert conn_ == conn
      assert subscription_id_ == subscription_id

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    {:ok, pid} = start_supervised({Bani.Subscriber, opts})
    :ok = GenServer.stop(pid)

    assert_receive {:expect_called, ^ref}
  end

  test "handles message delivery", %{connection_id: connection_id} do
    test_pid = self()
    ref = make_ref()

    handler = fn (_prev, curr) -> {:ok, curr} end
    store = Bani.MockSubscriberStore
    stream_name = "subscriber-handles-at-least-once-message-delivery"
    subscription_id = 10
    subscription_name = "subscriber-handles-at-least-once-message-delivery"
    tenant = "tenant-123"

    subscriber_key = Bani.KeyRing.subscriber_name(tenant, stream_name, subscription_name)

    opts = [
      broker: Bani.MockBroker,
      connection_manager: Bani.MockConnectionManager,
      connection_id: connection_id,
      handler: handler,
      store: store,
      strategy: :some_strategy,
      stream_name: stream_name,
      subscriber_strategy: Bani.MockSubscriberStrategy,
      subscription_id: subscription_id,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    stub(Bani.MockSubscriberStore, :get_subscriber, fn (_, _) -> %Bani.Store.SubscriberState{} end)
    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockSubscriberStrategy, :perform, fn (:some_strategy, tenant_, subscriber_key_, store_, process_fn_) ->
      assert tenant_ == tenant
      assert subscriber_key_ == subscriber_key
      assert store_ == store
      assert process_fn_

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, %{}}
    end)

    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, "some chunk"})

    assert_receive {:expect_called, ^ref}
  end
end
