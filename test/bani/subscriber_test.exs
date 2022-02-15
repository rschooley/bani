defmodule Bani.SubscriberTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  test "initializes" do
    test_pid = self()
    ref = make_ref()

    stream_name = "subscriber-initializes"
    conn = self()
    subscription_id = 10
    handler = fn (_prev, curr) -> {:ok, curr} end
    offset = 0

    expect(Bani.MockBroker, :subscribe, fn (conn_, stream_name_, subscription_id_, offset_) ->
      assert conn_ == conn
      assert stream_name_ == stream_name
      assert subscription_id_ == subscription_id
      assert offset_ == offset

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      handler: handler,
      stream_name: stream_name,
      subscription_id: subscription_id
    ]

    start_supervised!({Bani.Subscriber, opts})

    assert_receive {:expect_called, ^ref}
  end

  test "cleans up on exit" do
    raise "pending"

    test_pid = self()
    ref = make_ref()

    stream_name = "subscriber-cleans-up-on-exit"
    conn = self()
    subscription_id = 10
    handler = fn (_prev, curr) -> {:ok, curr} end


    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)

    expect(Bani.MockBroker, :unsubscribe, fn (conn_, subscription_id_) ->
      assert conn_ == conn
      assert subscription_id_ == subscription_id

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      handler: handler,
      stream_name: stream_name,
      subscription_id: 1
    ]

    start_supervised!({Bani.Subscriber, opts})

    assert_receive {:expect_called, ^ref}
  end

  test "handles message delivery" do
    test_pid = self()
    ref = make_ref()

    stream_name = "subscriber-handles-message-delivery"
    conn = self()
    handler = fn (_prev, curr) -> {:ok, curr} end
    chunk = "some chunk"

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)

    expect(Bani.MockMessageProcessor, :process, fn (parser_fn_, processing_fn_, chunk_, acc_) ->
      # assert parser_fn_ == &Bani.MockBroker.chunk_to_messages/1
      assert parser_fn_
      assert processing_fn_ == handler
      assert chunk_ == chunk
      refute acc_

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, %{a: "a"}}
    end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      stream_name: stream_name,
      subscription_id: 1
    ]

    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, chunk})

    assert_receive {:expect_called, ^ref}
  end

  test "updates state on deliver success" do
    stream_name = "subscriber-updates-state-on-deliver-success"
    conn = self()
    handler = fn (_prev, curr) -> {:ok, curr} end
    offset = 0

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)
    stub(Bani.MockMessageProcessor, :process, fn (_, _, _, _) -> {:ok, %{a: "a"}} end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      offset: offset,
      stream_name: stream_name,
      subscription_id: 1
    ]

    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, "some chunk"})

    assert get_state_field(pid, :acc) == %{a: "a"}
    assert get_state_field(pid, :offset) == 1
    assert get_state_field(pid, :poisoned) == false
  end

  test "updates state on deliver error" do
    stream_name = "subscriber-updates-state-on-deliver-error"
    conn = self()
    handler = fn (_prev, curr) -> {:ok, curr} end
    offset = 0

    stub(Bani.MockBroker, :subscribe, fn (_, _, _, _) -> :ok end)
    stub(Bani.MockBroker, :chunk_to_messages, fn (_) -> :ok end)
    stub(Bani.MockMessageProcessor, :process, fn (_, _, _, _) -> {:error, "some error"} end)

    opts = [
      broker: Bani.MockBroker,
      conn: conn,
      handler: handler,
      message_processor: Bani.MockMessageProcessor,
      offset: offset,
      stream_name: stream_name,
      subscription_id: 1
    ]

    pid = start_supervised!({Bani.Subscriber, opts})
    send(pid, {:deliver, _response_code = 1, "some chunk"})

    assert get_state_field(pid, :acc) == nil
    assert get_state_field(pid, :offset) == 0
    assert get_state_field(pid, :poisoned) == true
  end

  defp get_state_field(pid, field) do
    pid
    |> :sys.get_state()
    |> Map.get(field)
  end
end
