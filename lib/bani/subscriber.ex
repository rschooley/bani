defmodule Bani.Subscriber do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      conn: Keyword.fetch!(opts, :conn),
      handler: Keyword.fetch!(opts, :handler),
      message_processor: Keyword.get(opts, :message_processor, Bani.MessageProcessor),
      offset: Keyword.get(opts, :offset, 0),
      stream_name: Keyword.fetch!(opts, :stream_name),
      subscription_id: Keyword.fetch!(opts, :subscription_id)
    }

    GenServer.start_link(__MODULE__, init_state)
  end

  # Server (callbacks)

  @impl true
  def init(init_state) do
    state = Map.merge(init_state, %{poisoned: false, acc: nil})

    {:ok, state, {:continue, :subscribe}}
  end

  @impl true
  def handle_continue(:subscribe, state) do
    :ok = state.broker.subscribe(
      state.conn,
      state.stream_name,
      state.subscription_id,
      state.offset
    )

    pid = self()
    ref = make_ref()
    spawn_link(fn -> monitor_for_cleanup(pid, ref, {state.broker, state.conn, state.subscription_id}) end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:deliver, _response_code, chunk}, state) do
    processed = state.message_processor.process(
      &state.broker.chunk_to_messages/1,
      state.handler,
      chunk,
      state.acc
    )

    case processed do
      {:ok, result} ->
        new_state =
          state
          |> Map.put(:offset, state.offset + 1)
          |> Map.put(:acc, result)

        {:noreply, new_state}

      {:error, _} ->
        new_state = Map.put(state, :poisoned, true)

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:metadata_update, _, _stream_name}, state) do
    # TODO: handle these messages
    {:noreply, state}
  end

  defp monitor_for_cleanup(pid, ref, {broker, conn, subscription_id}) do
    Process.flag(:trap_exit, true)
    send(pid, {ref, :ready})

    receive do
      {:EXIT, ^pid, _reason} ->
        :ok = broker.unsubscribe(conn, subscription_id)
    end
  end
end
