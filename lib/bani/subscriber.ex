defmodule Bani.Subscriber do
  use GenServer

  # Client

  def start_link(opts) do
    state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      conn: Keyword.fetch!(opts, :conn),
      handler: Keyword.fetch!(opts, :handler),
      message_processor: Keyword.get(opts, :message_processor, Bani.MessageProcessor),
      stream_name: Keyword.fetch!(opts, :stream_name),
      subscription_id: Keyword.fetch!(opts, :subscription_id),
      subscription_name: Keyword.fetch!(opts, :subscription_name),
      tenant: Keyword.fetch!(opts, :tenant)
    }

    GenServer.start_link(__MODULE__, state, name: via_tuple(state.tenant, state.stream_name, state.subscription_name))
  end

  defp via_tuple(tenant, stream_name, subscription_name) do
    name = Bani.KeyRing.subscriber_name(tenant, stream_name, subscription_name)

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    {:ok, state, {:continue, :create_subscriber}}
  end

  @impl true
  def handle_continue(:create_subscriber, state) do
    offset = Bani.SubscriberStorage.offset(state.tenant, state.stream_name, state.subscription_name)

    # subscriptions are per conn per subscription id
    # conns don't reconnect
    # if a ConnectionManager restarts there is a new conn
    # so we create new subscriptions in rabbit on genserver init
    :ok = state.broker.subscribe(
      state.conn,
      state.stream_name,
      state.subscription_id,
      offset
    )

    pid = self()
    ref = make_ref()

    spawn_link(fn -> monitor_for_cleanup(pid, ref, {state.broker, state.conn, state.subscription_id})
    end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:deliver, _response_code, chunk}, state) do
    acc = Bani.SubscriberStorage.acc(state.tenant, state.stream_name, state.subscription_name)

    result = state.message_processor.process(
      &state.broker.chunk_to_messages/1,
      state.handler,
      chunk,
      acc
    )

    case result do
      {:ok, new_acc} ->
        # TODO: multiple messages & offset inc
        Bani.SubscriberStorage.update(
          state.tenant,
          state.stream_name,
          state.subscription_name,
          1,
          new_acc
        )

        {:noreply, state}

      {:error, err} ->
        # TODO: should this unsubscribe from the broker
        Bani.SubscriberStorage.poison(
          state.tenant,
          state.stream_name,
          state.subscription_name,
          err
        )

        {:noreply, state}
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
        # two most probable reasons for an exit
        # 1) the conn dropped and the ConnectionSupervisor subtree is restarting
        #    in that case the conn is dead, but that will remove the sub in rabbit
        # 2) this genserver alone is crashing
        #    in that case the conn is alive, we'll remove the existing sub and recreate on init
        if (Process.alive?(conn)) do
          # if this fails the init will fail and cascade crash the supervisor resetting the conn
          :ok = broker.unsubscribe(conn, subscription_id)
        end
    end
  end
end
