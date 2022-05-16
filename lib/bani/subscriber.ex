defmodule Bani.Subscriber do
  use GenServer, restart: :transient

  @moduledoc """
  Defines a subscriber GenServer.

  Critical parts of this GenServer's state is persisted to a local store
   to survive crashes, unexpected restarts, container orchestration upgrades, etc
  """

  @doc """
  Starts a subscriber GenServer.

  Required options:
    * `:connection_id`- The id of the connection.  Used to lookup through connection_manager module.
    * `:handler`- The function passed by the calling lib
    * `:stream_name`- The name of the stream in rabbit
    * `:subscription_id`- The id of the subscriber in rabbit
    * `:subscription_name`- The name of the subscription (sink_to_pg, main_pipeline, etc)
    * `:tenant`- The id/name of the tenant

  Default options:
    * `:strategy`- The subscriber strategy
      currently supports :at_least_once | :exactly_once, defaults to :exactly_once

  Optional dependencies for testing:
    * `:broker`- The rabbitmq lib
    * `:connection_manager`- The connection manager module
    * `:message_processor`- The message processor module
    * `:store`- The local store module for persisting state
    * `:subscriber_strategy`- The subscriber strategy module
  """
  def start_link(opts) do
    state = %{
      # required options
      connection_id: Keyword.fetch!(opts, :connection_id),
      handler: Keyword.fetch!(opts, :handler),
      stream_name: Keyword.fetch!(opts, :stream_name),
      subscription_id: Keyword.fetch!(opts, :subscription_id),
      subscription_name: Keyword.fetch!(opts, :subscription_name),
      tenant: Keyword.fetch!(opts, :tenant),

      # default options
      strategy: Keyword.get(opts, :strategy, :exactly_once),

      # optional dependencies for testing
      broker: Keyword.get(opts, :broker, Bani.Broker),
      connection_manager: Keyword.get(opts, :connection_manager, Bani.ConnectionManager),
      message_processor: Keyword.get(opts, :message_processor, Bani.MessageProcessor),
      store: Keyword.get(opts, :store, Bani.Store.SubscriberStore),
      subscriber_strategy: Keyword.get(opts, :subscriber_strategy, Bani.SubscriberStrategy)
    }

    GenServer.start_link(__MODULE__, state, name: via_tuple(state.tenant, state.stream_name, state.subscription_name))
  end

  def lookup(tenant, stream_name, subscription_name) do
    GenServer.call(via_tuple(tenant, stream_name, subscription_name), :lookup)
  end

  # TODO
  # def resubscribe() do
  # end

  defp via_tuple(tenant, stream_name, subscription_name) do
    name = subscriber_key(tenant, stream_name, subscription_name)

    {:via, Registry, {Bani.Registry, name}}
  end

  defp subscriber_key(state) do
    subscriber_key(state.tenant, state.stream_name, state.subscription_name)
  end

  defp subscriber_key(tenant, stream_name, subscription_name) do
    Bani.KeyRing.subscriber_name(tenant, stream_name, subscription_name)
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    # if we passed conn in to start_link
    #  and the conn dies and this server restarts
    #  on retart it would use the original conn
    conn = state.connection_manager.conn(state.connection_id)
    new_state = Map.put(state, :conn, conn)

    {:ok, new_state, {:continue, :create_subscriber}}
  end

  @impl true
  def handle_continue(:create_subscriber, state) do
    state.store.get_subscriber(state.tenant, subscriber_key(state))
    |> subscribe(state)

    # TODO: we could store a copy of the offset in the genserver state
    #  and check the offset against the store to better support the exactly_once strategy
    {:noreply, state}
  end

  defp subscribe({:ok, %Bani.Store.SubscriberState{locked: true}}, %{strategy: :exactly_once}) do
    # don't subscribe, needs manual reconciliation
  end

  defp subscribe({:ok, %Bani.Store.SubscriberState{offset: offset}}, state) do
    # in the case of at_least_once and locked
    #  pick up subscribe where subscriber_strategy marked in store

    # subscriptions are per conn per subscription id
    # conns don't reconnect
    # if a ConnectionManager restarts there is a new conn
    # and we unsubscribe on exit if the conn is still alive
    # so we always create a new subscription to rabbit on genserver init
    :ok = state.broker.subscribe(
      state.conn,
      state.stream_name,
      state.subscription_id,
      offset
    )

    # boilerplate process cleanup
    pid = self()
    ref = make_ref()

    spawn_link(fn -> monitor_for_cleanup(pid, ref, {state.broker, state.conn, state.subscription_id})
    end)

    receive do
      {^ref, :ready} -> :ok
    end
  end

  @impl true
  def handle_info({:deliver, _response_code, chunk}, state) do
    # TODO: refactor / cleanup
    process_fn = fn (acc) ->
      state.message_processor.process(
        &state.broker.chunk_to_messages/1,
        state.handler,
        chunk,
        acc
      )
    end

    {:ok, _} = state.subscriber_strategy.perform(
      state.strategy,
      state.tenant,
      subscriber_key(state),
      state.store,
      process_fn
    )

    {:noreply, state}
  end

  @impl true
  def handle_info({:metadata_update, _, _stream_name}, state) do
    # TODO: handle these messages
    {:noreply, state}
  end

  @impl true
  def handle_call(:lookup, _from, state) do
    {:reply, {self(), state.connection_id, state.subscription_id, state.subscription_name}, state}
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
