defmodule Bani.ConnectionManager do
  use GenServer
  @behaviour Bani.ConnectionManagerBehaviour

  # Client

  def start_link(opts) do
    state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      connection_id: Keyword.fetch!(opts, :connection_id),
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      username: Keyword.fetch!(opts, :username),
      password: Keyword.fetch!(opts, :password),
      vhost: Keyword.fetch!(opts, :vhost)
    }

    GenServer.start_link(__MODULE__, state, name: via_tuple(state.connection_id))
  end

  def conn(connection_id) do
    GenServer.call(via_tuple(connection_id), :conn)
  end

  defp via_tuple(connection_id) do
    name = Bani.KeyRing.connection_manager_name(connection_id)

    {:via, Registry, {Bani.Registry, name}}
  end

  # Callbacks

  @impl true
  def init(init_state) do
    {:ok, init_state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    {:ok, conn} = state.broker.connect(
      state.host,
      state.port,
      state.username,
      state.password,
      state.vhost
    )

    new_state = Map.put(state, :conn, conn)

    pid = self()
    ref = make_ref()
    spawn_link(fn -> monitor_for_cleanup(pid, ref, {new_state.broker, new_state.conn}) end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:conn, _from, state) do
    {:reply, state.conn, state}
  end

  defp monitor_for_cleanup(pid, ref, {broker, conn}) do
    Process.flag(:trap_exit, true)
    send(pid, {ref, :ready})

    receive do
      {:EXIT, ^pid, _reason} ->
        if Process.alive?(conn) do
          :ok = broker.disconnect(conn)
        end
    end
  end
end
