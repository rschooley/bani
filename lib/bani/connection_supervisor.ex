defmodule Bani.ConnectionSupervisor do
  use Supervisor

  # client

  def start_link(opts) do
    connection_id = Keyword.fetch!(opts, :connection_id)

    Supervisor.start_link(__MODULE__, opts, name: via_tuple(connection_id))
  end

  def exists?(connection_id) do
    connection_name = Bani.KeyRing.connection_name(connection_id)
    select = Registry.select(Bani.Registry, [{{:"$1", :_, :_}, [{:==, :"$1", connection_name}], [:"$1"]}])

    case select do
      [] -> false
      _ -> true
    end
  end

  def add_publisher(connection_id, tenant, stream_name, publisher_id) do
    publisher_supervisor = child_publisher_supervisor(connection_id)

    DynamicSupervisor.start_child(
      publisher_supervisor,
      {Bani.Publisher, [
        connection_id: connection_id,
        publisher_id: publisher_id,
        stream_name: stream_name,
        tenant: tenant
      ]}
    )
  end

  def remove_publisher(connection_id, pid) do
    publisher_supervisor = child_publisher_supervisor(connection_id)

    DynamicSupervisor.terminate_child(publisher_supervisor, pid)
  end

  def add_subscriber(connection_id, tenant, stream_name, subscription_id, subscription_name, handler) do
    subscriber_supervisor = child_subscriber_supervisor(connection_id)

    DynamicSupervisor.start_child(
      subscriber_supervisor,
      {Bani.Subscriber, [
        connection_id: connection_id,
        handler: handler,
        stream_name: stream_name,
        subscription_id: subscription_id,
        subscription_name: subscription_name,
        tenant: tenant
      ]}
    )
  end

  def remove_subscriber(connection_id, pid) do
    subscriber_supervisor = child_subscriber_supervisor(connection_id)

    DynamicSupervisor.terminate_child(subscriber_supervisor, pid)
  end

  # these could be moved to Registry lookups instead of direct process navigation

  def child_connection_manager(connection_id) do
    children(connection_id)
    |> Keyword.get(Bani.ConnectionManager)
  end

  def child_conn(connection_id) do
    child_connection_manager(connection_id)
    |> Bani.ConnectionManager.conn()
  end

  defp child_publisher_supervisor(connection_id) do
    children(connection_id)
    |> Keyword.get(Bani.PublisherSupervisor)
  end

  defp child_subscriber_supervisor(connection_id) do
    children(connection_id)
    |> Keyword.get(Bani.SubscriberSupervisor)
  end

  defp children(connection_id) do
    via_tuple(connection_id)
    |> Supervisor.which_children()
    |> Enum.map(fn ({type, child_pid, _, _}) -> {type, child_pid} end)
  end

  defp via_tuple(connection_id) do
    name = Bani.KeyRing.connection_name(connection_id)

    {:via, Registry, {Bani.Registry, name}}
  end

  # server

  @impl true
  def init(opts) do
    children = [
      {Bani.ConnectionManager, opts},
      {Bani.PublisherSupervisor, []},
      {Bani.SubscriberSupervisor, []}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
