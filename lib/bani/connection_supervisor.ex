defmodule Bani.ConnectionSupervisor do
  use Supervisor

  # client

  def start_link(conn_opts) do
    Supervisor.start_link(__MODULE__, conn_opts)
  end

  def add_publisher(pid, stream_name, publisher_id, conn) do
    publisher_supervisor = child_publisher_supervisor(pid)

    DynamicSupervisor.start_child(
      publisher_supervisor,
      {Bani.Publisher, [conn: conn, stream_name: stream_name, publisher_id: publisher_id]}
    )
  end

  def add_subscriber(pid, stream_name, subscription_id, conn, handler) do
    subscriber_supervisor = child_subscriber_supervisor(pid)

    DynamicSupervisor.start_child(
      subscriber_supervisor,
      {Bani.Subscriber, [conn: conn, stream_name: stream_name, subscription_id: subscription_id, handler: handler]}
    )
  end

  def child_connection_manager(pid) do
    pid
    |> children()
    |> Keyword.get(Bani.ConnectionManager)
  end

  def child_conn(pid) do
    pid
    |> child_connection_manager()
    |> Bani.ConnectionManager.conn()
  end

  defp child_publisher_supervisor(pid) do
    pid
    |> children()
    |> Keyword.get(Bani.PublisherSupervisor)
  end

  defp child_subscriber_supervisor(pid) do
    pid
    |> children()
    |> Keyword.get(Bani.SubscriberSupervisor)
  end

  defp children(pid) do
    pid
    |> Supervisor.which_children()
    |> Enum.map(fn ({type, child_pid, _, _}) -> {type, child_pid} end)
  end

  # server

  @impl true
  def init(conn_opts) do
    connection_manager_opts = Keyword.put(conn_opts, :supervisor, self())

    children = [
      {Bani.ConnectionManager, connection_manager_opts},
      {Bani.PublisherSupervisor, []},
      {Bani.SubscriberSupervisor, []}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
