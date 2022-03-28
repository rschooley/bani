defmodule Bani.Broker do
  @behaviour Bani.BrokerBehaviour

  @impl Bani.BrokerBehaviour
  def connect(host, port, username, password, vhost) do
    :lake.connect(host, port, username, password, vhost)
  end

  @impl Bani.BrokerBehaviour
  def disconnect(conn) when is_pid(conn) do
    :lake.stop(conn)
  end

  @impl Bani.BrokerBehaviour
  def create_stream(conn, stream_name) when is_pid(conn) and is_binary(stream_name) do
    :lake.create(conn, stream_name, [])
  end

  @impl Bani.BrokerBehaviour
  def delete_stream(conn, stream_name) when is_pid(conn) and is_binary(stream_name) do
    :lake.delete(conn, stream_name)
  end

  # TODO: offset atom

  @impl Bani.BrokerBehaviour
  def subscribe(conn, stream_name, subscription_id, offset \\ 0)
      when is_pid(conn) and is_binary(stream_name) and is_integer(subscription_id) and is_integer(offset) do
    :lake.subscribe(conn, stream_name, subscription_id, {:offset, offset}, 1000, [])
  end

  @impl Bani.BrokerBehaviour
  def unsubscribe(conn, subscription_id) when is_pid(conn) and is_integer(subscription_id) do
    :lake.unsubscribe(conn, subscription_id)
  end

  @impl Bani.BrokerBehaviour
  def create_publisher(conn, stream_name, publisher_id, publisher_name)
      when is_pid(conn) and is_binary(stream_name) and is_integer(publisher_id) and
             is_binary(publisher_name) do
    :lake.declare_publisher(conn, stream_name, publisher_id, publisher_name)
  end

  @impl Bani.BrokerBehaviour
  def delete_publisher(conn, publisher_id) when is_pid(conn) and is_integer(publisher_id) do
    :lake.delete_publisher(conn, publisher_id)
  end

  @impl Bani.BrokerBehaviour
  def publish(conn, publisher_id, messages)
      when is_pid(conn) and is_integer(publisher_id) and is_list(messages) do
    # TODO: verify published_id
    [{_published_id, :ok}] = :lake.publish_sync(conn, publisher_id, messages)

    :ok
  end

  @impl Bani.BrokerBehaviour
  def query_publisher_sequence(conn, publisher_name, stream_name)
      when is_pid(conn) and is_binary(publisher_name) and is_binary(stream_name) do
    :lake.query_publisher_sequence(conn, publisher_name, stream_name)
  end

  @impl Bani.BrokerBehaviour
  def chunk_to_messages(chunk) do
    :lake.chunk_to_messages(chunk)
  end
end
