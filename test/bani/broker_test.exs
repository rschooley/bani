defmodule Bani.BrokerTest do
  use ExUnit.Case, async: true

  alias Bani.Broker

  @host "localhost"
  @port 5552
  @username "guest"
  @password "guest"
  @vhost "/test"

  test "connects and disconnects" do
    assert {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)
    assert :ok = Broker.disconnect(conn)
  end

  test "creates and deletes a stream" do
    stream_name = "creates-and-deletes-a-stream"

    {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)
    assert :ok = Broker.create_stream(conn, stream_name)
    assert :ok = Broker.delete_stream(conn, stream_name)
    :ok = Broker.disconnect(conn)
  end

  test "subscribes and publishes to a stream" do
    stream_name = "subscribes-and-publishes-to-a-stream"
    message = "some message"

    {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)
    :ok = Broker.create_stream(conn, stream_name)
    :ok = Broker.subscribe(conn, stream_name, 1)

    :ok = Broker.create_publisher(conn, stream_name, 10, "some-publisher")
    :ok = Broker.publish(conn, 10, message, 1)

    {:ok, {[result], _other}} =
      receive do
        {:deliver, _response_code, chunk} ->
          Broker.chunk_to_messages(chunk)
      after
        5000 ->
          exit(:timeout)
      end

    assert result == message

    :ok = Broker.delete_publisher(conn, 10)
    :ok = Broker.unsubscribe(conn, 1)
    :ok = Broker.delete_stream(conn, stream_name)
    :ok = Broker.disconnect(conn)
  end

  test "supports multiple stream subscribers" do
    stream_1 = "supports-multiple-stream-subscribers-1"
    stream_2 = "supports-multiple-stream-subscribers-2"

    subscriber_1 = 1
    subscriber_2 = 2
    subscriber_3 = 2

    {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)
    :ok = Broker.create_stream(conn, stream_1)
    :ok = Broker.create_stream(conn, stream_2)

    assert :ok = Broker.subscribe(conn, stream_1, subscriber_1)
    assert :ok = Broker.subscribe(conn, stream_2, subscriber_2)

    assert {:error, :subscription_id_already_exists} =
             Broker.subscribe(conn, stream_2, subscriber_3)

    :ok = Broker.unsubscribe(conn, subscriber_1)
    :ok = Broker.unsubscribe(conn, subscriber_2)

    :ok = Broker.delete_stream(conn, stream_1)
    :ok = Broker.delete_stream(conn, stream_2)
    :ok = Broker.disconnect(conn)
  end

  # TODO: offset tests
end
