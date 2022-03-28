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
    :ok = Broker.create_publisher(conn, stream_name, 10, "some-publisher")
    :ok = Broker.publish(conn, 10, [{0, message}])

    :ok = Broker.subscribe(conn, stream_name, 1)

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

  test "handles disconnecting while active subscriber" do
    stream = "handles-disconnecting-while-active-subscriber"
    subscriber_id = 1

    {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)
    :ok = Broker.create_stream(conn, stream)
    :ok = Broker.subscribe(conn, stream, subscriber_id)

    assert :ok = Broker.disconnect(conn)
  end

  test "handles duplicate publisher names" do
    stream_name = "handles-duplicate-publisher-names"

    {:ok, conn_1} = Broker.connect(@host, @port, @username, @password, @vhost)
    {:ok, conn_2} = Broker.connect(@host, @port, @username, @password, @vhost)

    :ok = Broker.create_stream(conn_1, stream_name)

    assert :ok = Broker.create_publisher(conn_1, stream_name, 10, "the-publisher")

    # same conn, same publisher_id, same publisher_name
    assert {:error, :precondition_failed} = Broker.create_publisher(conn_1, stream_name, 10, "the-publisher")

    # same conn, same publisher_id, different publisher_name
    assert {:error, :precondition_failed} = Broker.create_publisher(conn_1, stream_name, 10, "another-publisher")

    # same conn, different publisher_id, same publisher_name
    assert {:error, :precondition_failed} = Broker.create_publisher(conn_1, stream_name, 11, "the-publisher")

    # different conn, same publisher_id, same publisher_name
    assert :ok = Broker.create_publisher(conn_2, stream_name, 10, "the-publisher")

    :ok = Broker.delete_publisher(conn_1, 10)
    :ok = Broker.delete_publisher(conn_2, 10)
    :ok = Broker.delete_stream(conn_1, stream_name)
    :ok = Broker.disconnect(conn_1)
  end

  test "prevents deleting a publisher from a different conn" do
    stream_name = "delete-a-publisher-from-a-different-conn"

    {:ok, conn_1} = Broker.connect(@host, @port, @username, @password, @vhost)
    {:ok, conn_2} = Broker.connect(@host, @port, @username, @password, @vhost)
    :ok = Broker.create_stream(conn_1, stream_name)

    assert :ok = Broker.create_publisher(conn_1, stream_name, 10, "the-publisher")

    assert {:error, :publisher_does_not_exist} = Broker.delete_publisher(conn_2, 10)
    assert :ok = Broker.delete_publisher(conn_1, 10)

    :ok = Broker.delete_stream(conn_1, stream_name)
    :ok = Broker.disconnect(conn_1)
  end

  test "queries publisher sequence" do
    stream_name = "queries-publisher-sequence"
    publisher_name = "publisher-queries-publisher-sequence"

    {:ok, conn_1} = Broker.connect(@host, @port, @username, @password, @vhost)
    {:ok, conn_2} = Broker.connect(@host, @port, @username, @password, @vhost)

    :ok = Broker.create_stream(conn_1, stream_name)
    :ok = Broker.create_publisher(conn_1, stream_name, 10, publisher_name)

    assert {:ok, 0} = Broker.query_publisher_sequence(conn_1, publisher_name, stream_name)
    :ok = Broker.publish(conn_1, 10, [{1, "some message 1"}])
    :ok = Broker.publish(conn_1, 10, [{2, "some message 2"}])
    assert {:ok, 2} = Broker.query_publisher_sequence(conn_1, publisher_name, stream_name)

    # the publisher_name is cross conns
    assert {:ok, 2} = Broker.query_publisher_sequence(conn_2, publisher_name, stream_name)

    :ok = Broker.delete_publisher(conn_1, 10)
    :ok = Broker.delete_stream(conn_1, stream_name)
    :ok = Broker.disconnect(conn_2)
    :ok = Broker.disconnect(conn_1)
  end

  test "queries publisher sequence after publisher deleted" do
    stream_name = "queries-publisher-sequence-after-publisher-deleted"
    publisher_name = "publisher-queries-publisher-sequence-after-publisher-deleted"

    {:ok, conn} = Broker.connect(@host, @port, @username, @password, @vhost)

    :ok = Broker.create_stream(conn, stream_name)
    :ok = Broker.create_publisher(conn, stream_name, 10, publisher_name)
    {:ok, 0} = Broker.query_publisher_sequence(conn, publisher_name, stream_name)

    :ok = Broker.publish(conn, 10, [{1, "some message 1"}])
    assert {:ok, 1} = Broker.query_publisher_sequence(conn, publisher_name, stream_name)

    :ok = Broker.delete_publisher(conn, 10)
    :ok = Broker.create_publisher(conn, stream_name, 11, publisher_name)
    assert {:ok, 1} = Broker.query_publisher_sequence(conn, publisher_name, stream_name)

    :ok = Broker.delete_publisher(conn, 11)
    :ok = Broker.delete_stream(conn, stream_name)
    :ok = Broker.disconnect(conn)
  end
end
