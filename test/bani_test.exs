defmodule BaniTest do
  use ExUnit.Case, async: false

  @conn_opts [
    {:host, "localhost"},
    {:port, 5552},
    {:username, "guest"},
    {:password, "guest"},
    {:vhost, "/test"}
  ]

  defmodule TestBani do
    use Bani
  end

  @tag capture_log: true
  test "creates stream and subscribes" do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-123"
    stream_name = "creates-stream-and-subscribes"
    subscription_name = "test-sink"
    message = "sample message"

    handler = fn (_prev, curr) ->
      assert curr == message

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, curr}
    end

    :ok = TestBani.add_tenant(tenant, @conn_opts)
    :ok = TestBani.create_stream(tenant, stream_name)
    :ok = TestBani.create_publisher(tenant, stream_name)
    :ok = TestBani.create_subscriber(tenant, stream_name, subscription_name, handler, %{}, 0, :exactly_once)

    :ok = TestBani.publish(tenant, stream_name, message)

    assert_receive {:expect_called, ^ref}

    :ok = TestBani.delete_stream(tenant, stream_name)
    :ok = TestBani.remove_tenant(tenant)
  end

  @tag capture_log: true
  test "creates multiple streams and subscribtions" do
    test_pid = self()
    ref = make_ref()

    tenant_1 = "tenant-123"
    tenant_2 = "tenant-234"

    stream_name_1 = "bani-create-multiple-stream-1"
    stream_name_2 = "bani-create-multiple-stream-2"
    stream_name_3 = "bani-create-multiple-stream-3"

    message_1 = "tenant-123 message 1"
    message_2 = "tenant-123 message 2"
    message_3 = "tenant-456 message 1"

    handler_1 = fn (_prev, curr) ->
      assert curr == message_1
      Process.send(test_pid, {:expect_handler_1_called, ref}, [])

      {:ok, curr}
    end

    handler_2 = fn (_prev, curr) ->
      assert curr == message_2
      Process.send(test_pid, {:expect_handler_2_called, ref}, [])

      {:ok, curr}
    end

    handler_3 = fn (_prev, curr) ->
      assert curr == message_3
      Process.send(test_pid, {:expect_handler_3_called, ref}, [])

      {:ok, curr}
    end

    :ok = TestBani.add_tenant(tenant_1, @conn_opts)
    :ok = TestBani.add_tenant(tenant_2, @conn_opts)

    :ok = TestBani.create_stream(tenant_1, stream_name_1)
    :ok = TestBani.create_stream(tenant_1, stream_name_2)
    :ok = TestBani.create_stream(tenant_2, stream_name_3)

    :ok = TestBani.create_publisher(tenant_1, stream_name_1)
    :ok = TestBani.create_publisher(tenant_1, stream_name_2)
    :ok = TestBani.create_publisher(tenant_2, stream_name_3)

    :ok = TestBani.create_subscriber(tenant_1, stream_name_1, "test-sink", handler_1, %{}, 0, :exactly_once)
    :ok = TestBani.create_subscriber(tenant_1, stream_name_2, "test-sink", handler_2, %{}, 0, :exactly_once)
    :ok = TestBani.create_subscriber(tenant_2, stream_name_3, "test-sink", handler_3, %{}, 0, :exactly_once)

    :ok = TestBani.publish(tenant_1, stream_name_1, message_1)
    :ok = TestBani.publish(tenant_1, stream_name_2, message_2)
    :ok = TestBani.publish(tenant_2, stream_name_3, message_3)

    assert_receive {:expect_handler_1_called, ^ref}
    assert_receive {:expect_handler_2_called, ^ref}
    assert_receive {:expect_handler_3_called, ^ref}

    :ok = TestBani.delete_subscriber(tenant_1, stream_name_1, "test-sink")
    :ok = TestBani.delete_subscriber(tenant_1, stream_name_2, "test-sink")
    :ok = TestBani.delete_subscriber(tenant_2, stream_name_3, "test-sink")

    :ok = TestBani.delete_publisher(tenant_1, stream_name_1)
    :ok = TestBani.delete_publisher(tenant_1, stream_name_2)
    :ok = TestBani.delete_publisher(tenant_2, stream_name_3)

    :ok = TestBani.delete_stream(tenant_1, stream_name_1)
    :ok = TestBani.delete_stream(tenant_1, stream_name_2)
    :ok = TestBani.delete_stream(tenant_2, stream_name_3)

    :ok = TestBani.remove_tenant(tenant_1)
    :ok = TestBani.remove_tenant(tenant_2)
  end

  @tag capture_log: true
  test "creates multiple subscribtions for single stream" do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-123"
    stream_name = "bani-create-multiple-subscribers-for-single-stream"
    message = "message 1"

    handler_1 = fn (_prev, curr) ->
      assert curr == message
      Process.send(test_pid, {:expect_handler_1_called, ref}, [])

      {:ok, curr}
    end

    handler_2 = fn (_prev, curr) ->
      assert curr == message
      Process.send(test_pid, {:expect_handler_2_called, ref}, [])

      {:ok, curr}
    end

    :ok = TestBani.add_tenant(tenant, @conn_opts)
    :ok = TestBani.create_stream(tenant, stream_name)
    :ok = TestBani.create_publisher(tenant, stream_name)

    :ok = TestBani.create_subscriber(tenant, stream_name, "test-sink-1", handler_1, %{}, 0, :exactly_once)
    :ok = TestBani.create_subscriber(tenant, stream_name, "test-sink-2", handler_2, %{}, 0, :exactly_once)

    :ok = TestBani.publish(tenant, stream_name, message)

    assert_receive {:expect_handler_1_called, ^ref}
    assert_receive {:expect_handler_2_called, ^ref}

    :ok = TestBani.delete_subscriber(tenant, stream_name, "test-sink-1")
    :ok = TestBani.delete_subscriber(tenant, stream_name, "test-sink-2")
    :ok = TestBani.delete_publisher(tenant, stream_name)
    :ok = TestBani.delete_stream(tenant, stream_name)

    :ok = TestBani.remove_tenant(tenant)
  end

  @tag capture_log: true
  test "handles subscription from offset" do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-123"
    stream_name = "bani-handles-subscription-from-offset"
    message_1 = "message 1"
    message_2 = "message 2"
    message_3 = "message 3"

    handler = fn (acc, curr) ->
      if (acc == 0) do
        assert curr == message_2
      else
        assert curr == message_3
      end

      Process.send(test_pid, {:expect_handler_called, ref}, [])

      {:ok, acc + 1}
    end

    :ok = TestBani.add_tenant(tenant, @conn_opts)
    :ok = TestBani.create_stream(tenant, stream_name)
    :ok = TestBani.create_publisher(tenant, stream_name)

    on_exit(fn ->
      :ok = TestBani.delete_stream(tenant, stream_name)
      :ok = TestBani.remove_tenant(tenant)
    end)

    :ok = TestBani.publish(tenant, stream_name, message_1)
    :ok = TestBani.publish(tenant, stream_name, message_2)
    :ok = TestBani.publish(tenant, stream_name, message_3)

    :ok = TestBani.create_subscriber(tenant, stream_name, "test-sink-1", handler, 0, 1, :exactly_once)

    assert_receive {:expect_handler_called, ^ref}
  end
end
