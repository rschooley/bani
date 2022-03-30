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

  test "creates stream and subscribes" do
    test_pid = self()
    ref = make_ref()

    tenant = "tenant-123"
    stream_name = "creates-stream-and-subscribes"
    message = "sample message"

    handler = fn (_prev, curr) ->
      assert curr == message

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, curr}
    end

    TestBani.foo("log this")

    # :ok = TestBani.add_tenant(tenant, @conn_opts)
    # :ok = TestBani.create_stream(tenant, stream_name)
    # :ok = TestBani.create_publisher(tenant, stream_name)
    # # :ok = TestBani.create_subscriber(tenant, stream_name, "test-sink", handler)

    # :ok = TestBani.publish(tenant, stream_name, message)

    # # assert_receive {:expect_called, ^ref}

    # # # :ok = TestBani.delete_subscriber(stream_name)
    # # # :ok = TestBani.delete_publisher(tenant, stream_name)
    # :ok = TestBani.delete_stream(tenant, stream_name)

    # # # TODO: remove tenant
    # :ok = DynamicSupervisor.stop(Bani.SchedulerDynamicSupervisor)
  end

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

    :ok = TestBani.create_subscriber(tenant_1, stream_name_1, "test-sink", handler_1)
    :ok = TestBani.create_subscriber(tenant_1, stream_name_2, "test-sink", handler_2)
    :ok = TestBani.create_subscriber(tenant_2, stream_name_3, "test-sink", handler_3)

    :ok = TestBani.publish(tenant_1, stream_name_1, message_1)
    :ok = TestBani.publish(tenant_1, stream_name_2, message_2)
    :ok = TestBani.publish(tenant_2, stream_name_3, message_3)

    assert_receive {:expect_handler_1_called, ^ref}
    assert_receive {:expect_handler_2_called, ^ref}
    assert_receive {:expect_handler_3_called, ^ref}

    # :ok = TestBani.delete_subscriber(stream_name)
    # :ok = TestBani.delete_publisher(tenant, stream_name)
    :ok = TestBani.delete_stream(tenant_1, stream_name_1)
    :ok = TestBani.delete_stream(tenant_1, stream_name_2)
    :ok = TestBani.delete_stream(tenant_2, stream_name_3)

    # TODO: remove tenant
    :ok = DynamicSupervisor.stop(Bani.SchedulerDynamicSupervisor)
  end

  # test "deletes publisher and subscriber when deleting stream" do
  # end

  #   test "allows two connection managers" do
  #   {:ok, pid_1} = GenServer.start(Bani.ConnectionManager, @valid_opts)
  #   {:ok, pid_2} = GenServer.start(Bani.ConnectionManager, @valid_opts)

  #   assert pid_1 != pid_2

  #   assert :ok = GenServer.stop(pid_1)
  #   assert :ok = GenServer.stop(pid_2)
  # end

  #   test "crashing conn crashes connection manager" do
  #   {:ok, pid} = GenServer.start(Bani.ConnectionManager, @valid_opts)

  #   conn = Bani.ConnectionManager.conn(pid)
  #   Process.exit(conn, :kill)

  #   wait_for_passing(_2_seconds = 2000, fn ->
  #     refute Process.alive?(conn)
  #     refute Process.alive?(pid)
  #   end)
  # end
end


#  @valid_opts [
#     {:host, "localhost"},
#     {:port, 5552},
#     {:username, "guest"},
#     {:password, "guest"},
#     {:vhost, "/test"}
#   ]

#   test "publishes and subscribes" do
#     stream_name = "publishes-and-subscribes"

#     # setup
#     {:ok, conn} = Bani.Broker.connect(@valid_opts)
#     :ok = Bani.Broker.create_stream(conn, stream_name)

#     # steps
#     message = "test message"
#     test_pid = self()
#     ref = make_ref()

#     handler = fn (_prev, curr) ->
#       assert curr == message
#       Process.send(test_pid, {:handler_called, ref}, [])

#       {:ok, curr}
#     end

#     {:ok, pid} = start_supervised({Bani.ConnectionSupervisor, @valid_opts})
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid, stream_name, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid, stream_name, 1, handler)

#     Bani.Publisher.publish_sync(stream_name, message, 1)

#     assert_receive {:handler_called, ^ref}

#     # cleanup
#     :ok = Bani.Broker.delete_stream(conn, stream_name)
#     :ok = Bani.Broker.disconnect(conn)
#   end

#   test "allows two connection supervisors" do
#     assert {:ok, pid_1} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     assert {:ok, pid_2} = Bani.ConnectionSupervisor.start_link(@valid_opts)

#     assert pid_1 != pid_2

#     assert :ok = Supervisor.stop(pid_1)
#     assert :ok = Supervisor.stop(pid_2)
#   end

#   test "subscribes to same stream twice in same connection" do
#     stream_name = "subscribes-to-same-stream-twice-in-same-connection"

#     # setup
#     {:ok, conn} = Bani.Broker.connect(@valid_opts)
#     :ok = Bani.Broker.create_stream(conn, stream_name)

#     # steps
#     message = "test message"

#     handler_1 = fn (_prev, curr) ->
#       assert curr == message

#       {:ok, curr}
#     end

#     handler_2 = fn (_prev, curr) ->
#       assert curr == message

#       {:ok, curr}
#     end

#     {:ok, pid} = start_supervised({Bani.ConnectionSupervisor, @valid_opts})
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid, stream_name, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid, stream_name, 1, handler_1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid, stream_name, 2, handler_2)

#     Bani.Publisher.publish_sync(stream_name, message, 1)

#     Process.sleep(100)

#     # cleanup
#     :ok = Bani.Broker.delete_stream(conn, stream_name)
#     :ok = Bani.Broker.disconnect(conn)
#   end

#   test "subscribes to same stream twice in different connections" do
#     stream_name = "subscribes-to-same-stream-twice-in-different-connections"

#     # setup
#     {:ok, conn} = Bani.Broker.connect(@valid_opts)
#     :ok = Bani.Broker.create_stream(conn, stream_name)

#     # steps
#     message = "test message"

#     handler_1 = fn (_prev, curr) ->
#       assert curr == message

#       {:ok, curr}
#     end

#     handler_2 = fn (_prev, curr) ->
#       assert curr == message

#       {:ok, curr}
#     end

#     {:ok, pid_1} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid_1, stream_name, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_1, stream_name, 1, handler_1)

#     {:ok, pid_2} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_2, stream_name, 1, handler_2)

#     Bani.Publisher.publish_sync(stream_name, message, 1)

#     Process.sleep(100)

#     # cleanup
#     :ok = Supervisor.stop(pid_1)
#     :ok = Supervisor.stop(pid_2)
#     :ok = Bani.Broker.delete_stream(conn, stream_name)
#     :ok = Bani.Broker.disconnect(conn)
#   end

#   test "subscribes to different streams in different connections" do
#     stream_name_1 = "subscribes-to-different-streams-in-different-connections-1"
#     stream_name_2 = "subscribes-to-different-streams-in-different-connections-2"

#     # setup
#     {:ok, conn} = Bani.Broker.connect(@valid_opts)
#     :ok = Bani.Broker.create_stream(conn, stream_name_1)
#     :ok = Bani.Broker.create_stream(conn, stream_name_2)

#     # steps
#     message_1 = "test message 1"
#     message_2 = "test message 2"

#     handler_1 = fn (_prev, curr) ->
#       assert curr == message_1

#       {:ok, curr}
#     end

#     handler_2 = fn (_prev, curr) ->
#       assert curr == message_2

#       {:ok, curr}
#     end

#     {:ok, pid_1} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid_1, stream_name_1, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_1, stream_name_1, 1, handler_1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_1, stream_name_1, 2, handler_1)

#     {:ok, pid_2} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid_2, stream_name_2, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_2, stream_name_2, 1, handler_2)

#     Bani.Publisher.publish_sync(stream_name_1, message_1, 1)
#     Bani.Publisher.publish_sync(stream_name_2, message_2, 1)

#     Process.sleep(100)

#     # cleanup
#     :ok = Supervisor.stop(pid_1)
#     :ok = Supervisor.stop(pid_2)
#     :ok = Bani.Broker.delete_stream(conn, stream_name_1)
#     :ok = Bani.Broker.delete_stream(conn, stream_name_2)
#     :ok = Bani.Broker.disconnect(conn)
#   end

#   test "subscribes to different streams in same connection" do
#     stream_name_1 = "subscribes-to-different-streams-in-same-connection-1"
#     stream_name_2 = "subscribes-to-different-streams-in-same-connection-2"

#     # setup
#     {:ok, conn} = Bani.Broker.connect(@valid_opts)
#     :ok = Bani.Broker.create_stream(conn, stream_name_1)
#     :ok = Bani.Broker.create_stream(conn, stream_name_2)

#     # steps
#     message_1 = "test message 1"
#     message_2 = "test message 2"

#     handler_1 = fn (_prev, curr) ->
#       assert curr == message_1

#       {:ok, curr}
#     end

#     handler_2 = fn (_prev, curr) ->
#       assert curr == message_2

#       {:ok, curr}
#     end

#     {:ok, pid_1} = Bani.ConnectionSupervisor.start_link(@valid_opts)
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid_1, stream_name_1, 1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_publisher(pid_1, stream_name_2, 2)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_1, stream_name_1, 1, handler_1)
#     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid_1, stream_name_2, 2, handler_2)

#     Bani.Publisher.publish_sync(stream_name_1, message_1, 1)
#     Bani.Publisher.publish_sync(stream_name_2, message_2, 1)

#     Process.sleep(100)

#     # cleanup
#     :ok = Supervisor.stop(pid_1)
#     :ok = Bani.Broker.delete_stream(conn, stream_name_1)
#     :ok = Bani.Broker.delete_stream(conn, stream_name_2)
#     :ok = Bani.Broker.disconnect(conn)
#   end

  # describe "availability" do
  #   import Liveness

  #   test "killing publisher" do
  #     stream_name = "killing-publisher"

  #     # setup
  #     {:ok, conn} = Bani.Broker.connect(@valid_opts)
  #     :ok = Bani.Broker.create_stream(conn, stream_name)

  #     # steps
  #     message = "test message"

  #     handler = fn (_prev, curr) ->
  #       IO.inspect("does this run")
  #       assert curr == message

  #       {:ok, curr}
  #     end

  #     {:ok, pid} = start_supervised({Bani.ConnectionSupervisor, @valid_opts})
  #     {:ok, publisher} = Bani.ConnectionSupervisor.add_publisher(pid, stream_name, 1)
  #     {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(pid, stream_name, 1, handler)

  #     Process.exit(publisher, :kill)
  #     eventually(fn ->  Registry.lookup(Bani.Registry, "#{stream_name}-publisher") end)
  #     Bani.Publisher.publish_sync(stream_name, message, 1)

  #     Process.sleep(100)

  #     # cleanup
  #     :ok = Bani.Broker.delete_stream(conn, stream_name)
  #     :ok = Bani.Broker.disconnect(conn)
  #   end
  # end

  # 1 restart, publisher/subscriber still work
  # 2 kill conn, supervisor restarts
  # 3 kill connection manager, supervisor restarts
  # 4 kill publisher, supervisor fine, publisher restarts
  # 5 kill subscriber, supervisor fine, subscriber restarts
# end

# test "subscribes after connection restart" do
#     stream_name = "subscribes-after-connection-restart"
#     message = "some message"

#     {:ok, conn} = Broker.connect(@valid_opts)
#     :ok = Broker.create_stream(conn, stream_name)
#     :ok = Broker.subscribe(conn, stream_name, 1)
#     :ok = Broker.create_publisher(conn, stream_name, 10, "some-publisher")

#     Process.exit(conn, :kill)

#     :ok = Broker.publish_sync(conn, 10, message, 1)

#     {:ok, {[result], _other}} =
#       receive do
#         {:deliver, _response_code, chunk} ->
#           :lake.chunk_to_messages(chunk)
#       after
#         5000 ->
#           exit(:timeout)
#       end

#     assert result == message

#     :ok = Broker.delete_publisher(conn, 10)
#     :ok = Broker.unsubscribe(conn, 1)
#     :ok = Broker.delete_stream(conn, stream_name)
#     :ok = Broker.disconnect(conn)
#   end