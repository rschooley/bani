defmodule Bani.SchedulerTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  test "initializes" do
    conn_opts = [a: "a"]
    tenant = "some tenant"

    opts = [
      broker: Bani.MockBroker,
      scheduling: Bani.MockScheduling,
      conn_opts: conn_opts,
      tenant: tenant
    ]

    start_supervised!({Bani.Scheduler, opts})
  end

  # TODO: if a pub/sub exists in the stream and it is deleted

  test "creates and deletes stream" do
    test_pid = self()
    ref = make_ref()

    stream_name = "scheduler-create-stream"
    conn_opts = [a: "a"]
    tenant = "some tenant"

    opts = [
      scheduling: Bani.MockScheduling,
      conn_opts: conn_opts,
      tenant: tenant
    ]

    expect(Bani.MockScheduling, :create_stream, fn (tenant_, conn_opts_, stream_name_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_create_stream_called, ref}, [])

      :ok
    end)

    expect(Bani.MockScheduling, :delete_stream, fn (tenant_, conn_opts_, stream_name_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_delete_stream_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Scheduler, opts})

    assert :ok = Bani.Scheduler.create_stream(tenant, stream_name)
    assert :ok = Bani.Scheduler.delete_stream(tenant, stream_name)

    assert_receive {:expect_create_stream_called, ^ref}
    assert_receive {:expect_delete_stream_called, ^ref}
  end

  test "create_publisher" do
    test_pid = self()
    ref = make_ref()

    stream_name = "scheduler-create-publisher"
    conn_opts = [a: "a"]
    tenant = "some tenant"

    expect(Bani.MockScheduling, :create_publisher, fn (tenant_, conn_opts_, stream_name_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    opts = [
      scheduling: Bani.MockScheduling,
      conn_opts: conn_opts,
      tenant: tenant
    ]

    start_supervised!({Bani.Scheduler, opts})

    assert :ok = Bani.Scheduler.create_publisher(tenant, stream_name)
    assert_receive {:expect_called, ^ref}
  end

  test "create_subscriber" do
    test_pid = self()
    ref = make_ref()

    stream_name = "scheduler-create-subscriber"
    conn_opts = [a: "a"]
    tenant = "some tenant"
    handler = fn (_prev, curr) -> {:ok, curr} end

    expect(Bani.MockScheduling, :create_subscriber, fn (tenant_, conn_opts_, stream_name_, handler_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name
      assert handler_ == handler

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, self()}
    end)

    opts = [
      scheduling: Bani.MockScheduling,
      conn_opts: conn_opts,
      tenant: tenant
    ]

    start_supervised!({Bani.Scheduler, opts})

    assert :ok = Bani.Scheduler.create_subscriber(tenant, stream_name, handler)
    assert_receive {:expect_called, ^ref}
  end
end
