defmodule Bani.TenantTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  test "initializes" do
    conn_opts = [a: "a"]
    tenant = "some tenant"

    opts = [
      conn_opts: conn_opts,
      scheduling: Bani.MockScheduling,
      tenant: tenant
    ]

    start_supervised!({Bani.Tenant, opts})
  end

  test "creates and deletes stream" do
    test_pid = self()
    ref = make_ref()

    stream_name = "tenant-creates-and-deletes-stream"
    conn_opts = [a: "a"]
    tenant = "some tenant"

    opts = [
      conn_opts: conn_opts,
      scheduling: Bani.MockScheduling,
      tenant: tenant
    ]

    expect(Bani.MockScheduling, :create_stream, fn (conn_opts_, stream_name_) ->
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_create_stream_called, ref}, [])

      :ok
    end)

    expect(Bani.MockScheduling, :delete_stream, fn (conn_opts_, stream_name_) ->
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_delete_stream_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Tenant, opts})

    assert :ok = Bani.Tenant.create_stream(tenant, stream_name)
    assert :ok = Bani.Tenant.delete_stream(tenant, stream_name)

    assert_receive {:expect_create_stream_called, ^ref}
    assert_receive {:expect_delete_stream_called, ^ref}
  end

  test "creates and deletes publisher" do
    test_pid = self()
    ref = make_ref()

    stream_name = "tenant-creates-and-deletes-publisher"
    conn_opts = [a: "a"]
    tenant = "some tenant"

    opts = [
      conn_opts: conn_opts,
      scheduling: Bani.MockScheduling,
      tenant: tenant
    ]

    expect(Bani.MockScheduling, :create_publisher, fn (tenant_, conn_opts_, stream_name_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_create_publisher_called, ref}, [])

      :ok
    end)

    expect(Bani.MockScheduling, :delete_publisher, fn (tenant_, stream_name_) ->
      assert tenant_ == tenant
      assert stream_name_ == stream_name

      Process.send(test_pid, {:expect_delete_publisher_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Tenant, opts})

    assert :ok = Bani.Tenant.create_publisher(tenant, stream_name)
    assert :ok = Bani.Tenant.delete_publisher(tenant, stream_name)

    assert_receive {:expect_create_publisher_called, ^ref}
    assert_receive {:expect_delete_publisher_called, ^ref}
  end

  test "creates and deletes subscriber" do
    test_pid = self()
    ref = make_ref()

    stream_name = "tenant-creates-and-deletes-subscriber"
    conn_opts = [a: "a"]
    tenant = "some tenant"
    subscription_name = "some-subscription-name"
    handler = fn (_prev, curr) -> {:ok, curr} end

    offset = 0
    acc = %{}

    opts = [
      conn_opts: conn_opts,
      scheduling: Bani.MockScheduling,
      tenant: tenant
    ]

    expect(Bani.MockScheduling, :create_subscriber, fn (tenant_, conn_opts_, stream_name_, subscription_name_, handler_, acc_, offset_) ->
      assert tenant_ == tenant
      assert conn_opts_ == conn_opts
      assert stream_name_ == stream_name
      assert subscription_name_ == subscription_name
      assert handler_ == handler

      assert acc_ == acc
      assert offset_ == offset

      Process.send(test_pid, {:expect_create_subscriber_called, ref}, [])

      :ok
    end)

    expect(Bani.MockScheduling, :delete_subscriber, fn (tenant_, stream_name_, subscription_name_) ->
      assert tenant_ == tenant
      assert stream_name_ == stream_name
      assert subscription_name_ == subscription_name

      Process.send(test_pid, {:expect_delete_subscriber_called, ref}, [])

      :ok
    end)

    start_supervised!({Bani.Tenant, opts})

    assert :ok = Bani.Tenant.create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset)
    assert :ok = Bani.Tenant.delete_subscriber(tenant, stream_name, subscription_name)

    assert_receive {:expect_create_subscriber_called, ^ref}
    assert_receive {:expect_delete_subscriber_called, ^ref}
  end
end
