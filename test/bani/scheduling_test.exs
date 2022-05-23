defmodule Bani.SchedulingTest do
  use BaniTest.Case, async: false

  # suppress mnesia stop info & Bani logger info
  @moduletag :capture_log

  @conn_opts [
    {:host, "localhost"},
    {:port, 5552},
    {:username, "guest"},
    {:password, "guest"},
    {:vhost, "/test"}
  ]

  test "creates and deletes stream" do
    stream_name = "scheduling-creates-and-deletes-stream"

    assert :ok = Bani.Scheduling.create_stream(@conn_opts, stream_name)
    assert :ok = Bani.Scheduling.delete_stream(@conn_opts, stream_name)
  end

  test "creates and deletes publisher" do
    tenant = "some tenant"
    stream_name = "scheduling-creates-and-deletes-publisher"

    # Scheduling is made to back the Tenant GenServer which inits the store(s)
    :ok = Bani.Store.SubscriberStore.init_store(tenant)
    :ok = Bani.Store.SchedulingStore.init_store(tenant)
    :ok = Bani.Scheduling.create_stream(@conn_opts, stream_name)

    assert :ok = Bani.Scheduling.create_publisher(tenant, @conn_opts, stream_name)
    assert :ok = Bani.Scheduling.delete_publisher(tenant, stream_name)

    Bani.Scheduling.delete_stream(@conn_opts, stream_name)
  end

  test "creates and deletes subscriber" do
    tenant = "some tenant"
    stream_name = "scheduling-creates-and-deletes-subscriber"
    subscription_name = "subscription-name"
    handler = fn (_prev, curr) -> {:ok, curr} end

    # Scheduling is made to back the Tenant GenServer which inits the store(s)
    :ok = Bani.Store.SubscriberStore.init_store(tenant)
    :ok = Bani.Store.SchedulingStore.init_store(tenant)
    :ok = Bani.Scheduling.create_stream(@conn_opts, stream_name)

    assert :ok = Bani.Scheduling.create_subscriber(tenant, @conn_opts, stream_name, subscription_name, handler, %{}, 0, :at_least_once)
    assert :ok = Bani.Scheduling.delete_subscriber(tenant, stream_name, subscription_name)

    Bani.Scheduling.delete_stream(@conn_opts, stream_name)
  end
end
