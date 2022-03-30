defmodule Bani.SchedulingTest do
  use BaniTest.Case, async: true

  @conn_opts [
    {:host, "localhost"},
    {:port, 5552},
    {:username, "guest"},
    {:password, "guest"},
    {:vhost, "/test"}
  ]

  test "creates and deletes stream" do
    tenant = "some tenant"
    stream_name = "scheduling-create-delete-stream"

    assert :ok = Bani.Scheduling.create_stream(tenant, @conn_opts, stream_name)
    assert :ok = Bani.Scheduling.delete_stream(tenant, @conn_opts, stream_name)
  end

  test "creates publisher" do
    tenant = "some tenant"
    stream_name = "scheduling-create-publisher"

    assert :ok = Bani.Scheduling.create_stream(tenant, @conn_opts, stream_name)
    assert :ok = Bani.Scheduling.create_publisher(tenant, @conn_opts, stream_name)

    # TODO: results in {:state, 6... from lake
    # Bani.Scheduling.delete_stream(tenant, @conn_opts, stream_name)
  end

  test "creates subscriber" do
    tenant = "some tenant"
    stream_name = "scheduling-create-subscriber"
    handler = fn (_prev, curr) -> {:ok, curr} end

    assert :ok = Bani.Scheduling.create_stream(tenant, @conn_opts, stream_name)
    assert {:ok, _} = Bani.Scheduling.create_subscriber(tenant, @conn_opts, stream_name, handler)

    # TODO: results in {:state, 6... from lake
    # Bani.Scheduling.delete_stream(tenant, @conn_opts, stream_name)
  end
end
