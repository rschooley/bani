defmodule Bani.StoreTest do
  use ExUnit.Case, async: false

  test "tenants are separated" do
    tenant_1 = "tenant-123"
    tenant_2 = "tenant-456"

    assert :ok = Bani.Store.init_store(tenant_1)
    assert :ok = Bani.Store.init_store(tenant_2)

    assert {connection_id_1, 0} = Bani.Store.next_available_pubsub_opts(tenant_1, :publisher)
    assert {connection_id_2, 0} = Bani.Store.next_available_pubsub_opts(tenant_2, :publisher)

    assert connection_id_1 != connection_id_2
  end

  for pubsub_type <- [:publisher, :subscriber] do
    test "gets next available #{pubsub_type} opts" do
      pubsub_type = unquote(pubsub_type)
      tenant = "tenant-123"

      assert :ok = Bani.Store.init_store(tenant)
      assert {connection_id_1, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      # rest have same connection id
      Enum.each(1..255, fn (index) ->
        assert {connection_id, ^index} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
        assert connection_id == connection_id_1
      end)

      # next after max (255) gets a new connection_id
      assert {connection_id_2, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      # rest have same connection id
      Enum.each(1..255, fn (index) ->
        assert {connection_id, ^index} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
        assert connection_id == connection_id_2
      end)

      # next after max (255) gets a new connection_id
      assert {connection_id_3, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      assert connection_id_1 != connection_id_2 != connection_id_3
    end
  end

  for pubsub_type <- [:publisher, :subscriber] do
    test "releases #{pubsub_type} id from available connection" do
      pubsub_type = unquote(pubsub_type)
      tenant = "tenant-123"

      assert :ok = Bani.Store.init_store(tenant)

      assert {connection_id, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      # release first in list and continue
      assert :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, pubsub_type, 0)
      assert {^connection_id, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
      assert {^connection_id, 1} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      # release first in list after getting more and continue
      assert :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, pubsub_type, 0)
      assert {^connection_id, 0} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
      assert {^connection_id, 2} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
      assert {^connection_id, 3} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
      assert {^connection_id, 4} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

      # release in the middle, next gets that released id
      assert :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, pubsub_type, 3)
      assert {^connection_id, 3} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
    end
  end

  for pubsub_type <- [:publisher, :subscriber] do
    test "releases #{pubsub_type} id from unavailable connection" do
      pubsub_type = unquote(pubsub_type)
      tenant = "tenant-123"
      connection_id = "connection-123"

      assert :ok = Bani.Store.init_store(tenant)

      # releasing from a connection not in the available table (all ids have been taken)
      assert :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, pubsub_type, 11)

      # next id uses that released id instead of creating a new connection id
      assert {^connection_id, 11} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
    end
  end

  for {target, other} <- [{:publisher, :subscriber}, {:subscriber, :publisher}] do
    test "getting next available & releasing pubsub opts for #{target} & #{other}" do
      target = unquote(target)
      other = unquote(other)

      tenant = "tenant-123"

      assert :ok = Bani.Store.init_store(tenant)

      # weave next available and releasing across pub sub types
      assert {connection_id_1, 0} = Bani.Store.next_available_pubsub_opts(tenant, target)
      assert {^connection_id_1, 0} = Bani.Store.next_available_pubsub_opts(tenant, other)

      assert {^connection_id_1, 1} = Bani.Store.next_available_pubsub_opts(tenant, target)
      assert {^connection_id_1, 1} = Bani.Store.next_available_pubsub_opts(tenant, other)
      assert {^connection_id_1, 2} = Bani.Store.next_available_pubsub_opts(tenant, target)

      assert :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id_1, target, 1)
      assert {^connection_id_1, 2} = Bani.Store.next_available_pubsub_opts(tenant, other)
      assert {^connection_id_1, 1} = Bani.Store.next_available_pubsub_opts(tenant, target)
    end
  end
end
