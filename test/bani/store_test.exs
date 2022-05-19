defmodule Bani.StoreTest do
  use ExUnit.Case, async: false

  alias Bani.Store
  alias Bani.Store.{SchedulingStore, SubscriberStore, TenantStore}
  alias Bani.Store.{SubscriberState, TenantState}

  describe "init_database/0" do
    # database is created in application.ex
    # test "succeeds when schema does not exist" do
    #   assert :ok = Store.init_database()
    # end

    test "succeeds when called multiple times" do
      assert :ok = Store.init_database()
      assert :ok = Store.init_database()
    end
  end

  describe "tenant store" do
    setup do
      on_exit(fn ->
        {:atomic, :ok} = :mnesia.clear_table(:tenants)
      end)
    end

    test "init_tenants/0 succeeds when table does not exist" do
      assert :ok = TenantStore.init_store()
    end

    test "init_tenants/0 succeeds when table does exist" do
      assert :ok = TenantStore.init_store()
      assert :ok = TenantStore.init_store()
    end

    test "adds, removes, lists tenant ids" do
      :ok = TenantStore.init_store()

      assert [] = TenantStore.list_tenant_ids()

      assert :ok = TenantStore.add_tenant("tenant-1", [])
      assert ["tenant-1"] = TenantStore.list_tenant_ids()

      assert :ok = TenantStore.add_tenant("tenant-2", [])
      assert ["tenant-2", "tenant-1"] = TenantStore.list_tenant_ids()

      assert :ok = TenantStore.remove_tenant("tenant-2")
      assert ["tenant-1"] = TenantStore.list_tenant_ids()
    end

    test "get_tenant/1 returns tenant" do
      :ok = TenantStore.init_store()

      assert :ok = TenantStore.add_tenant("tenant-1", a: :b)

      assert {:ok, %TenantState{conn_opts: [a: :b], id: "tenant-1"}} =
               TenantStore.get_tenant("tenant-1")
    end

    test "get_tenant/1 returns error when tenant does not exist" do
      :ok = TenantStore.init_store()

      assert {:error, _} = TenantStore.get_tenant("tenant-1")
    end

    test "add_tenant/1 can be called multiple times" do
      :ok = TenantStore.init_store()
      :ok = TenantStore.add_tenant("tenant-1", [])
      :ok = TenantStore.add_tenant("tenant-1", [])

      assert ["tenant-1"] = TenantStore.list_tenant_ids()
    end
  end

  describe "subscriber store" do
    test "add_subscriber/4 adds the subscriber record" do
      assert :ok = SubscriberStore.init_store("tenant-1")
      assert :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", nil, 0)

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "get_subscriber/2 returns the subscriber record" do
      assert :ok = SubscriberStore.init_store("tenant-1")
      assert :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", nil, 0)

      assert {:ok,
              %SubscriberState{
                acc: nil,
                locked: false,
                offset: 0,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "get_subscriber/2 returns an error when record not found" do
      assert {:error, _} = SubscriberStore.get_subscriber("tenant-21", "subscriber-key-1")
    end

    test "lock_subscriber/2 updates the subscriber record" do
      :ok = SubscriberStore.init_store("tenant-1")
      :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)

      # make sure returned value is good
      assert {:ok,
              %SubscriberState{
                acc: %{a: :b},
                locked: true,
                offset: 0,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.lock_subscriber("tenant-1", "subscriber-key-1")

      # make sure stored value is good
      assert {:ok,
              %SubscriberState{
                acc: %{a: :b},
                locked: true,
                offset: 0,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "lock_subscriber/2 returns an error when called on locked record" do
      :ok = SubscriberStore.init_store("tenant-1")
      :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)

      assert {:ok,
              %SubscriberState{
                acc: %{a: :b},
                locked: true,
                offset: 0,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.lock_subscriber("tenant-1", "subscriber-key-1")

      assert {:error, :already_locked} =
               SubscriberStore.lock_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "lock_subscriber/2 returns an error when called on missing tenant" do
      assert {:error, _} = SubscriberStore.lock_subscriber("tenant-1", "subscriber-key-1")
    end

    test "unlock_subscriber/4 updates the subscriber" do
      :ok = SubscriberStore.init_store("tenant-1")
      :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)
      {:ok, _} = SubscriberStore.lock_subscriber("tenant-1", "subscriber-key-1")

      # make sure returned value is good
      assert {:ok,
              %SubscriberState{
                acc: %{c: :d},
                locked: false,
                offset: 1,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.unlock_subscriber("tenant-1", "subscriber-key-1", %{c: :d}, 1)

      # make sure stored value is good
      assert {:ok,
              %SubscriberState{
                acc: %{c: :d},
                locked: false,
                offset: 1,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "unlock_subscriber/4 returns an error when called on unlocked record" do
      :ok = SubscriberStore.init_store("tenant-1")
      :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)

      assert {:error, :already_unlocked} =
               SubscriberStore.unlock_subscriber("tenant-1", "subscriber-key-1", %{c: :d}, 1)

      # make sure stored value is good
      assert {:ok,
              %SubscriberState{
                acc: %{a: :b},
                locked: false,
                offset: 0,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "unlock_subscriber/4 returns an error when called on missing tenant" do
      assert {:error, _} =
               SubscriberStore.unlock_subscriber("tenant-1", "subscriber-key-1", nil, 0)
    end

    test "updated_subscriber/4 updates the subscriber record" do
      :ok = SubscriberStore.init_store("tenant-1")
      :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)

      # make sure returned value is good
      assert {:ok,
              %SubscriberState{
                acc: %{a: :c},
                locked: false,
                offset: 1,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.update_subscriber("tenant-1", "subscriber-key-1", %{a: :c}, 1)

      # make sure stored value is good
      assert {:ok,
              %SubscriberState{
                acc: %{a: :c},
                locked: false,
                offset: 1,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "update_subscriber/4 returns an error when called on missing tenant" do
      assert {:error, _} =
               SubscriberStore.update_subscriber("tenant-1", "subscriber-key-1", %{a: :c}, 1)
    end

    test "remove_subscriber/2 removes the subscriber record" do
      :ok = SubscriberStore.init_store("tenant-1")

      assert :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", %{a: :b}, 0)
      assert :ok = SubscriberStore.remove_subscriber("tenant-1", "subscriber-key-1")

      assert {:error, _} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
    end

    test "remove_subscriber/2 returns an error when called on missing subscriber" do
      assert {:error, _} = SubscriberStore.remove_subscriber("tenant-1", "subscriber-key-1")
    end

    test "isolates tenant subscribers" do
      assert :ok = SubscriberStore.init_store("tenant-1")
      assert :ok = SubscriberStore.add_subscriber("tenant-1", "subscriber-key-1", "a", 1)

      assert :ok = SubscriberStore.init_store("tenant-2")
      assert :ok = SubscriberStore.add_subscriber("tenant-2", "subscriber-key-1", "b", 2)

      assert {:ok,
              %SubscriberState{
                acc: "a",
                locked: false,
                offset: 1,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-1", "subscriber-key-1")

      assert {:ok,
              %SubscriberState{
                acc: "b",
                locked: false,
                offset: 2,
                subscriber_key: "subscriber-key-1"
              }} = SubscriberStore.get_subscriber("tenant-2", "subscriber-key-1")

      assert :ok = SubscriberStore.delete_store("tenant-1")
      assert :ok = SubscriberStore.delete_store("tenant-2")
    end
  end

  describe "scheduling store" do
    test "tenants are separated" do
      tenant_1 = "tenant-123"
      tenant_2 = "tenant-456"

      assert :ok = SchedulingStore.init_store(tenant_1)
      assert :ok = SchedulingStore.init_store(tenant_2)

      assert {:ok, {connection_id_1, 0}} =
               SchedulingStore.next_available_pubsub_opts(tenant_1, :publisher)

      assert {:ok, {connection_id_2, 0}} =
               SchedulingStore.next_available_pubsub_opts(tenant_2, :publisher)

      assert connection_id_1 != connection_id_2

      assert :ok = SchedulingStore.delete_store(tenant_1)
      assert :ok = SchedulingStore.delete_store(tenant_2)
    end

    for pubsub_type <- [:publisher, :subscriber] do
      test "next_available_pubsub_opts/2 gets next available #{pubsub_type} opts" do
        pubsub_type = unquote(pubsub_type)
        tenant = "tenant-123"

        assert :ok = SchedulingStore.init_store(tenant)

        assert {:ok, {connection_id_1, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        # rest have same connection id
        Enum.each(1..255, fn index ->
          assert {:ok, {connection_id, ^index}} =
                   SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

          assert connection_id == connection_id_1
        end)

        # next after max (255) gets a new connection_id
        assert {:ok, {connection_id_2, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        # rest have same connection id
        Enum.each(1..255, fn index ->
          assert {:ok, {connection_id, ^index}} =
                   SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

          assert connection_id == connection_id_2
        end)

        # next after max (255) gets a new connection_id
        assert {:ok, {connection_id_3, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert connection_id_1 != connection_id_2 != connection_id_3

        assert :ok = SchedulingStore.delete_store(tenant)
      end
    end

    for pubsub_type <- [:publisher, :subscriber] do
      test "release_available_pubsub_id/4 releases #{pubsub_type} id from available connection" do
        pubsub_type = unquote(pubsub_type)
        tenant = "tenant-123"

        assert :ok = SchedulingStore.init_store(tenant)

        assert {:ok, {connection_id, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        # release first in list and continue
        assert :ok =
                 SchedulingStore.release_available_pubsub_id(
                   tenant,
                   connection_id,
                   pubsub_type,
                   0
                 )

        assert {:ok, {^connection_id, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert {:ok, {^connection_id, 1}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        # release first in list after getting more and continue
        assert :ok =
                 SchedulingStore.release_available_pubsub_id(
                   tenant,
                   connection_id,
                   pubsub_type,
                   0
                 )

        assert {:ok, {^connection_id, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert {:ok, {^connection_id, 2}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert {:ok, {^connection_id, 3}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert {:ok, {^connection_id, 4}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        # release in the middle, next gets that released id
        assert :ok =
                 SchedulingStore.release_available_pubsub_id(
                   tenant,
                   connection_id,
                   pubsub_type,
                   3
                 )

        assert {:ok, {^connection_id, 3}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert :ok = SchedulingStore.delete_store(tenant)
      end
    end

    for pubsub_type <- [:publisher, :subscriber] do
      test "release_available_pubsub_id/4 releases #{pubsub_type} id from unavailable connection" do
        pubsub_type = unquote(pubsub_type)
        tenant = "tenant-123"
        connection_id = "connection-123"

        assert :ok = SchedulingStore.init_store(tenant)

        # releasing from a connection not in the available table (all ids have been taken)
        assert :ok =
                 SchedulingStore.release_available_pubsub_id(
                   tenant,
                   connection_id,
                   pubsub_type,
                   11
                 )

        # next id uses that released id instead of creating a new connection id
        assert {:ok, {^connection_id, 11}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, pubsub_type)

        assert :ok = SchedulingStore.delete_store(tenant)
      end
    end

    for {target, other} <- [{:publisher, :subscriber}, {:subscriber, :publisher}] do
      test "next_available_pubsub_opts/2 & release_available_pubsub_id/4 isolates #{target} & #{other}" do
        target = unquote(target)
        other = unquote(other)

        tenant = "tenant-123"

        assert :ok = SchedulingStore.init_store(tenant)

        # weave next available and releasing across pub sub types
        assert {:ok, {connection_id_1, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, target)

        assert {:ok, {^connection_id_1, 0}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, other)

        assert {:ok, {^connection_id_1, 1}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, target)

        assert {:ok, {^connection_id_1, 1}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, other)

        assert {:ok, {^connection_id_1, 2}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, target)

        assert :ok =
                 SchedulingStore.release_available_pubsub_id(tenant, connection_id_1, target, 1)

        assert {:ok, {^connection_id_1, 2}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, other)

        assert {:ok, {^connection_id_1, 1}} =
                 SchedulingStore.next_available_pubsub_opts(tenant, target)

        assert :ok = SchedulingStore.delete_store(tenant)
      end
    end
  end
end
