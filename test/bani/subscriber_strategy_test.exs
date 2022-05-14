defmodule Bani.SubscriberStrategyTest do
  use BaniTest.Case, async: false

  import Mox
  import ExUnit.CaptureLog

  setup [:set_mox_global, :verify_on_exit!]

  describe "perform/5 :exactly_once" do
    @strategy :exactly_once

    test "succeeds" do
      test_pid = self()
      ref_lock = make_ref()
      ref_process = make_ref()
      ref_unlock = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :lock_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_lock_subscriber_called, ref_lock}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      expect(Bani.MockSubscriberStore, :unlock_subscriber, fn (tenant_, subscriber_key_, new_acc_, inc_count_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key
        assert new_acc_ == new_acc
        assert inc_count_ == inc_count

        Process.send(test_pid, {:expect_unlock_subscriber_called, ref_unlock}, [])

        {:ok, "some result"}
      end)

      assert {:ok, "some result"} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_lock_subscriber_called, ^ref_lock}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_unlock_subscriber_called, ^ref_unlock}
    end

    test "lock_subscriber fails" do
      test_pid = self()
      ref_lock = make_ref()
      ref_process = make_ref()
      ref_unlock = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :lock_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_lock_subscriber_called, ref_lock}, [])

        {:error, "some error"}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      stub(Bani.MockSubscriberStore, :unlock_subscriber, fn (_, _, _, _) ->
        Process.send(test_pid, {:expect_unlock_subscriber_called, ref_unlock}, [])

        {:ok, "some result"}
      end)

      assert {:error, "some error"} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_lock_subscriber_called, ^ref_lock}
      refute_receive {:expect_process_called, ^ref_process}
      refute_receive {:expect_unlock_subscriber_called, ^ref_unlock}
    end

    test "process_fn partially fails" do
      test_pid = self()
      ref_lock = make_ref()
      ref_process = make_ref()
      ref_unlock = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :lock_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_lock_subscriber_called, ref_lock}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:partial_error, new_acc, inc_count}
      end

      expect(Bani.MockSubscriberStore, :unlock_subscriber, fn (tenant_, subscriber_key_, new_acc_, inc_count_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key
        assert new_acc_ == new_acc
        assert inc_count_ == inc_count

        Process.send(test_pid, {:expect_unlock_subscriber_called, ref_unlock}, [])

        {:ok, "some result"}
      end)

      assert {:error, _} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_lock_subscriber_called, ^ref_lock}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_unlock_subscriber_called, ^ref_unlock}
    end

    test "unlock_subscriber fails" do
      test_pid = self()
      ref_lock = make_ref()
      ref_process = make_ref()
      ref_unlock = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :lock_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_lock_subscriber_called, ref_lock}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      stub(Bani.MockSubscriberStore, :unlock_subscriber, fn (_, _, _, _) ->
        Process.send(test_pid, {:expect_unlock_subscriber_called, ref_unlock}, [])

        {:error, "some error"}
      end)

      assert capture_log(fn ->
        assert {:error, _} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)
      end) =~ "some error"

      assert_receive {:expect_lock_subscriber_called, ^ref_lock}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_unlock_subscriber_called, ^ref_unlock}
    end
  end

  describe "perform/5 :at_least_once" do
    @strategy :at_least_once

    test "succeeds" do
      test_pid = self()
      ref_get = make_ref()
      ref_process = make_ref()
      ref_update = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :get_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_get_subscriber_called, ref_get}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      expect(Bani.MockSubscriberStore, :update_subscriber, fn (tenant_, subscriber_key_, new_acc_, inc_count_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key
        assert new_acc_ == new_acc
        assert inc_count_ == inc_count

        Process.send(test_pid, {:expect_update_subscriber_called, ref_update}, [])

        {:ok, "some result"}
      end)

      assert {:ok, "some result"} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_get_subscriber_called, ^ref_get}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_update_subscriber_called, ^ref_update}
    end

    test "get_subscriber fails" do
      test_pid = self()
      ref_get = make_ref()
      ref_process = make_ref()
      ref_update = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :get_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_get_subscriber_called, ref_get}, [])

        {:error, "some error"}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      stub(Bani.MockSubscriberStore, :update_subscriber, fn (_, _, _, _) ->
        Process.send(test_pid, {:expect_update_subscriber_called, ref_update}, [])

        {:ok, "some result"}
      end)

      assert {:error, "some error"} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_get_subscriber_called, ^ref_get}
      refute_receive {:expect_process_called, ^ref_process}
      refute_receive {:expect_update_subscriber_called, ^ref_update}
    end

    test "process_fn partially fails" do
      test_pid = self()
      ref_get = make_ref()
      ref_process = make_ref()
      ref_update = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :get_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_get_subscriber_called, ref_get}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:partial_error, new_acc, inc_count}
      end

      expect(Bani.MockSubscriberStore, :update_subscriber, fn (tenant_, subscriber_key_, new_acc_, inc_count_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key
        assert new_acc_ == new_acc
        assert inc_count_ == inc_count

        Process.send(test_pid, {:expect_update_subscriber_called, ref_update}, [])

        {:ok, "some result"}
      end)

      assert {:error, _} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)

      assert_receive {:expect_get_subscriber_called, ^ref_get}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_update_subscriber_called, ^ref_update}
    end

    test "update_subscriber fails" do
      test_pid = self()
      ref_get = make_ref()
      ref_process = make_ref()
      ref_update = make_ref()

      tenant = "tenant-1"
      subscriber_key = "subscriber-key-1"
      acc = %{a: :b}
      new_acc = %{a: :c}
      inc_count = 3

      expect(Bani.MockSubscriberStore, :get_subscriber, fn (tenant_, subscriber_key_) ->
        assert tenant_ == tenant
        assert subscriber_key_ == subscriber_key

        Process.send(test_pid, {:expect_get_subscriber_called, ref_get}, [])

        {:ok, %Bani.Store.SubscriberState{acc: acc}}
      end)

      process_fn = fn acc_ ->
        assert acc_ == acc

        Process.send(test_pid, {:expect_process_called, ref_process}, [])

        {:ok, new_acc, inc_count}
      end

      stub(Bani.MockSubscriberStore, :update_subscriber, fn (_, _, _, _) ->
        Process.send(test_pid, {:expect_update_subscriber_called, ref_update}, [])

        {:error, "some error"}
      end)

      assert capture_log(fn ->
        assert {:error, _} = Bani.SubscriberStrategy.perform(@strategy, tenant, subscriber_key, Bani.MockSubscriberStore, process_fn)
      end) =~ "some error"

      assert_receive {:expect_get_subscriber_called, ^ref_get}
      assert_receive {:expect_process_called, ^ref_process}
      assert_receive {:expect_update_subscriber_called, ^ref_update}
    end
  end
end
