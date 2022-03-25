defmodule Bani.SubscriberStorageTest do
  use BaniTest.Case

  @acc %{a: "a"}
  @offset 1
  @poisoned false
  @stream_name "some-stream-name"
  @subscription_name "some-subscription-name"
  @tenant "some-tenant"

  @opts [
    acc: @acc,
    offset: @offset,
    poisoned: @poisoned,
    stream_name: @stream_name,
    subscription_name: @subscription_name,
    tenant: @tenant
  ]

  test "initializes and gets values" do
    start_supervised!({Bani.SubscriberStorage, @opts})

    values = Bani.SubscriberStorage.values(@tenant, @stream_name, @subscription_name)
    assert values.acc == @acc
    assert values.offset == @offset
    assert values.poisoned == @poisoned
    assert values.stream_name == @stream_name
    assert values.subscription_name == @subscription_name
    assert values.tenant == @tenant
  end

  test "gets acc" do
    start_supervised!({Bani.SubscriberStorage, @opts})

    assert @acc == Bani.SubscriberStorage.acc(@tenant, @stream_name, @subscription_name)
  end

  test "gets offset" do
    start_supervised!({Bani.SubscriberStorage, @opts})

    assert @offset == Bani.SubscriberStorage.offset(@tenant, @stream_name, @subscription_name)
  end

  test "updates values" do
    start_supervised!({Bani.SubscriberStorage, @opts})

    count = 4
    updated_acc = %{a: "updated a"}
    Bani.SubscriberStorage.update(@tenant, @stream_name, @subscription_name, count, updated_acc)

    values = Bani.SubscriberStorage.values(@tenant, @stream_name,@subscription_name)
    assert values.acc == updated_acc
    assert values.offset == @offset + count
  end

  test "poisons" do
    start_supervised!({Bani.SubscriberStorage, @opts})

    values = Bani.SubscriberStorage.values(@tenant, @stream_name, @subscription_name)
    refute values.poisoned
    refute values.poisoned_err

    err = "some err"
    Bani.SubscriberStorage.poison(@tenant, @stream_name, @subscription_name, err)

    updated_values = Bani.SubscriberStorage.values(@tenant, @stream_name, @subscription_name)
    assert updated_values.poisoned
    assert updated_values.poisoned_err == err
  end
end
