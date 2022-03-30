defmodule Bani.StorageTest do
  use ExUnit.Case, async: true

  test "registers and looksup" do
    key = "some key"
    value = "some value"

    assert :ok = Bani.Storage.register(key, value)
    assert [{self(), value}] == Bani.Storage.lookup(key)
  end

  test "updates value and looks up" do
    key = "some key"
    value_1 = "some value"
    value_2 = "some updated value"

    assert :ok = Bani.Storage.register(key, value_1)
    assert [{self(), value_1}] == Bani.Storage.lookup(key)

    assert :ok = Bani.Storage.update_value(key, value_2)
    assert [{self(), value_2}] == Bani.Storage.lookup(key)
  end
end
