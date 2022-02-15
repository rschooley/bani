defmodule BaniTest do
  use ExUnit.Case
  doctest Bani

  test "greets the world" do
    assert Bani.hello() == :world
  end
end
