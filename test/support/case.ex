defmodule BaniTest.Case do
  @moduledoc """
  This module defines the setup for tests.

  You may define functions here to be used as helpers in
  your tests.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import BaniTest.Case
    end
  end

  setup_all do
    :ok
  end

  def wait_for_passing(timeout, fun) when timeout > 0 do
    fun.()
  rescue
    _ ->
    Process.sleep(100)
    wait_for_passing(timeout - 100, fun)
  end

  def wait_for_passing(_timeout, fun), do: fun.()
end
