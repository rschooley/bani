# TODO: move this to a behaviour so we can have Disk, InMemory, Mox, etc impl
#  remember that Application.ex loads this Registry and will have to check the
#  strategy to see what is loaded
defmodule Bani.Storage do
  def register(key, value) do
    {:ok, _} = Registry.register(Bani.AvailableConnectionRegistry, key, value)

    :ok
  end

  def lookup(key) do
    Registry.lookup(Bani.AvailableConnectionRegistry, key)
  end

  def update_value(key, new_value) do
    # TODO: this will change based on how distribution is handled
    :ok = Registry.unregister(Bani.AvailableConnectionRegistry, key)
    {:ok, _} = Registry.register(Bani.AvailableConnectionRegistry, key, new_value)

    :ok
  end
end
