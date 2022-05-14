defmodule Bani.Store do
  @behaviour Bani.Store.StoreBehaviour

  @impl Bani.Store.StoreBehaviour
  def init_database() do
    :ok = init()
    :ok = :mnesia.start()
  end

  defp init() do
    case :mnesia.create_schema([node()]) do
      :ok ->
        :ok

      {:error, {_, {:already_exists, _}}} ->
        :ok

      error ->
        {:error, error}
    end
  end
end
