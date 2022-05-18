defmodule Bani.Store.SchedulingStoreBehaviour do
  @callback init_store(tenant :: String) :: :ok

  @callback delete_store(tenant :: String) :: :ok

  @callback next_available_pubsub_opts(
              tenant :: String,
              pub_sub_type :: atom()
            ) :: {:ok, term()} | {:error, term()}

  @callback release_available_pubsub_id(
              tenant :: String,
              connection_id :: String,
              pub_sub_type :: atom(),
              pubsub_id :: Integer.t()
            ) :: {:ok, term()} | {:error, term()}
end
