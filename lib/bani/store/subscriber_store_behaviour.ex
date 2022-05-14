defmodule Bani.Store.SubscriberStoreBehaviour do
  @callback init_store(
              tenant :: String
            ) :: :ok

  @callback delete_store(
              tenant :: String
            ) :: :ok

  @callback add_subscriber(
              tenant :: String.t(),
              subscriber_key :: String.t(),
              acc :: term(),
              offset :: Integer.t()
            ) :: :ok

  @callback get_subscriber(
              tenant :: String.t(),
              subscriber_key :: String.t()
            ) :: {
              {:ok, subscriber_state :: term()} | {:error, term()}
            }

  @callback lock_subscriber(
              tenant :: String.t(),
              subscriber_key :: String.t()
            ) :: {
              {:ok, subscriber_state :: term()} | {:error, term()}
            }

  @callback unlock_subscriber(
              tenant :: String.t(),
              subscriber_key :: String.t(),
              new_acc :: term(),
              inc_offset :: Integer.t()
            ) :: {
              {:ok, subscriber_state :: term()} | {:error, term()}
            }

  @callback update_subscriber(
              tenant :: String.t(),
              subscriber_key :: String.t(),
              new_acc :: term(),
              inc_offset :: Integer.t()
            ) :: {
              {:ok, subscriber_state :: term()} | {:error, term()}
            }
end
