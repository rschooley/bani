defmodule Bani.SchedulingBehaviour do
  @callback create_stream(
              conn_opts :: keyword(),
              stream_name :: String.t()
            ) :: :ok

  @callback delete_stream(
              conn_opts :: keyword(),
              stream_name :: String.t()
            ) :: :ok

  @callback create_publisher(
              tenant :: String.t(),
              conn_opts :: keyword(),
              stream_name :: String.t()
            ) :: {:ok, pid()} | {:error, term()}

  @callback delete_publisher(
              tenant :: String.t(),
              publisher_id :: integer()
            ) :: :ok | {:error, term()}

  @callback create_subscriber(
              tenant :: String.t(),
              conn_opts :: keyword(),
              stream_name :: String.t(),
              subscription_name :: String.t(),
              handler :: function(),
              offset :: integer(),
              acc :: term(),
              poisoned :: boolean()
            ) :: {:ok, pid()} | {:error, term()}

  @callback delete_subscriber(
              tenant :: String.t(),
              stream_name :: String.t(),
              subscription_name :: String.t()
            ) :: :ok | {:error, term()}
end
