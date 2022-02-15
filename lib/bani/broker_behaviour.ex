defmodule Bani.BrokerBehaviour do
  @doc """
  Connects to the broker.  The resulting conn process is linked with the caller.

  ## Examples

      iex> Bani.Broker.connect("localhost", 5552, "guest", "guest", "/dev")
      {:ok, conn}

  """
  @callback connect(
              host :: String.t(),
              port :: integer(),
              username :: String.t(),
              password :: String.t(),
              vhost :: String.t()
            ) :: {:ok, pid()} | {:error, term()}

  @doc """
  Disconnects from the broker.

  ## Examples

      iex> Bani.Broker.disconnect(conn)
      :ok

  """
  @callback disconnect(conn :: pid()) :: :ok | {:error, term()}

  @doc """
  Creates a stream.

  ## Examples

      iex> Bani.Broker.create_stream(conn, "stream-123")
      :ok

  """
  @callback create_stream(conn :: pid(), stream_name :: String.t()) :: :ok | {:error, term()}

  @doc """
  Deletes a stream.

  ## Examples

      iex> Bani.Broker.delete_stream(conn, "stream-123")
      :ok

  """
  @callback delete_stream(conn :: pid(), stream_name :: String.t()) :: :ok | {:error, term()}

  @doc """
  Subscribes to a stream.

  ## Examples

      iex> Bani.Broker.subscribe(conn, "stream-123", 1, :first)
      :ok

  """
  @callback subscribe(
              conn :: pid(),
              stream_name :: String.t(),
              subscription_id :: integer(),
              offset :: integer()
            ) :: :ok | {:error, term()}

  @doc """
  Unsubscribes from a stream.

  ## Examples

      iex> Bani.Broker.unsubscribe(conn, 1)
      :ok

  """
  @callback unsubscribe(conn :: pid(), subscription_id :: integer()) :: :ok | {:error, term()}

  @doc """
  Creates a publisher for a stream.

  ## Examples

      iex> Bani.Broker.create_publisher(conn, "stream-123", 10, "some-publisher")
      :ok

  """
  @callback create_publisher(
              conn :: pid(),
              stream_name :: String.t(),
              publisher_id :: integer(),
              publisher_name :: String.t()
            ) :: :ok | {:error, term()}

  @doc """
  Deletes a publisher for a stream.

  ## Examples

      iex> Bani.Broker.delete_publisher(conn, 10)
      :ok

  """
  @callback delete_publisher(conn :: pid(), publisher_id :: integer()) :: :ok | {:error, term()}

  @doc """
  Publishes to a stream synchronously.

  See here for more on publishing_id:
  https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-message-deduplication/

  ## Examples

      iex> Bani.Broker.publish(conn, 10, "message", 1)
      :ok

  """
  @callback publish(
              conn :: pid(),
              publisher_id :: integer(),
              message :: String.t(),
              publishing_id :: integer()
            ) :: :ok

  @callback chunk_to_messages(chunk :: term()) :: :ok | {:error, term()}
end
