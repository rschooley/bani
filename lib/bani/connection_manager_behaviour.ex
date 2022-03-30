defmodule Bani.ConnectionManagerBehaviour do
  @callback conn(
    connection_id :: String.t()
  ) :: pid()
end
