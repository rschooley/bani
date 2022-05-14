defmodule Bani.SubscriberStrategyBehaviour do
  @callback perform(
              strategy :: String.t(),
              tenant :: String.t(),
              subscriber_key :: String.t(),
              store :: term(),
              process_fn :: function()
            ) :: {:ok, term()} | {:error, term()}
end
