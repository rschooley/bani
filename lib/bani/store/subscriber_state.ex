defmodule Bani.Store.SubscriberState do
  defstruct [:subscriber_key, :acc, :offset, :locked]
end
