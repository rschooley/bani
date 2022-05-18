defmodule Bani.Store.SubscriberState do
  # change this change subscriber store table_attrs
  defstruct [:subscriber_key, :acc, :offset, :locked]
end
