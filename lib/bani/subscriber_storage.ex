defmodule Bani.SubscriberStorage do
  use Agent

  def start_link(opts) do
    state = %{
      acc: Keyword.fetch!(opts, :acc),
      offset: Keyword.fetch!(opts, :offset),
      poisoned: Keyword.fetch!(opts, :poisoned),
      poisoned_err: nil,
      stream_name: Keyword.fetch!(opts, :stream_name),
      subscription_name: Keyword.fetch!(opts, :subscription_name),
      tenant: Keyword.fetch!(opts, :tenant)
    }

    Agent.start_link(fn -> state end, name: via_tuple(state.tenant, state.stream_name, state.subscription_name))
  end

  defp via_tuple(tenant, stream_name, subscription_name) do
    name = Bani.KeyRing.subscriber_agent_name(tenant, stream_name, subscription_name)

    {:via, Registry, {Bani.Registry, name}}
  end

  def acc(tenant, stream_name, subscription_name) do
    name = via_tuple(tenant, stream_name, subscription_name)

    Agent.get(name, fn (state) -> state.acc end)
  end

  def offset(tenant, stream_name, subscription_name) do
    name = via_tuple(tenant, stream_name, subscription_name)

    Agent.get(name, fn (state) -> state.offset end)
  end

  def values(tenant, stream_name, subscription_name) do
    name = via_tuple(tenant, stream_name, subscription_name)

    Agent.get(name, & &1)
  end

  def update(tenant, stream_name, subscription_name, count, acc) do
    name = via_tuple(tenant, stream_name, subscription_name)

    Agent.update(name, fn (state) ->
      %{state | acc: acc, offset: state.offset + count}
    end)
  end

  def poison(tenant, stream_name, subscription_name, err) do
    name = via_tuple(tenant, stream_name, subscription_name)

    Agent.update(name, fn (state) ->
      %{state | poisoned: true, poisoned_err: err}
    end)
  end
end