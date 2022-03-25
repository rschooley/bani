defmodule Bani.KeyRing do
  def connection_name(tenant, connection_id) do
    "tenants/#{tenant}/connections/#{connection_id}"
  end

  def publisher_name(tenant, stream_name) do
    "tenants/#{tenant}/streams/#{stream_name}/publisher"
  end

  def subscriber_agent_name(tenant, stream_name, subscription_name) do
    "tenants/#{tenant}/streams/#{stream_name}/subscribers/#{subscription_name}/agent"
  end

  def subscriber_name(tenant, stream_name, subscription_name) do
    "tenants/#{tenant}/streams/#{stream_name}/subscribers/#{subscription_name}"
  end
end
