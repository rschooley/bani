defmodule Bani.KeyRing do
  def connection_name(connection_id) do
    "connections/#{connection_id}"
  end

  def connection_manager_name(connection_id) do
    "connections/#{connection_id}/connection_manager"
  end

  def publisher_name(tenant, stream_name) do
    "tenants/#{tenant}/streams/#{stream_name}/publisher"
  end

  def subscriber_agent_name(tenant, stream_name, subscription_name) do
    "tenants/#{tenant}/streams/#{stream_name}/subscribers/#{subscription_name}/agent"
  end

  def subscriber_key(tenant, stream_name, subscription_name) do
    "tenants/#{tenant}/streams/#{stream_name}/subscribers/#{subscription_name}"
  end

  def subscriber_name(tenant, stream_name, subscription_name) do
    subscriber_key(tenant, stream_name, subscription_name)
  end

  def tenant_name(tenant) do
    "tenants/#{tenant}"
  end
end
