defmodule Bani.SubscriberDeliverError do
  defexception [:inner_error, :reason, :subscriber_key, :tenant, :extra]

  def message(%{reason: :store_lock_failed} = opts) do
    """
    The following exception happened when locking the pubsub store for
    tenant_#{opts.tenant} subscriber_#{opts.subscriber_key}.

        #{format_inner_error(opts.inner_error)}

    """
  end

  def message(%{reason: :store_unlock_failed} = opts) do
    """
    The following exception happened when unlocking the pubsub store for
    tenant_#{opts.tenant} subscriber_#{opts.subscriber_key}.

        #{format_inner_error(opts.inner_error)}

    """
  end

  def message(%{reason: :handler_partial_error} = opts) do
    """
    The following exception happened when calling the subscriber handler for
    tenant_#{opts.tenant} subscriber_#{opts.subscriber_key}.

    The store has been updated with the latest successful handler values.
    The subscriber can be restarted once the handler code has been fixed.

        #{format_inner_error(opts.inner_error)}

    """
  end

  def message(%{reason: :store_update_failed_after_partial_handler_failed} = opts) do
    """
    The following exception happened when updating the pubsub store after a partial
    subscriber handler error tenant_#{opts.tenant} subscriber_#{opts.subscriber_key}.

    The store has not been updated with the latest successful handler values.
    The subscriber can only be restarted once the store is manually reconciled.

    The new acc for the value in the store (unchanged) is #{inspect(opts.extra.new_acc)}.
    The new offset is the value in the store (unchanged) + #{opts.extra.inc_count}.

    A full stacktrace of the handler error has been logged just prior to this message.

    The store update error is:

        #{format_inner_error(opts.inner_error)}

    """
  end

  defp format_inner_error(inner_error) do
    Exception.format(:error, inner_error, []) |> String.replace("\n", "\n    ")
  end
end
