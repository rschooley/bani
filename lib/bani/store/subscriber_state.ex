defmodule Bani.Store.SubscriberState do
  @doc """
    * `:acc`- the accumulator for the subscriber
    * `:connection_id`- The id of the connection.  Used to lookup through connection_manager module.
    * `:handler`- The function passed by the calling lib
    * `:locked`- the locked state for the subscriber.  Used by the exactly_once strategy.
    * `:offset`- the offset for the subscriber
    * `:strategy`- The strategy for message processor in the subscriber; exactly_once or at_least_once.
    * `:stream_name`- The name of the stream in rabbit
    * `:subscriber_key`- the storage key / registry name for the subscriber
    * `:subscription_id`- The id of the subscriber in rabbit
    * `:subscription_name`- The name of the subscription (sink_to_pg, main_pipeline, etc)
    * `:tenant`- The id/name of the tenant
  """
  defstruct [
    :acc,
    :locked,
    :offset,
    :subscriber_key,

    # for recreating genserver from store
    :connection_id,
    :handler,
    :strategy,
    :stream_name,
    :subscription_id,
    :subscription_name,
    :tenant
  ]
end
