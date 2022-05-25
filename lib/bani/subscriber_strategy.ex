defmodule Bani.SubscriberStrategy do
  require Logger

  @behaviour Bani.SubscriberStrategyBehaviour

  alias Bani.Store.SubscriberState

  @doc """
  use two phase commit to store to ensure subscriber can continue processing
    or signal that an error has happened and crash the process in a recoverable initial
    and persisted state

  Happy case:
    1) first phase write to store succeeds => processing: true
    2) handle message(s) succeeds => caller code performs external action (db write, rabbitmq stream, etc)
    3) second phase write to store succeeds => processing: false, offset: N + success message count

  intial state
    store => processing: false, offset: N

  A) #1 fails

    1) first phase write to local storage fails
    store not updated
    2) handler not called
    3) second phase write not called

    store remains => processing: false, offset: N

    state is consistent and good for restart
    on restart the subscriber tries N again

  B) #1 succeeds, #2 fails, #3 succeeds

    1) first phase write succeeds
      store => processing: true
    2) handler fails
      message processing in caller app failed, no external state should be changed
    3) second phase write succeeds, does not change offset
      store => processing: false
      use Logger.info so caller can pair with the message processing error in caller code

    store => processing: false, offset: N

    state is consistent and good for restart
    on restart the subscriber tries N again

  C) #1 succeeds, #2 fails OR succeeds, #3 fails

    unhappiest path

    1) first phase write succeeds
      store => processing: true
    2) handler fails or succeeds
    3) second phase write fails
      use Logger.error to inform caller that manual reconciliation is required

    store => processing: true, offset: N

    bad state
    don't know if #2 passed or failed
    since the second phase write didn't update the store

    on restart the subscriber will not subscribe to the stream
    and will remain in a poisoned state until acted upon by the calling system

    consuming application devs need to manually correct state
    if #2 succeeded, call set_offset with the correct offset value (0 based)
    if #2 failed, call set_processing with false
    then subscribe to the stream
  """
  @impl Bani.SubscriberStrategyBehaviour
  def perform(:exactly_once, tenant, subscriber_key, store, process_fn) do
    store.lock_subscriber(tenant, subscriber_key)
    |> call_process_fn(process_fn, tenant, subscriber_key)
    |> unlock_store(tenant, subscriber_key, store)
    |> log_error(tenant, subscriber_key)
  end

  # def perform(:at_least_once, tenant, subscriber_key, store, process_fn) do
  #   store.get_subscriber(tenant, subscriber_key)
  #   |> call_process_fn(process_fn, tenant, subscriber_key)
  #   |> update_store(tenant, subscriber_key, store)
  #   |> log_error(tenant, subscriber_key)
  # end

  defp call_process_fn({:ok, %SubscriberState{acc: acc}}, process_fn, _tenant, _subscriber_key) do
    process_fn.(acc)
  end

  defp call_process_fn({:error, error}, _process_fn, tenant, subscriber_key) do
    {:error,
     %Bani.SubscriberDeliverError{
       reason: :store_lock_failed,
       inner_error: error,
       subscriber_key: subscriber_key,
       tenant: tenant
     }}
  end

  defp unlock_store({:ok, new_acc, inc_count}, tenant, subscriber_key, store) do
    store.unlock_subscriber(tenant, subscriber_key, new_acc, inc_count)
  end

  defp unlock_store({:partial_error, error, new_acc, inc_count}, tenant, subscriber_key, store) do
    # update and don't unlock
    #  new acc and inc count are last successful values
    new_error = %Bani.SubscriberDeliverError{
      subscriber_key: subscriber_key,
      tenant: tenant,
      extra: %{
        new_acc: new_acc,
        inc_count: inc_count
      }
    }

    case store.update_subscriber(tenant, subscriber_key, new_acc, inc_count) do
      {:ok, _} ->
        {:error, %{new_error | reason: :handler_partial_error, inner_error: error}}

      {:error, update_error} ->
        {:error, %{new_error | reason: :store_update_failed_after_partial_handler_failed, inner_error: update_error}}
    end
  end

  defp unlock_store({:error, %Bani.SubscriberDeliverError{} = error}, _, _, _) do
    {:error, error}
  end

  defp log_error({:ok, result}, _, _) do
    {:ok, result}
  end

  defp log_error({:error, %Bani.SubscriberDeliverError{} = error}, _, _) do
    Logger.error(Exception.message(error))

    {:error, error}
  end

  defp log_error({:error, error}, tenant, subscriber_key) do
    new_error = %Bani.SubscriberDeliverError{
      reason: :store_unlock_failed,
      inner_error: error,
      subscriber_key: subscriber_key,
      tenant: tenant
    }

    log_error({:error, new_error}, tenant, subscriber_key)
  end
end
