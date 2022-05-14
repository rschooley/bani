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
    with {:ok, %SubscriberState{acc: acc}} <- store.lock_subscriber(tenant, subscriber_key),
         {process_status, new_acc, inc_count} <- process_fn.(acc),
         {unlock_status, unlock_response} <- store.unlock_subscriber(tenant, subscriber_key, new_acc, inc_count)
    do
      cond do
        process_status == :ok && unlock_status == :ok ->
          {:ok, unlock_response}

        unlock_status == :error ->
          message = "Bani: could not unlock exactly_once subscriber #{subscriber_key}"
          Logger.error(Exception.format(:error, unlock_response))
          Logger.error(message)

          {:error, message}

        process_status == :partial_error ->
          # process_fn has already logged the error
          {:error, "Bani: process fn had partial error in exactly_once subscriber #{subscriber_key}"}
      end
    else
      err -> err
    end
  end

  def perform(:at_least_once, tenant, subscriber_key, store, process_fn) do
    with {:ok, %SubscriberState{acc: acc}} <- store.get_subscriber(tenant, subscriber_key),
         {process_status, new_acc, inc_count} <- process_fn.(acc),
         {update_status, update_response} <- store.update_subscriber(tenant, subscriber_key, new_acc, inc_count)
    do
      cond do
        process_status == :ok && update_status == :ok ->
          {:ok, update_response}

        update_status == :error ->
          message = "Bani: could not update at_least_once subscriber #{subscriber_key}"
          Logger.error(Exception.format(:error, update_response))
          Logger.error(message)

          {:error, message}

        process_status == :partial_error ->
          # process_fn has already logged the error
          {:error, "Bani: process fn had partial error in at_least_once subscriber #{subscriber_key}"}
      end
    end
  end
end
