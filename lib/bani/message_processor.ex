defmodule Bani.MessageProcessor do
  require Logger

  @behaviour Bani.MessageProcessorBehaviour

  @impl Bani.MessageProcessorBehaviour
  def process(parser_fn, handler_fn, chunk, initial_acc) do
    {:ok, {messages, _metadata}} = parser_fn.(chunk)

    {new_acc, success_count} =
      Enum.reduce_while(messages, {initial_acc, 0}, fn message, {acc, success_count} ->
        # handler is from the calling application
        #  and could raise all sorts of errors
        #  partial success for batch of multiple messages allowed
        #  stop processing after the first failure
        try do
          # fail hard on anything but ok
          #  and let resuce log the error
          #  and return the latest success count
          {:ok, new_acc} = handler_fn.(acc, message)

          {:cont, {new_acc, success_count + 1}}
        rescue
          err ->
            Logger.error(Exception.format(:error, err, __STACKTRACE__))

            {:halt, {acc, success_count}}
        end
      end)

    cond do
      length(messages) == success_count ->
        {:ok, new_acc, success_count}

      true ->
        {:partial_error, new_acc, success_count}
    end
  end
end
