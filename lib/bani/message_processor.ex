defmodule Bani.MessageProcessor do
  @behaviour Bani.MessageProcessorBehaviour

  @impl Bani.MessageProcessorBehaviour
  def process(parser_fn, processing_fn, chunk, acc) do
    {:ok, {[message], _metadata}} = parser_fn.(chunk)
    {:ok, result} = processing_fn.(acc, message)

    {:ok, result}
  end
end
