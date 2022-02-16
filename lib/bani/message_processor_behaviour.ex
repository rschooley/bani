defmodule Bani.MessageProcessorBehaviour do
  @callback process(
    parser_fn :: function(),
    processing_fn :: function(),
    chunk :: term(),
    acc :: term()
  ) :: {:ok, term()}
end
