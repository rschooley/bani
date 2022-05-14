defmodule Bani.MessageProcessorBehaviour do
  @callback process(
              parser_fn :: function(),
              handler_fn :: function(),
              chunk :: term(),
              acc :: term()
            ) :: {:ok | :error | :partial_error, result :: term(), success_count :: integer()}
end
