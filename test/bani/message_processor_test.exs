defmodule Bani.MessageProcessorTest do
  use BaniTest.Case

  import ExUnit.CaptureLog
  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  test "handles success" do
    result = "some result"

    parser_fn = fn (_) -> {:ok, {["some message"], nil}} end
    processing_fn = fn (_, _) -> {:ok, result} end
    chunk = "some chunk"
    acc = nil

    assert {:ok, result} == Bani.MessageProcessor.process(parser_fn, processing_fn, chunk, acc)
  end

  test "handles parser error" do
    parser_fn = fn (_) -> {:error} end
    processing_fn = fn (_, _) -> {:ok, "some result"} end
    chunk = "some chunk"
    acc = nil

    assert_raise MatchError, fn ->
      Bani.MessageProcessor.process(parser_fn, processing_fn, chunk, acc)
    end
  end

  test "handles processing error" do
    parser_fn = fn (_) -> {:ok, {["some message"], nil}} end
    processing_fn = fn (_, _) -> {:error, "some error"} end
    chunk = "some chunk"
    acc = nil

    assert_raise MatchError, fn ->
      Bani.MessageProcessor.process(parser_fn, processing_fn, chunk, acc)
    end
  end
end
