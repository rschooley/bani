defmodule Bani.MessageProcessorTest do
  use BaniTest.Case

  import Mox
  import ExUnit.CaptureLog

  setup [:set_mox_global, :verify_on_exit!]

  test "handles single message" do
    messages = ["some message"]

    parser_fn = fn _ -> {:ok, {messages, nil}} end
    handler_fn = fn acc_, _ -> {:ok, %{value: acc_.value + 1}} end

    assert {:ok, %{value: 1}, 1} =
             Bani.MessageProcessor.process(parser_fn, handler_fn, "some chunk", %{value: 0})
  end

  test "handles multiple messages" do
    messages = ["some message", "some message 1"]

    parser_fn = fn _ -> {:ok, {messages, nil}} end
    handler_fn = fn acc_, _ -> {:ok, %{value: acc_.value + 1}} end

    assert {:ok, %{value: 2}, 2} =
             Bani.MessageProcessor.process(parser_fn, handler_fn, "some chunk", %{value: 0})
  end

  test "handles error in handler fn" do
    messages = ["some message", "some message 1"]

    parser_fn = fn _ -> {:ok, {messages, nil}} end

    # error after first message is processed successfully
    handler_fn = fn acc_, _ ->
      if acc_.value == 1 do
        raise "random error"
      else
        {:ok, %{value: acc_.value + 1}}
      end
    end

    assert capture_log(fn ->
             assert {:partial_error, _err, %{value: 1}, 1} =
                      Bani.MessageProcessor.process(parser_fn, handler_fn, "some chunk", %{
                        value: 0
                      })
           end) =~ "random error"
  end
end
