defmodule Bani.ConnectionManagerTest do
  use BaniTest.Case

  import Mox

  setup [:set_mox_global, :verify_on_exit!]

  @host "localhost"
  @port 5552
  @username "guest"
  @password "guest"
  @vhost "/test"
  @broker Bani.MockBroker
  @connection_id "some-connection-id"

  @valid_opts [
    {:host, @host},
    {:port, @port},
    {:username, @username},
    {:password, @password},
    {:vhost, @vhost},
    {:broker, @broker},
    {:connection_id, @connection_id}
  ]

  setup_all do
    valid_opts = Keyword.put(@valid_opts, :supervisor, self())

    {:ok, %{valid_opts: valid_opts}}
  end

  test "initializes", %{valid_opts: valid_opts} do
    test_pid = self()
    ref = make_ref()

    expect(Bani.MockBroker, :connect, fn (host_, port_, username_, password_, vhost_) ->
      assert host_ == @host
      assert port_ == @port
      assert username_ == @username
      assert password_ == @password
      assert vhost_ == @vhost

      Process.send(test_pid, {:expect_called, ref}, [])

      {:ok, self()}
    end)

    start_supervised!({Bani.ConnectionManager, valid_opts})

    assert_receive {:expect_called, ^ref}
  end

  test "cleans up on exit", %{valid_opts: valid_opts} do
    test_pid = self()
    ref = make_ref()

    conn = self()

    stub(Bani.MockBroker, :connect, fn (_, _, _, _, _) -> {:ok, conn} end)

    expect(Bani.MockBroker, :disconnect, fn (conn_) ->
      assert conn_ == conn

      Process.send(test_pid, {:expect_called, ref}, [])

      :ok
    end)

    {:ok, pid} = start_supervised({Bani.ConnectionManager, valid_opts})
    :ok = GenServer.stop(pid)

    assert_receive {:expect_called, ^ref}
  end

  test "returns conn", %{valid_opts: valid_opts} do
    conn = self()

    stub(Bani.MockBroker, :connect, fn (_, _, _, _, _) -> {:ok, conn} end)
    start_supervised!({Bani.ConnectionManager, valid_opts})

    assert conn == Bani.ConnectionManager.conn(@connection_id)
  end
end
