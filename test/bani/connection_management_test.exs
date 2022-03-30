defmodule Bani.ConnectionManagementTest do
  use BaniTest.Case

  @host "localhost"
  @port 5552
  @username "guest"
  @password "guest"
  @vhost "/test"

  @valid_opts [
    {:host, @host},
    {:port, @port},
    {:username, @username},
    {:password, @password},
    {:vhost, @vhost}
  ]

  test "available_connection_manager" do
    tenant_key = "key"

    {ok, pid} = Bani.ConnectionSupervisor.start_link(@valid_opts)

    Bani.ConnectionSupervisor.foo(pid)

  end
end
