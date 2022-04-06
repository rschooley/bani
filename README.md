# Bani

RabbitMQ Stream processing in Elixir

## Rules
- RabbitMQ streams support multiple (TCP) connections
- each connection supports 256 (0-255) publishing ids
- each connection supports 256 (0-255) subscription ids
- each tenant has their own RabbitMQ server

## Please note
This libabry is in active development and is not suited for production use at this time.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `bani` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bani, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/bani>.


## Use

```
  defmodule MyBani do
    use Bani
  end
```

```
  conn_opts = [
    {:host, "localhost"},
    {:port, 5552},
    {:username, "guest"},
    {:password, "guest"},
    {:vhost, "/dev"}
  ]

  tenant = "tenant-123"
  stream_name = "item-abc"
  
  # use inline handler, Ecto repo function, etc
  handler = fn (_prev, curr) -> 
    {:ok, result} = MyContext.insert(curr)
  
    # return acc
    {:ok, result}
  end)

  MyBani.add_tenant(tenant, conn_opts)
  MyBani.create_stream(tenant, stream_name)
  MyBani.create_publisher(tenant, stream_name)
  
  # the last arg is the initial acc value
  MyBani.create_subscriber(tenant, stream_name, "database-skink", handler, %{})

  MyBani.publish(tenant, stream_name, "a message")
```

## Running tests
The libarary uses a docker image of RabbitMQ with streams enabled.

### build rabbitmq with streaming enabled
`docker build -t docker-rabbitmq-streams .`

### run containers
`docker compose up`

### Create virtual host with guest user permissions
```bash
docker exec rabbitmq rabbitmqctl add_vhost /test
docker exec rabbitmq rabbitmqctl set_permissions -p /test guest ".*" ".*" ".*"
```

### run tests
`mix test`
