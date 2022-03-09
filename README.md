# Bani

RabbitMQ Stream processing in Elixir

## Rules
RabbitMQ streams support multiple (TCP) connections
each connection supports 256 (0-255) publishing ids
each connection supports 256 (0-255) subscription ids

- scheduler
- retain offset

- defrag connections
- Multi tenant support for RabbitMQ servers
- multi application node


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


## Design
Just don't reuse publisher/subscriber ids in connections, never cross the streams

Be able to:
1) remove Publisher/Subscription from connection
2) retain the genservers
3) move them to another connection when publish is called for the stream

research ChunkFirstOffset => uint64
try out crashing and restarting connection and pid ref in child

--> Question 2: if the like conn is restarted what is the state of the publisher / subscription ids?

:lake.connection
  - start_link with caller
  - allows subscription_ids 0-255 & publishing_ids 0-255
  - if this crashes it needs to be restarted
    what happens then?

Application
  Scheduler (agent) - current Publisher/Subscription connection managers
  Supervisor (dynamic)
    ConnectionSupervisor (one_for_all)
      ConnectionManager - conn pid & publisher/subscription ids
      SubscriptionSupervisor (dynamic)
        Subscription
      PublisherSupervisor (dynamic)
        Publisher
  SubscriptionState - independent of Supervisor tree restarts

```elixir
scenario 1
  UI button -> create entity
    Bani.create_stream(tenant_id, stream_id)
      find next connection manager with available publisher id
        nil -> create connection_manager
      connection manager
        create_publisher
    Bani.create_subscriber(tenant_id, 
      find next connection manager with available subscriber id
      create_subscriber
```

```elixir
create_stream(tenant, stream_name)
create_publisher(tenant, stream_name)
  link(conn, tenant, stream_name, publisher_id)
create_subscriber(tenant, stream_name)
publish
delete_publisher(tenant, stream_name)
delete_subscriber

connection supervisor     "#{connection_name}-connection"
connection manager        "connection-manager-#{connection_name}"
publisher                 "#{stream_name}-publisher-#{publisher_id}"
subscriber                "#{stream_name}-subscriber-"

```

examples (tests & readme)
  Device sends status (temp)
  Entity builds state (shopping cart)

Broker.connect() is a TCP connection
Hosting plans like CloudAMQP have max connections per plan
SubscriptionId/PublisherId are limited to 256 (0-255)

when should old Publisher/Subscription ids be freed
- inactivity for time with some watcher?


# id = :crypto.strong_rand_bytes(10) |> Base.url_encode64(padding: false)
    # term = "connection_supervisor_#{id}"