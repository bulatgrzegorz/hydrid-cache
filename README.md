# Sharded hybrid cache

A live, animated demo of a sharded two-level cache in .NET. Each service runs as several shard instances and each instance uses
[FusionCache](https://github.com/ZiggyCreatures/FusionCache) with in-process memory (L1), Redis (L2) and a Redis backplane.
Every user belongs to one shard, which warms its cache the moment that user logs in.

Each service narrates what it does over Redis pub/sub. The demo page plays the narration back step by step: a dot travels each hop, colored by what happened (hit, miss, ignored, rejected).

## What you can see

- **Login warms the owner's cache.** AuthService issues a JWT and publishes `UserLoggedIn` to a RabbitMQ fanout exchange.
  Every account and order instance receives it, but only the owner (`md5(user) % shards`) loads the user's data into its cache.
- **Reads go L1 → L2 → MongoDB.** A hit in memory needs no network call. A Redis hit is copied into memory. Only a double miss reaches the database.
- **Writes keep every instance honest.** The write goes to MongoDB, then to L1 + L2. The backplane tells the other instances that the key changed, so they drop or refresh their stale L1 copy.
- **Wrong shard.** Call a shard that doesn't own the user to see the cold path.
- **No JWT.** Call before logging in to see the request rejected.

## Architecture

```
            ┌──────── Web :5050 (demo page + backend) ────────┐
            │ HTTP                                    gRPC    │
            ▼                                                 ▼
     AuthService :5000 ──► RabbitMQ ──fanout──►  account-0, account-1  :5100+
       (JWT)          UserLoggedIn               order-0,   order-1    :5200+
                                                 (each with its own L1 memory)
                                                          │
                                       ┌──────────────────┴──────────────┐
                                       ▼                                 ▼
                              Redis: L2 + backplane              MongoDB: source of truth
```

[.NET Aspire](https://aspire.dev) runs everything. It starts the services and the Redis, RabbitMQ and MongoDB containers, and it collects traces, metrics and logs in its dashboard.

## Run it

You need the [.NET 10 SDK](https://dotnet.microsoft.com/download) and Docker or Podman.

```sh
dotnet run --project AppHost
```

- Demo page: http://localhost:5050
- Aspire dashboard: http://localhost:15000 (the login link is printed in the console)

With Podman, first run `export ASPIRE_CONTAINER_RUNTIME=podman`.
To change the number of shards, edit `shards` in [`AppHost/AppHost.cs`](AppHost/AppHost.cs).

## Using the page

1. **Log in** with any name. The owner shard is highlighted in the diagram.
2. **Pick an action**, such as Get account or Add order. It opens with its fields and a shard picker, which defaults to the owner. Press **Run**.
3. Watch the story. Use **Speed** to slow it down, or turn on **Step by step** and advance with the button, `→` or a presenter clicker (`PageDown`).

## Project layout

| Path | What's in it |
|------|--------------|
| `AppHost/` | Aspire orchestration: containers, services, shard count, ports |
| `AuthService/` | Login: issues the JWT, publishes `UserLoggedIn` |
| `AccountService/`, `OrderService/` | Sharded gRPC services behind JWT, reading through FusionCache |
| `Shared/Demo.cs` | What the services share: JWT setup, sharding, FusionCache config, cache warming on login |
| `Shared/Timeline.cs` | The narration: turns FusionCache events into steps for the page |
| `Web/` | Backend for the page (browsers can't call gRPC) and `wwwroot/index.html`. GSAP is vendored so it works offline |

## Demo shortcuts

This is a conference demo, not production code:

- There are no passwords: any name gets a token. The JWT signing key is hardcoded in `Shared/Demo.cs`.
- Each instance listens on a throwaway RabbitMQ queue, so logins that happen while a shard is down are lost.
