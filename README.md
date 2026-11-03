# Sharded hybrid cache

A live, animated demo of a sharded two-level cache in .NET. Each service runs as several shard instances and each instance uses
[FusionCache](https://github.com/ZiggyCreatures/FusionCache) with in-process memory (L1), Redis (L2) and a Redis backplane.
Every user belongs to one shard, which warms its cache the moment that user logs in.

Each service narrates what it does over Redis pub/sub. The demo page plays the narration back step by step: a dot travels each hop, colored by what happened (hit, miss, ignored, rejected).

![The demo page drawing itself on load](docs/intro.gif)

## Walkthrough

These are real recordings of the demo page's animation running against the app at 1× speed, with the controls and log left out.
The app starts with 2 shards, so 4 service instances take part:
account-0, account-1, order-0 and order-1. (account-2 and order-2 also run, on standby until you [scale out](#scaling-out-modulo-ring-or-hrw).)
The user is `ada`, and with the default modulo routing `md5("ada") % 2` makes **shard 0 her owner**.

### 1. No token, no entry

![A call without a JWT is rejected](docs/rejected.gif)

Before logging in, **Get account** goes to account-0 without a JWT. The gRPC service rejects it with `Unauthenticated` (red),
so the request never reaches the cache.

### 2. Logging in warms the owner's cache

![Logging in as ada: JWT, RabbitMQ fanout, and the owner shard warming its cache](docs/login.gif)

1. AuthService issues a JWT and publishes `UserLoggedIn` to a RabbitMQ fanout exchange.
   The page gets its token right away. The rest happens in the background.
2. RabbitMQ delivers the message to all four instances, and each one checks who owns `ada`.
   account-0 and order-0 own her, so they start warming their caches. account-1 and order-1 ignore the message (grey).
3. The owners have never seen `ada`, so they miss in L1 (memory) and in L2 (Redis). They load her account and orders from MongoDB and save them to L1 + L2.
4. Saving also sends a backplane message. The other instances hear it, but they hold no copy, so there's nothing to refresh.

### 3. Owner shard: an L1 hit

![Get account on the owner shard is answered from memory](docs/hit.gif)

**Get account** goes to account-0, the owner. Login already put `ada` into its memory, so this is an L1 hit, answered
without a single network hop. This is the path that sharding by user is built for.

### 4. Wrong shard: the cold path

![Get account on a shard that doesn't own the user goes to Redis](docs/wrong-shard.gif)

The same call goes to account-1 this time. The orange request means it's not the owner. Its memory is empty, so L1 misses.
Redis has the copy the owner saved at login, so L2 hits and the value is copied into account-1's memory.
There's no database trip, but there is a network hop that the owner would have skipped.

### 5. An update every instance hears

![Update account: MongoDB, L1 + L2, then the backplane refreshes the stale copy on the other instance](docs/update.gif)

**Update account** runs on the owner, account-0:

1. It writes MongoDB first, the source of truth, and then L1 + L2.
2. It publishes a backplane message saying `account:ada` changed.
3. account-1 hears it. The wrong-shard read in step 4 left it holding a copy in L1, which is now stale, so it refreshes that copy from Redis.

No instance serves the old value, and no instance had to go back to the database for the new one.

### Scaling out: modulo, ring or HRW

The **Cluster** panel sets how many shards are in (the rest wait on standby) and how users are routed to them.
Every service reads both from Redis on each login, so a change applies right away. Adding a third shard to 3,000 users:

| Routing | Users that get a new owner | Users per shard afterwards |
|---------|----------------------------|----------------------------|
| **Modulo**, `hash % shards` | 66%: almost every remainder changes | 973 / 1002 / 1025 |
| **Ring** (consistent hashing, one point per shard) | 23%, all of them to the new shard | 1584 / 717 / 699 |
| **HRW** (rendezvous: every shard scores the user, the highest wins) | 32%, all of them to the new shard | 1028 / 997 / 975 |

Ring and HRW both move only what the new shard takes over. The ring's points aren't hashed here: each new shard's point
halves the biggest slice, the way Cassandra assigned tokens by hand, so points never crowd together on the page.
Even so, a new shard relieves only the one neighbor it splits, which is why real rings place many virtual points per shard.
HRW is even without them.

A user who moves starts cold on their new owner: the first call misses L1, and Redis (L2) answers it.

The **Hashing** tab under the diagram explains all three side by side, with the users who logged in:
a clock of hash remainders for modulo, the ring cut into one slice per shard for consistent hashing,
and each user's ranking of the shards for HRW.
Each change animates who moves. Each card's footer then compares the share of all users that got a new owner
with the least any strategy must move to keep the shards even: a third, going from 2 to 3 shards.
A handful of logged-in users is too few for those rates, so the cards measure them, and the load bars, on 1,000 test users.
The trace log is one tab over.

Each recording below starts the same way: 2 shards, six users logged in, and `ada`'s data warm in her owner's L1.
Then a third shard joins, and `ada` makes her next call.

#### Modulo: most users move

![Scaling out under modulo: the clock repaints, 4 of 6 users move, and ada's next call misses L1 on her new owner](docs/scale-modulo.gif)

With 3 shards, almost every hour on the clock changes color: 4 of 6 users get a new owner (64% of all users, where 33% would keep the shards even).
`ada` is one of them. Her call lands on account-1, whose L1 has never seen her, and Redis answers it.

#### Consistent hashing: only the new shard's arc moves

![Scaling out under consistent hashing: shard 2's slice sweeps in, 2 of 6 users move, and ada's next call is an L1 hit](docs/scale-ring.gif)

Shard 2's point halves shard 1's slice and takes the first half: 2 of 6 users move (25% of all users: less than the 33% an even split needs).
`ada` stays on account-1, so her call is still an L1 hit. The load bar shows the catch: shard 0 still holds half the keys, because only shard 1 got relief.

#### Rendezvous (HRW): few move, and the load stays even

![Scaling out under HRW: shard 2 slots into every user's ranking, 2 of 6 users move, and ada's next call is an L1 hit](docs/scale-hrw.gif)

Shard 2 slots into every user's ranking, but takes only the users where it lands first: 2 of 6 (34% of all users, just the 33% an even split needs).
`ada` stays on account-1 and hits L1, and every shard keeps a third of the load.

## Architecture

![Architecture: Web, AuthService and RabbitMQ on top, four shard instances with their own L1 in the middle, Redis and MongoDB below](docs/architecture.png)

- **Web** serves the demo page and forwards its calls. It uses HTTP to reach AuthService and gRPC to reach the shard instances, because browsers can't call gRPC.
- **AuthService** issues JWTs and publishes `UserLoggedIn` to a RabbitMQ fanout exchange, which delivers it to every instance.
- **account-N** and **order-N** each keep an L1 cache in their own memory. They share Redis for L2 and the backplane, and fall back to MongoDB.

[.NET Aspire](https://aspire.dev) runs everything. It starts the services and the Redis, RabbitMQ and MongoDB containers, and it collects traces, metrics and logs in its dashboard.

## Run it

You need the [.NET 10 SDK](https://dotnet.microsoft.com/download) and Docker or Podman.

```sh
dotnet run --project AppHost
```

- Demo page: http://localhost:5050
- Aspire dashboard: http://localhost:15000 (the login link is printed in the console)

With Podman, first run `export ASPIRE_CONTAINER_RUNTIME=podman`.
`shards` in [`AppHost/AppHost.cs`](AppHost/AppHost.cs) sets how many instances each service runs. The page's **Cluster** panel picks how many of them are in.

## Using the page

1. **Log in** with any name. The owner shard is highlighted in the diagram.
2. **Pick an action**, such as Get account or Add order. It opens with its fields and a shard picker, which defaults to the owner. Press **Run**.
3. Watch the story. Use **Speed** to slow it down, or turn on **Step by step** and advance with the button, `→` or a presenter clicker (`PageDown`).
4. Turn on **Show TTL** to see how long each cached copy has left. Entries live a minute in L1 and two minutes in Redis, and when one runs out of L1 the page plays its eviction.
5. Use **Cluster** to add or remove a shard, or to switch routing between Modulo, Ring and HRW, and watch the **Hashing** tab. Log in a few users first so there's someone to move.

## Project layout

| Path | What's in it |
|------|--------------|
| `AppHost/` | Aspire orchestration: containers, services, instances per service, ports |
| `AuthService/` | Login: issues the JWT, publishes `UserLoggedIn` |
| `AccountService/`, `OrderService/` | Sharded gRPC services behind JWT, reading through FusionCache |
| `Shared/Demo.cs` | What the services share: JWT setup, FusionCache config, cache warming on login |
| `Shared/Cluster.cs` | Routing: the shard count and strategy kept in Redis, and the modulo, ring and HRW owner lookups |
| `Shared/Timeline.cs` | The narration: turns FusionCache events into steps for the page |
| `Web/` | Backend for the page (browsers can't call gRPC) and `wwwroot/index.html`. GSAP is vendored so it works offline |

## Demo shortcuts

This is a conference demo, not production code:

- There are no passwords: any name gets a token. The JWT signing key is hardcoded in `Shared/Demo.cs`.
- Each instance listens on a throwaway RabbitMQ queue, so logins that happen while a shard is down are lost.
