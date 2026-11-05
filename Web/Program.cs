using System.Collections.Concurrent;
using AccountService;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Diagnostics;
using OrderService;
using StackExchange.Redis;

// Backend for the demo page: browsers can't speak gRPC, so this forwards each call to the region and shard the page picked.
// It's the one piece both regions share, like a global front door.
var builder = WebApplication.CreateBuilder(args);
builder.AddTelemetry();
// Aspire gave us a ConnectionStrings:<region>-redis for each region: its cluster, its users and its timeline.
string[] regions = [.. builder.Configuration.PerRegion("redis").Keys];
foreach (var region in regions) builder.AddKeyedRedisClient($"{region}-redis");

var app = builder.Build();
// A failed gRPC call becomes { error: "Unauthenticated" } for the page instead of a bare 500.
app.UseExceptionHandler(e => e.Run(ctx => ctx.Response.WriteAsJsonAsync(new
{
    error = ctx.Features.Get<IExceptionHandlerFeature>()?.Error is RpcException rpc ? rpc.StatusCode.ToString() : "Error",
})));
app.UseDefaultFiles();
app.UseStaticFiles();

// An unknown region finds no Redis, auth or shard here, and fails like any other call.
IDatabase Redis(string region) => app.Services.GetRequiredKeyedService<IConnectionMultiplexer>($"{region}-redis").GetDatabase();
var auth = regions.ToDictionary(r => r, r => new HttpClient { BaseAddress = new(app.Configuration[$"services:{r}-auth:http:0"]!) });
var channels = new ConcurrentDictionary<string, GrpcChannel>();
GrpcChannel Shard(string region, string service, int shard) =>
    channels.GetOrAdd(app.Configuration[$"services:{region}-{service}-{shard}:http:0"]!, GrpcChannel.ForAddress);
Metadata Bearer(HttpRequest r) => new() { { "Authorization", r.Headers.Authorization.ToString() } };

// The page draws one box per shard instance, standby ones too; Aspire gave us a services:<region>-account-N entry for each.
var maxShards = app.Configuration.GetSection("services").GetChildren().Count(s => s.Key.StartsWith($"{regions[0]}-account-"));
app.MapGet("/api/shards", () => maxShards);
app.MapGet("/api/regions", () => regions);
// The page's TTL countdowns, from the same numbers the services cache with.
app.MapGet("/api/ttl", () => new { l1 = Demo.L1Ttl.TotalSeconds, l2 = Demo.L2Ttl.TotalSeconds });

app.MapPost("/api/login/{userId}", async (string userId, string region) =>
    Results.Text(await (await auth[region].PostAsync($"login/{userId}", null)).Content.ReadAsStringAsync(), "application/json"));

// A region's cluster, which the page shows and scales. Changing it is a Redis write; that region's services read it on each login.
app.MapGet("/api/cluster", async (string region) =>
{
    var db = Redis(region);
    return await ClusterView(db, await Cluster.Load(db));
});
app.MapPost("/api/cluster", async (string region, int? shards, string? strategy) =>
{
    if (shards is < 1 || shards > maxShards || strategy is not null && !Cluster.Strategies.Contains(strategy))
        return Results.BadRequest();
    var db = Redis(region);
    var cluster = await Cluster.Load(db);
    cluster = cluster with { Shards = shards ?? cluster.Shards, Strategy = strategy ?? cluster.Strategy };
    await cluster.Save(db);
    return Results.Ok(await ClusterView(db, cluster));
});

app.MapGet("/api/account", (string region, int shard, HttpRequest r) =>
    new Accounts.AccountsClient(Shard(region, "account", shard)).GetAsync(new Empty(), Bearer(r)).ResponseAsync);
app.MapPost("/api/account", (string region, int shard, UpdateAccountRequest body, HttpRequest r) =>
    new Accounts.AccountsClient(Shard(region, "account", shard)).UpdateAsync(body, Bearer(r)).ResponseAsync);
app.MapGet("/api/order", (string region, int shard, HttpRequest r) =>
    new Orders.OrdersClient(Shard(region, "order", shard)).GetAsync(new Empty(), Bearer(r)).ResponseAsync);
app.MapPost("/api/order", (string region, int shard, AddOrderRequest body, HttpRequest r) =>
    new Orders.OrdersClient(Shard(region, "order", shard)).AddAsync(body, Bearer(r)).ResponseAsync);

// Every service narrates to its region's timeline channel; stream one region's to the page as server-sent events.
app.MapGet("/api/timeline", async (string region, CancellationToken ct) =>
{
    var queue = await Redis(region).Multiplexer.GetSubscriber().SubscribeAsync(Timeline.Channel);
    ct.Register(() => queue.Unsubscribe());
    return TypedResults.ServerSentEvents(queue.Select(m => m.Message.ToString()));
});

app.Run();

// Where every user who logged in lands under each strategy, and the hashes behind it (ring points, HRW scores),
// so the page can draw them. A thousand sample keys show what a handful of users can't: how evenly each strategy
// spreads the load, and how much of it moves when the shard count changes.
static async Task<object> ClusterView(IDatabase db, Cluster cluster)
{
    var shards = Enumerable.Range(0, cluster.Shards).ToArray();
    var keys = Enumerable.Range(0, 1000).Select(i => $"key-{i}").ToArray();
    return new
    {
        cluster.Shards,
        cluster.Strategy,
        Ring = shards.Select(Cluster.RingPoint),
        Sample = Cluster.Strategies.ToDictionary(s => s, s => keys.Select((cluster with { Strategy = s }).OwnerOf)),
        Users = (await db.SetMembersAsync(Cluster.Users)).Select(u => u.ToString()).Order().Select(u => new
        {
            Id = u,
            Hash = Cluster.Hash(u),
            Scores = shards.Select(i => Cluster.Score(u, i)),
            Owners = Cluster.Strategies.ToDictionary(s => s, s => (cluster with { Strategy = s }).OwnerOf(u)),
        }),
    };
}
