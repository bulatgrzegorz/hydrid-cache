using System.Collections.Concurrent;
using AccountService;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Diagnostics;
using OrderService;
using StackExchange.Redis;

// Backend for the demo page: browsers can't speak gRPC, so this forwards each call to the shard the page picked.
var builder = WebApplication.CreateBuilder(args);
builder.AddTelemetry();
builder.AddRedisClient("redis");

var app = builder.Build();
// A failed gRPC call becomes { error: "Unauthenticated" } for the page instead of a bare 500.
app.UseExceptionHandler(e => e.Run(ctx => ctx.Response.WriteAsJsonAsync(new
{
    error = ctx.Features.Get<IExceptionHandlerFeature>()?.Error is RpcException rpc ? rpc.StatusCode.ToString() : "Error",
})));
app.UseDefaultFiles();
app.UseStaticFiles();

var auth = new HttpClient { BaseAddress = new(app.Configuration["services:auth:http:0"]!) };
var channels = new ConcurrentDictionary<string, GrpcChannel>();
GrpcChannel Shard(string service, int shard) =>
    channels.GetOrAdd(app.Configuration[$"services:{service}-{shard}:http:0"]!, GrpcChannel.ForAddress);
Metadata Bearer(HttpRequest r) => new() { { "Authorization", r.Headers.Authorization.ToString() } };

// The page draws one box per shard instance; Aspire gave us a services:account-N entry for each.
app.MapGet("/api/shards", () => app.Configuration.GetSection("services").GetChildren().Count(s => s.Key.StartsWith("account-")));

app.MapPost("/api/login/{userId}", async (string userId) =>
    Results.Text(await (await auth.PostAsync($"login/{userId}", null)).Content.ReadAsStringAsync(), "application/json"));

app.MapGet("/api/account", (int shard, HttpRequest r) =>
    new Accounts.AccountsClient(Shard("account", shard)).GetAsync(new Empty(), Bearer(r)).ResponseAsync);
app.MapPost("/api/account", (int shard, UpdateAccountRequest body, HttpRequest r) =>
    new Accounts.AccountsClient(Shard("account", shard)).UpdateAsync(body, Bearer(r)).ResponseAsync);
app.MapGet("/api/order", (int shard, HttpRequest r) =>
    new Orders.OrdersClient(Shard("order", shard)).GetAsync(new Empty(), Bearer(r)).ResponseAsync);
app.MapPost("/api/order", (int shard, AddOrderRequest body, HttpRequest r) =>
    new Orders.OrdersClient(Shard("order", shard)).AddAsync(body, Bearer(r)).ResponseAsync);

// Every service narrates to the timeline channel; stream it to the page as server-sent events.
app.MapGet("/api/timeline", async (IConnectionMultiplexer redis, CancellationToken ct) =>
{
    var queue = await redis.GetSubscriber().SubscribeAsync(Timeline.Channel);
    ct.Register(() => queue.Unsubscribe());
    return TypedResults.ServerSentEvents(queue.Select(m => m.Message.ToString()));
});

app.Run();
