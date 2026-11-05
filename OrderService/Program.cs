using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using OrderService;
using ZiggyCreatures.Caching.Fusion;

var builder = WebApplication.CreateBuilder(args);
builder.AddShardedService();
builder.AddMongoDBClient("orders");
builder.Services.AddSingleton<OrdersApi>(); // singleton so gRPC and login hydration share it

var app = builder.Build();
app.MapGrpcService<OrdersApi>().RequireAuthorization();
var api = app.Services.GetRequiredService<OrdersApi>();

await app.HydrateOnLogin(api.Load);
await app.EvictOnChangeFeed("order", "orders", "UserId", api.Evict);

app.Run();

record OrderDoc(string Id, string UserId, string Product, int Quantity, [property: BsonElement(Demo.WrittenBy)] string WrittenBy = "");

class OrdersApi(IFusionCache cache, HomeDatabase dbs, Timeline timeline) : Orders.OrdersBase
{
    static string Key(string userId) => $"orders:{userId}";
    IMongoCollection<OrderDoc> OrdersIn(string home) => dbs[home].GetCollection<OrderDoc>("orders");

    public async Task<List<OrderDoc>> Load(string userId, string home) =>
        await cache.GetOrSetAsync(Key(userId), async ct =>
        {
            var list = await OrdersIn(home).Find(o => o.UserId == userId).ToListAsync(ct);
            timeline.Loaded(Key(userId), home);
            return list;
        });

    public override async Task<OrderList> Get(Empty request, ServerCallContext context) =>
        ToProto(await Load(context.UserId(), context.HomeRegion()));

    public override async Task<OrderList> Add(AddOrderRequest request, ServerCallContext context)
    {
        var (userId, home) = (context.UserId(), context.HomeRegion());
        timeline.Writing($"a new order for {userId}", home);
        await OrdersIn(home).InsertOneAsync(new OrderDoc(Guid.NewGuid().ToString(), userId, request.Product, request.Quantity, dbs.Region));
        var list = await OrdersIn(home).Find(o => o.UserId == userId).ToListAsync();
        await cache.SetAsync(Key(userId), list); // writes L1 + L2, backplane makes other nodes holding a copy refresh it from L2
        return ToProto(list);
    }

    // Another region changed this user's data: drop the cached copy from L1 + L2, the backplane drops every other one.
    public Task Evict(string userId) => cache.RemoveAsync(Key(userId)).AsTask();

    static OrderList ToProto(List<OrderDoc> docs) =>
        new() { Orders = { docs.Select(d => new Order { Id = d.Id, Product = d.Product, Quantity = d.Quantity }) } };
}
