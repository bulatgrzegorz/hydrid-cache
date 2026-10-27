using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MongoDB.Driver;
using OrderService;
using ZiggyCreatures.Caching.Fusion;

var builder = WebApplication.CreateBuilder(args);
builder.AddShardedService();
builder.AddMongoDBClient("orders");
builder.Services.AddSingleton<OrdersApi>(); // singleton so gRPC and login hydration share it

var app = builder.Build();
app.MapGrpcService<OrdersApi>().RequireAuthorization();
await app.HydrateOnLogin(app.Services.GetRequiredService<OrdersApi>().Load);
app.Run();

record OrderDoc(string Id, string UserId, string Product, int Quantity);

class OrdersApi(IFusionCache cache, IMongoDatabase db, Timeline timeline) : Orders.OrdersBase
{
    readonly IMongoCollection<OrderDoc> orders = db.GetCollection<OrderDoc>("orders");

    public async Task<List<OrderDoc>> Load(string userId) =>
        await cache.GetOrSetAsync($"orders:{userId}", ct => orders.Find(o => o.UserId == userId).ToListAsync(ct));

    public override async Task<OrderList> Get(Empty request, ServerCallContext context) =>
        ToProto(await Load(context.UserId()));

    public override async Task<OrderList> Add(AddOrderRequest request, ServerCallContext context)
    {
        var userId = context.UserId();
        await orders.InsertOneAsync(new OrderDoc(Guid.NewGuid().ToString(), userId, request.Product, request.Quantity));
        timeline.Tell("db-write", $"Wrote new order for {userId} to MongoDB", to: "mongo");
        var list = await orders.Find(o => o.UserId == userId).ToListAsync();
        await cache.SetAsync($"orders:{userId}", list); // writes L1 + L2, backplane evicts stale L1 copies on other nodes
        return ToProto(list);
    }

    static OrderList ToProto(List<OrderDoc> docs) =>
        new() { Orders = { docs.Select(d => new Order { Id = d.Id, Product = d.Product, Quantity = d.Quantity }) } };
}
