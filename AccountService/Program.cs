using AccountService;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using ZiggyCreatures.Caching.Fusion;

var builder = WebApplication.CreateBuilder(args);
builder.AddShardedService();
builder.AddMongoDBClient("accounts");
builder.Services.AddSingleton<AccountsApi>(); // singleton so gRPC and login hydration share it

var app = builder.Build();
app.MapGrpcService<AccountsApi>().RequireAuthorization();
var api = app.Services.GetRequiredService<AccountsApi>();
await app.HydrateOnLogin(api.Load);
await app.EvictOnChangeFeed("account", "accounts", "_id", api.Evict);
app.Run();

record AccountDoc(string Id, string Name, string Email, [property: BsonElement(Demo.WrittenBy)] string WrittenBy = "");

class AccountsApi(IFusionCache cache, HomeDatabase dbs, Timeline timeline) : Accounts.AccountsBase
{
    static string Key(string userId) => $"account:{userId}";
    IMongoCollection<AccountDoc> AccountsIn(string home) => dbs[home].GetCollection<AccountDoc>("accounts");

    public async Task<AccountDoc> Load(string userId, string home) =>
        await cache.GetOrSetAsync(Key(userId), async ct =>
        {
            var doc = await AccountsIn(home).Find(a => a.Id == userId).FirstOrDefaultAsync(ct) ?? new AccountDoc(userId, "", "");
            timeline.Loaded(Key(userId), home);
            return doc;
        });

    // Another region changed this user's data: drop the cached copy from L1 + L2, the backplane drops every other one.
    public Task Evict(string userId) => cache.RemoveAsync(Key(userId)).AsTask();

    public override async Task<Account> Get(Empty request, ServerCallContext context) =>
        ToProto(await Load(context.UserId(), context.HomeRegion()));

    public override async Task<Account> Update(UpdateAccountRequest request, ServerCallContext context)
    {
        var (doc, home) = (new AccountDoc(context.UserId(), request.Name, request.Email, dbs.Region), context.HomeRegion());
        timeline.Writing($"account {doc.Id}", home);
        await AccountsIn(home).ReplaceOneAsync(a => a.Id == doc.Id, doc, new ReplaceOptions { IsUpsert = true });
        await cache.SetAsync(Key(doc.Id), doc); // writes L1 + L2, backplane makes other nodes holding a copy refresh it from L2
        return ToProto(doc);
    }

    static Account ToProto(AccountDoc d) => new() { Id = d.Id, Name = d.Name, Email = d.Email };
}
