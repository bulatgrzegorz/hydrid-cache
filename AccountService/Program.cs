using AccountService;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MongoDB.Driver;
using ZiggyCreatures.Caching.Fusion;

var builder = WebApplication.CreateBuilder(args);
builder.AddShardedService();
builder.AddMongoDBClient("accounts");
builder.Services.AddSingleton<AccountsApi>(); // singleton so gRPC and login hydration share it

var app = builder.Build();
app.MapGrpcService<AccountsApi>().RequireAuthorization();
await app.HydrateOnLogin(app.Services.GetRequiredService<AccountsApi>().Load);
app.Run();

record AccountDoc(string Id, string Name, string Email);

class AccountsApi(IFusionCache cache, IMongoDatabase db, Timeline timeline) : Accounts.AccountsBase
{
    readonly IMongoCollection<AccountDoc> accounts = db.GetCollection<AccountDoc>("accounts");

    public async Task<AccountDoc> Load(string userId) =>
        await cache.GetOrSetAsync($"account:{userId}", async ct =>
            await accounts.Find(a => a.Id == userId).FirstOrDefaultAsync(ct) ?? new AccountDoc(userId, "", ""));

    public override async Task<Account> Get(Empty request, ServerCallContext context) =>
        ToProto(await Load(context.UserId()));

    public override async Task<Account> Update(UpdateAccountRequest request, ServerCallContext context)
    {
        var doc = new AccountDoc(context.UserId(), request.Name, request.Email);
        await accounts.ReplaceOneAsync(a => a.Id == doc.Id, doc, new ReplaceOptions { IsUpsert = true });
        timeline.Tell("db-write", $"Wrote account {doc.Id} to MongoDB", to: "mongo");
        await cache.SetAsync($"account:{doc.Id}", doc); // writes L1 + L2, backplane makes other nodes holding a copy refresh it from L2
        return ToProto(doc);
    }

    static Account ToProto(AccountDoc d) => new() { Id = d.Id, Name = d.Name, Email = d.Email };
}
