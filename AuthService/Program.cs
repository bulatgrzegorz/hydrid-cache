using System.Security.Claims;
using System.Text.Json;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using MongoDB.Bson;
using MongoDB.Driver;
using RabbitMQ.Client;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);
builder.AddTelemetry();
builder.AddRabbitMQClient("rabbit");
builder.AddRedisClient("redis");
builder.Services.AddSingleton<Timeline>();
var region = builder.Configuration["REGION"]!;
// Every region's users, to find a user's home: Aspire hands AuthService every region's MongoDB.
var users = builder.Configuration.PerRegion("mongo")
    .ToDictionary(c => c.Key, c => new MongoClient(c.Value).GetDatabase("users").GetCollection<BsonDocument>("users"));

var app = builder.Build();

// shortcut: no password, any user id gets a token. Add credentials if the demo ever needs them.
app.MapPost("/login/{userId}", async (string userId, IConnection rabbit, IConnectionMultiplexer redis, Timeline timeline) =>
{
    // A user's home is the region of their first login, which signs them up there, and their data lives only in its MongoDB.
    // shortcut: it asks every region's MongoDB, and first logins in two regions at once sign the user up twice. Production keeps
    // one global user directory.
    var home = (await Task.WhenAll(users.Select(async u => await u.Value.Find(new BsonDocument("_id", userId)).AnyAsync() ? u.Key : null)))
        .FirstOrDefault(r => r is not null);
    if (home is null) await users[home = region].InsertOneAsync(new BsonDocument("_id", userId));
    var token = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([new Claim("sub", userId), new Claim(Demo.HomeClaim, home)]),
        SigningCredentials = new SigningCredentials(Demo.JwtKey, SecurityAlgorithms.HmacSha256),
    });
    var db = redis.GetDatabase();
    await db.SetAddAsync(Cluster.Users, userId);
    // shard tells the caller which account/order instance owns this user
    var cluster = await Cluster.Load(db);
    var shard = cluster.OwnerOf(userId);
    timeline.Tell("jwt", $"Issued JWT for {userId}, home region {home}, owner is shard {shard} ({cluster.Strategy} over {cluster.Shards} shards)");

    await using var channel = await rabbit.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(Demo.UserLoggedIn, ExchangeType.Fanout);
    await channel.BasicPublishAsync(Demo.UserLoggedIn, "", JsonSerializer.SerializeToUtf8Bytes(new Login(userId, home)));
    timeline.Tell("publish", $"Published UserLoggedIn({userId}) to RabbitMQ, fanout to every account/order instance", to: "rabbit");

    timeline.Tell("reply", $"Replied to Web with the JWT for {userId}", to: "web");
    return new { token, shard, home };
});

app.Run();
