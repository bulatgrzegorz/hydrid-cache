using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using RabbitMQ.Client;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);
builder.AddTelemetry();
builder.AddRabbitMQClient("rabbit");
builder.AddRedisClient("redis");
builder.Services.AddSingleton<Timeline>();

var app = builder.Build();

// shortcut: no password, any user id gets a token. Add credentials if the demo ever needs them.
app.MapPost("/login/{userId}", async (string userId, IConnection rabbit, IConnectionMultiplexer redis, Timeline timeline) =>
{
    var token = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([new Claim("sub", userId)]),
        SigningCredentials = new SigningCredentials(Demo.JwtKey, SecurityAlgorithms.HmacSha256),
    });
    var db = redis.GetDatabase();
    await db.SetAddAsync(Cluster.Users, userId);
    // shard tells the caller which account/order instance owns this user
    var cluster = await Cluster.Load(db);
    var shard = cluster.OwnerOf(userId);
    timeline.Tell("jwt", $"Issued JWT for {userId}, owner is shard {shard} ({cluster.Strategy} over {cluster.Shards} shards)");

    await using var channel = await rabbit.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(Demo.UserLoggedIn, ExchangeType.Fanout);
    await channel.BasicPublishAsync(Demo.UserLoggedIn, "", Encoding.UTF8.GetBytes(userId));
    timeline.Tell("publish", $"Published UserLoggedIn({userId}) to RabbitMQ, fanout to every account/order instance", to: "rabbit");

    timeline.Tell("reply", $"Replied to Web with the JWT for {userId}", to: "web");
    return new { token, shard };
});

app.Run();
