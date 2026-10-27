using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
builder.AddTelemetry();
builder.AddRabbitMQClient("rabbit");
builder.Services.AddSingleton<Timeline>();

var app = builder.Build();
var shards = app.Configuration.GetValue<int>("SHARDS");

// shortcut: no password, any user id gets a token. Add credentials if the demo ever needs them.
app.MapPost("/login/{userId}", async (string userId, IConnection rabbit, Timeline timeline) =>
{
    var token = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([new Claim("sub", userId)]),
        SigningCredentials = new SigningCredentials(Demo.JwtKey, SecurityAlgorithms.HmacSha256),
    });
    // shard tells the caller which account/order instance owns this user
    var shard = Demo.ShardOf(userId, shards);
    timeline.Tell("jwt", $"Issued JWT for {userId}, owner is shard {shard}");

    await using var channel = await rabbit.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(Demo.UserLoggedIn, ExchangeType.Fanout);
    await channel.BasicPublishAsync(Demo.UserLoggedIn, "", Encoding.UTF8.GetBytes(userId));
    timeline.Tell("publish", $"Published UserLoggedIn({userId}) to RabbitMQ, fanout to every account/order instance", to: "rabbit");

    timeline.Tell("reply", $"Replied to Web with the JWT for {userId}", to: "web");
    return new { token, shard };
});

app.Run();
