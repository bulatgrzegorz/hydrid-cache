using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

public static class Demo
{
    public const string UserLoggedIn = "UserLoggedIn";

    // shortcut: hardcoded signing key, move it to a secret if this ever leaves the demo.
    public static readonly SymmetricSecurityKey JwtKey = new("ndc-demo-jwt-signing-key-at-least-32-bytes"u8.ToArray());

    // string.GetHashCode is randomized per process, so shard on a stable hash.
    public static int ShardOf(string userId, int shards) =>
        (int)(BitConverter.ToUInt32(MD5.HashData(Encoding.UTF8.GetBytes(userId))) % shards);

    public static string UserId(this ServerCallContext context) =>
        context.GetHttpContext().User.FindFirstValue(ClaimTypes.NameIdentifier)!;

    // Traces, metrics and logs to the Aspire dashboard; Aspire hands every project the OTLP endpoint via env vars.
    // Redis, RabbitMQ and MongoDB spans come from the Aspire client integrations on their own.
    public static void AddTelemetry(this WebApplicationBuilder builder)
    {
        builder.Logging.AddOpenTelemetry(o => o.IncludeFormattedMessage = true);
        builder.Services.AddOpenTelemetry()
            .WithTracing(t => t
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddSource("Grpc.Net.Client")
                .AddFusionCacheInstrumentation(o =>
                {
                    o.IncludeMemoryLevel = true;
                    o.IncludeDistributedLevel = true;
                    o.IncludeBackplane = true;
                }))
            .WithMetrics(m => m
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddFusionCacheInstrumentation())
            .UseOtlpExporter();
    }

    // What Account and Order services share: gRPC behind JWT, FusionCache with L1 memory + L2 Redis + Redis backplane.
    public static void AddShardedService(this WebApplicationBuilder builder)
    {
        builder.AddTelemetry();
        builder.WebHost.ConfigureKestrel(k => k.ConfigureEndpointDefaults(e => e.Protocols = HttpProtocols.Http2)); // gRPC without TLS
        builder.Services.AddGrpc();
        builder.Services.AddAuthentication().AddJwtBearer(o => o.TokenValidationParameters = new()
        {
            IssuerSigningKey = JwtKey,
            ValidateIssuer = false,
            ValidateAudience = false,
        });
        builder.Services.AddAuthorization();
        builder.AddRabbitMQClient("rabbit");
        builder.AddRedisDistributedCache("redis");
        builder.Services.AddSingleton<Timeline>();
        builder.Services.AddFusionCache()
            .WithOptions(o =>
            {
                o.BackplaneChannelPrefix = builder.Environment.ApplicationName; // accounts and orders share Redis, keep their backplanes apart
                o.EnableSyncEventHandlersExecution = true; // background handlers narrated out of order; ours only fire a Redis publish
            })
            .WithDefaultEntryOptions(o => o.Duration = TimeSpan.FromMinutes(10))
            .WithSerializer(new FusionCacheSystemTextJsonSerializer())
            .WithRegisteredDistributedCache()
            .WithStackExchangeRedisBackplane(o => o.Configuration = builder.Configuration.GetConnectionString("redis"))
            .WithPostSetup((sp, cache) => sp.GetRequiredService<Timeline>().Narrate(cache));
    }

    // Every instance gets every UserLoggedIn (fanout, own queue); only the owning shard warms its cache.
    public static async Task HydrateOnLogin(this WebApplication app, Func<string, Task> hydrate)
    {
        int shard = app.Configuration.GetValue<int>("SHARD"), shards = app.Configuration.GetValue<int>("SHARDS");
        var timeline = app.Services.GetRequiredService<Timeline>();
        var channel = await app.Services.GetRequiredService<IConnection>().CreateChannelAsync();
        await channel.ExchangeDeclareAsync(UserLoggedIn, ExchangeType.Fanout);
        // shortcut: throwaway queue per instance, logins while a shard is starting or down are lost. Use a durable queue per shard if that matters.
        var queue = await channel.QueueDeclareAsync();
        await channel.QueueBindAsync(queue.QueueName, UserLoggedIn, "");
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, e) =>
        {
            var userId = Encoding.UTF8.GetString(e.Body.Span);
            var owner = ShardOf(userId, shards);
            if (owner != shard)
            {
                timeline.Tell("fanout", $"Got UserLoggedIn({userId}), shard {owner} owns this user, ignoring", from: "rabbit", tone: "skip");
                return;
            }
            timeline.Tell("fanout", $"Got UserLoggedIn({userId}), my user, hydrating cache", from: "rabbit");
            await hydrate(userId);
        };
        await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer);
    }
}
