using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using MongoDB.Bson;
using MongoDB.Driver;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

public static class Demo
{
    public const string UserLoggedIn = "UserLoggedIn";
    // Every service write says which region made it, so the eviction after it can skip that region: it saved the new value already.
    public const string WrittenBy = "writtenBy";
    // The JWT's claim for the user's home region, the only one whose MongoDB holds their data. Their first login decides it.
    public const string HomeClaim = "home";

    // shortcut: hardcoded signing key, move it to a secret if this ever leaves the demo.
    public static readonly SymmetricSecurityKey JwtKey = new("ndc-demo-jwt-signing-key-at-least-32-bytes"u8.ToArray());

    // Short, so evictions show up during the demo. L2 outlives L1, so an L1 eviction falls back to Redis, not MongoDB.
    public static readonly TimeSpan L1Ttl = TimeSpan.FromMinutes(1), L2Ttl = TimeSpan.FromMinutes(2);

    public static string UserId(this ServerCallContext context) => context.GetHttpContext().User.FindFirstValue(ClaimTypes.NameIdentifier)!;
    
    public static string HomeRegion(this ServerCallContext context) => context.GetHttpContext().User.FindFirstValue(HomeClaim)!;

    // Aspire hands a service another region's resource as ConnectionStrings:<region>-<resource>, e.g. us-mongo; keyed by region here.
    public static Dictionary<string, string> PerRegion(this IConfiguration config, string resource) =>
        config.GetSection("ConnectionStrings").GetChildren().Where(c => c.Key.EndsWith($"-{resource}"))
            .ToDictionary(c => c.Key[..^(resource.Length + 1)], c => c.Value!);
    
    extension(WebApplicationBuilder builder)
    {
        // Traces, metrics and logs to the Aspire dashboard; Aspire hands every project the OTLP endpoint via env vars.
        // Redis, RabbitMQ and MongoDB spans come from the Aspire client integrations on their own.
        public void AddTelemetry()
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
        public void AddShardedService()
        {
            builder.AddTelemetry();
            builder.WebHost.ConfigureKestrel(k => k.ConfigureEndpointDefaults(e => e.Protocols = HttpProtocols.Http2)); // gRPC without TLS
            builder.Services.AddGrpc();
            builder.Services
                .AddAuthentication()
                .AddJwtBearer(o => o.TokenValidationParameters = new() 
                { 
                    IssuerSigningKey = JwtKey, 
                    ValidateIssuer = false, 
                    ValidateAudience = false, 
                });
            
            builder.Services.AddAuthorization();
            builder.AddRabbitMQClient("rabbit");
            builder.AddRedisDistributedCache("redis");
            builder.Services.AddSingleton<Timeline>();
            builder.Services.AddSingleton<HomeDatabase>();
            // MemoryCache drops an expired entry only when something touches it, so sweep every second: the page sees the eviction as the TTL runs out.
            var memory = new MemoryCache(new MemoryCacheOptions());
            builder.Services.AddSingleton(new Timer(_ => memory.Compact(0), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1))); // DI keeps it alive
            builder.Services.AddFusionCache()
                .WithOptions(o =>
                {
                    o.BackplaneChannelPrefix = builder.Environment.ApplicationName; // accounts and orders share Redis, keep their backplanes apart
                    o.EnableSyncEventHandlersExecution = true; // background handlers narrated out of order; ours only fire a Redis publish
                })
                .WithDefaultEntryOptions(o => { o.Duration = L2Ttl; o.MemoryCacheDuration = L1Ttl; })
                .WithMemoryCache(memory)
                .WithSerializer(new FusionCacheSystemTextJsonSerializer())
                .WithRegisteredDistributedCache()
                .WithStackExchangeRedisBackplane(o => o.Configuration = builder.Configuration.GetConnectionString("redis"))
                .WithPostSetup((sp, cache) => sp.GetRequiredService<Timeline>().Narrate(cache));
        }
    }
    
    extension(WebApplication app)
    {
        // Every instance gets every UserLoggedIn (fanout, own queue); only the owning shard warms its cache. A login outside the
        // user's home region warms it from the home region's MongoDB.
        public async Task HydrateOnLogin(Func<string, string, Task> hydrate)
        {
            var shard = app.Configuration.GetValue<int>("SHARD");
            var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
            var timeline = app.Services.GetRequiredService<Timeline>();
            var channel = await app.Services.GetRequiredService<IConnection>().CreateChannelAsync();
            
            await channel.ExchangeDeclareAsync(UserLoggedIn, ExchangeType.Fanout);
            // shortcut: throwaway queue per instance, logins while a shard is starting or down are lost. Use a durable queue per shard if that matters.
            var queue = await channel.QueueDeclareAsync();
            await channel.QueueBindAsync(queue.QueueName, UserLoggedIn, "");
            
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, e) =>
            {
                var (userId, userHome) = JsonSerializer.Deserialize<Login>(e.Body.Span)!;
                
                var owner = (await Cluster.Load(redis)).OwnerOf(userId);
                if (owner != shard)
                {
                    timeline.Tell("fanout", $"Got UserLoggedIn({userId}), shard {owner} owns this user, ignoring", from: "rabbit", tone: "skip");
                    return;
                }
                
                timeline.Tell("fanout", $"Got UserLoggedIn({userId}), my user, hydrating cache", from: "rabbit");
                await hydrate(userId, userHome);
            };
            
            await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer);
        }

        // A region caches users whose home is elsewhere too, and no backplane message crosses regions. So Debezium streams every region's
        // MongoDB into Kafka, and every instance of a service in a region joins one consumer group (eu-account): Kafka gives the feed to
        // one of them, and to another when that one stops. It evicts every change another region wrote, in any region's database: a home
        // write evicts the other regions' copies, and a write from another region evicts the home region's. Its own writes already saved
        // the new value. The database has a write before its change feed reports it, so the eviction is never early. Its backplane
        // message then drops every other copy in the region. Cheap when nothing here cached the user: a Redis delete and a pub/sub message.
        // The offset is committed after the eviction, so whichever instance takes the feed over carries on from there.
        public async Task EvictOnChangeFeed(string service, string collection, string userField, Func<string, Task> evict)
        {
            var dbs = app.Services.GetRequiredService<HomeDatabase>();
            var region = dbs.Region;
            
            var kafka = app.Configuration.GetConnectionString("kafka");
            var timeline = app.Services.GetRequiredService<Timeline>();
            
            // Debezium names a topic <region>.<database>.<collection>, and each database here is named after its one collection.
            var topics = dbs.Regions.Select(r => $"{r}.{collection}.{collection}").ToList();
            
            // Created here rather than on Debezium's first change, so the consumer is already reading when that change comes. Every instance
            // tries; the first one wins.
            await WebApplication.CreateTopics(kafka, topics);
            
            var timeout = TimeSpan.FromSeconds(10);
            var consumer = new ConsumerBuilder<Ignore, string?>(new ConsumerConfig { BootstrapServers = kafka, GroupId = $"{region}-{service}", EnableAutoCommit = false })
                // A new group starts at the end of each topic, and commits that spot right away, so a restart before its first change resumes there.
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    var start = 
                        c.Committed(partitions, timeout).Select(
                            p => 
                                p.Offset != Offset.Unset ? 
                                    p : 
                                    new TopicPartitionOffset(p.TopicPartition, c.QueryWatermarkOffsets(p.TopicPartition, timeout).High)).ToList();

                    if (start.Count > 0)
                    {
                        c.Commit(start);
                    }
                
                    return start;
                })
                .Build();
        
            consumer.Subscribe(topics);
        
            var stopping = app.Lifetime.ApplicationStopping;
            _ = Task.Run(async () =>
            {
                try
                {
                    while (!stopping.IsCancellationRequested)
                    {
                        var change = consumer.Consume(stopping);
                        // The whole document as extended JSON; null for a delete, and the tombstone after it.
                        var after = change.Message.Value is { } value
                            ? JsonNode.Parse(value)!["after"]?.GetValue<string>()
                            : null;
                        if (after is not null)
                        {
                            var doc = BsonDocument.Parse(after);
                            var userId = doc.GetValue(userField).AsString;
                            var writer = doc.GetValue(WrittenBy, "").AsString;
                            var source =  change.Topic.Split('.')[0];
                            
                            if (writer != region)
                            {
                                // Debezium can't narrate, so the consumer tells its part.
                                timeline.Tell("cdc-out",
                                    $"Change stream: {userId}'s {collection} changed in {source}'s MongoDB, publishing it to Kafka",
                                    from: $"{source}-mongo", to: "kafka", by: "kafka");
                                
                                timeline.Tell("cdc-in",
                                    $"Kafka: {writer} wrote {userId}'s {collection}, evicting the cached copy",
                                    from: "kafka");
                                
                                await evict(userId);
                            }
                        }
                        
                        try
                        {
                            consumer.Commit(change);
                        }
                        catch (KafkaException)
                        {
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                } // leaves the group now, so a restart doesn't wait out the old session
            }, stopping).ContinueWith(t => app.Logger.LogError(t.Exception, "Change feed on {Collection} stopped", collection), TaskContinuationOptions.OnlyOnFaulted);
        }

        private static async Task CreateTopics(string? kafka, List<string> topics)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka }).Build();
            
            try
            {
                await admin.CreateTopicsAsync(topics.Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 }));
            }
            catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
            {
                //ignored, we just want them to be present
            }
        }
    }
}

// UserLoggedIn's message: who logged in, and their home region.
public record Login(string UserId, string Home);

// Where a user's data lives: only in their home region's MongoDB. A call in another region reads and writes it there, so Aspire hands
// each shard every region's MongoDB as ConnectionStrings:<region>-mongo. Its own region's is the client Aspire set up.
public class HomeDatabase(IConfiguration config, IMongoDatabase own)
{
    public readonly string Region = config["REGION"]!;

    private readonly Dictionary<string, IMongoDatabase> _databases = config.PerRegion("mongo").ToDictionary(c => c.Key,
        c => c.Key == config["REGION"] ? own : new MongoClient(c.Value).GetDatabase(own.DatabaseNamespace.DatabaseName));

    public IEnumerable<string> Regions => _databases.Keys;

    public IMongoDatabase this[string home] => _databases[home];
}
