using System.Text.Json;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using ZiggyCreatures.Caching.Fusion;

// Demo-only side channel: every instance narrates what it does over Redis pub/sub, the Web page streams it live.
public class Timeline(IConfiguration config)
{
    public static readonly RedisChannel Channel = RedisChannel.Literal("timeline");

    // Own connection, not the instrumented one from DI, so the narration stays out of the OpenTelemetry traces.
    readonly ISubscriber redis = ConnectionMultiplexer.Connect(config.GetConnectionString("redis")!).GetSubscriber();
    readonly string source = config["OTEL_SERVICE_NAME"]!; // Aspire sets it to the resource name, e.g. account-1

    // step is the stage of the story the page groups into one scene (jwt, fanout, l1, l2, db-read, save, bp-send, ...).
    // from/to is the hop the page animates ("rabbit", "redis", "mongo", "web", or "l1" for this instance's memory), defaulting to this instance.
    // tone colors it: "hit", "miss", "skip", or null for a plain request/message.
    public void Tell(string step, string text, string? from = null, string? to = null, string? tone = null) =>
        redis.Publish(Channel, JsonSerializer.Serialize(new { step, source, text, from = from ?? source, to = to ?? source, tone }),
            CommandFlags.FireAndForget);

    // FusionCache raises an event for every step, so the L1 / L2 / MongoDB / backplane story tells itself.
    public void Narrate(IFusionCache cache)
    {
        cache.Events.Memory.Hit += (_, e) => Say(e.Key, "l1", $"L1 (memory) hit {e.Key}", to: "l1", tone: "hit");
        cache.Events.Memory.Miss += (_, e) => Say(e.Key, "l1", $"L1 (memory) miss {e.Key}", to: "l1", tone: "miss");
        cache.Events.Distributed.Hit += (_, e) => Say(e.Key, "l2", $"L2 (Redis) hit {e.Key}, copied into L1", from: "redis", tone: "hit");
        cache.Events.Distributed.Miss += (_, e) => Say(e.Key, "l2", $"L2 (Redis) miss {e.Key}", to: "redis", tone: "miss");
        cache.Events.FactorySuccess += (_, e) => Say(e.Key, "db-read", $"Loaded {e.Key} from MongoDB", from: "mongo");
        cache.Events.Distributed.Set += (_, e) => Say(e.Key, "save", $"Saved {e.Key} to L1 + L2", to: "redis");
        cache.Events.Backplane.MessagePublished += (_, e) =>
            Say(e.Message.CacheKey!, "bp-send", $"Backplane: told other instances {e.Message.CacheKey} changed", to: "redis");
        cache.Events.Backplane.MessageReceived += (_, e) =>
            Say(e.Message.CacheKey!, "bp-receive", $"Backplane: {e.Message.CacheKey} changed on another instance, refreshing my L1 copy if I have one", from: "redis");
    }

    string? lastText;
    DateTime lastAt;

    void Say(string key, string step, string text, string? from = null, string? to = null, string? tone = null)
    {
        if (key.StartsWith("__fc:")) return; // FusionCache's internal tagging keys
        // FusionCache checks L1 twice on a miss (before and after its stampede lock), say it once.
        if (text == lastText && DateTime.UtcNow - lastAt < TimeSpan.FromMilliseconds(500)) return;
        (lastText, lastAt) = (text, DateTime.UtcNow);
        Tell(step, text, from, to, tone);
    }
}
