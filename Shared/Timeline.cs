using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Caching.Memory;
using StackExchange.Redis;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane;

// Demo-only side channel: every instance narrates what it does over Redis pub/sub, the Web page streams it live.
// Each region has its own Redis, so its own timeline.
public class Timeline(IConfiguration config)
{
    public static readonly RedisChannel Channel = RedisChannel.Literal("timeline");

    // Own connection, not the instrumented one from DI, so the narration stays out of the OpenTelemetry traces.
    private readonly ISubscriber _redis = ConnectionMultiplexer.Connect(config.GetConnectionString("redis")!).GetSubscriber();
    private readonly string _source = config["OTEL_SERVICE_NAME"]!; // Aspire sets it to the resource name, e.g. eu-account-1
    private readonly string? _region = config["REGION"];

    // step is the stage of the story the page groups into one scene (jwt, fanout, l1, l2, db-read, save, bp-send, ...).
    // from/to is the hop the page animates ("rabbit", "redis", "mongo", "web", or "l1" for this instance's memory), defaulting to this instance.
    // tone colors it: "hit", "miss", "skip", or null for a plain request/message.
    // key is the cache key, so the page can count down how long each copy has left.
    // by tells it on behalf of something that can't narrate itself, like Debezium ("kafka").
    // The page puts the region in front of a name that isn't a box (redis → eu-redis); another region's box is named in full (us-mongo).
    public void Tell(string step, string text, string? from = null, string? to = null, string? tone = null, string? key = null, string? by = null) =>
        _redis.Publish(Channel, JsonSerializer.Serialize(new { step, source = by ?? _source, text, from = from ?? _source, to = to ?? _source, tone, key }),
            CommandFlags.FireAndForget);

    // Told before the write, so the change feed it sets off is told after. home is the user's home region, whose MongoDB it goes to.
    public void Writing(string what, string home)
    {
        if (home == _region) Tell("db-write", $"Writing {what} to MongoDB", to: "mongo");
        else Tell("home-write", $"Writing {what} to {home}'s MongoDB, the user's home region", to: $"{home}-mongo");
    }

    // Told by a cache factory once it has the value, from the user's home region's MongoDB.
    public void Loaded(string key, string home)
    {
        if (home == _region) Say(key, "db-read", $"Loaded {key} from MongoDB", from: "mongo");
        else Say(key, "home-read", $"Loaded {key} from {home}'s MongoDB, the user's home region", from: $"{home}-mongo");
    }

    // FusionCache raises an event for every step, so the L1 / L2 / backplane story tells itself.
    public void Narrate(IFusionCache cache)
    {
        cache.Events.Memory.Hit += (_, e) => Say(e.Key, "l1", $"L1 (memory) hit {e.Key}", to: "l1", tone: "hit");
        cache.Events.Memory.Miss += (_, e) => Say(e.Key, "l1", $"L1 (memory) miss {e.Key}", to: "l1", tone: "miss");
        cache.Events.Distributed.Hit += (_, e) => Say(e.Key, "l2", $"L2 (Redis) hit {e.Key}, copied into L1", from: "redis", tone: "hit");
        cache.Events.Distributed.Miss += (_, e) => Say(e.Key, "l2", $"L2 (Redis) miss {e.Key}", to: "redis", tone: "miss");
        cache.Events.Distributed.Set += (_, e) => Say(e.Key, "save", $"Saved {e.Key} to L1 + L2", to: "redis");
        cache.Events.Memory.Eviction += (_, e) =>
        {
            if (e.Reason == EvictionReason.Expired) // every Set evicts the entry it overwrites too, as Replaced
                Say(e.Key, "evict", $"L1 (memory) evicted {e.Key}, its TTL ran out", to: "l1", tone: "miss");
        };
        // A remove, like the eviction after another region's write, drops the other instances' copies instead of refreshing them.
        cache.Events.Backplane.MessagePublished += (_, e) => Say(e.Message.CacheKey!, "bp-send",
            e.Message.Action == BackplaneMessageAction.EntryRemove ? $"Backplane: told other instances to drop {e.Message.CacheKey}"
                : $"Backplane: told other instances {e.Message.CacheKey} changed", to: "redis");
        cache.Events.Backplane.MessageReceived += (_, e) => Say(e.Message.CacheKey!, "bp-receive",
            e.Message.Action == BackplaneMessageAction.EntryRemove ? $"Backplane: {e.Message.CacheKey} was evicted on another instance, dropping my L1 copy if I have one"
                : $"Backplane: {e.Message.CacheKey} changed on another instance, refreshing my L1 copy if I have one", from: "redis");
    }

    private string? _lastText;
    private DateTime _lastAt;

    private void Say(string key, string step, string text, string? from = null, string? to = null, string? tone = null)
    {
        if (key.StartsWith("__fc:")) return; // FusionCache's internal tagging keys
        // FusionCache checks L1 twice on a miss (before and after its stampede lock), say it once.
        if (text == _lastText && DateTime.UtcNow - _lastAt < TimeSpan.FromMilliseconds(500)) return;
        (_lastText, _lastAt) = (text, DateTime.UtcNow);
        Tell(step, text, from, to, tone, key);
    }
}
