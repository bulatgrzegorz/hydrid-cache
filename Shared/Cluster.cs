using System.Security.Cryptography;
using System.Text;
using StackExchange.Redis;

// How many shards are in, and which strategy maps users onto them. Kept in Redis, so the page can scale out
// mid-demo and every service routes the same way. AppHost runs more instances than this; the rest wait on standby.
public record Cluster(int Shards, string Strategy)
{
    public static readonly string[] Strategies = ["modulo", "ring", "hrw"];
    static readonly RedisKey Key = "cluster";
    public static readonly RedisKey Users = "users"; // everyone who logged in, so the page can show where each of them lands

    public static async Task<Cluster> Load(IDatabase redis)
    {
        var v = await redis.HashGetAsync(Key, ["shards", "strategy"]);
        return new((int?)v[0] ?? 2, (string?)v[1] ?? "modulo");
    }

    public Task Save(IDatabase redis) => redis.HashSetAsync(Key, [new("shards", Shards), new("strategy", Strategy)]);

    public int OwnerOf(string userId)
    {
        var hash = Hash(userId);
        return Strategy switch
        {
            // Consistent hashing: one point per shard on a ring of uints, the user belongs to the first point clockwise.
            // uint subtraction wraps around, so point - user is exactly that clockwise distance.
            "ring" => Enumerable.Range(0, Shards).MinBy(i => RingPoint(i) - hash),
            // Rendezvous (HRW): every shard scores the user, the highest score wins.
            "hrw" => Enumerable.Range(0, Shards).MaxBy(i => Score(userId, i)),
            _ => (int)(hash % Shards),
        };
    }

    // Shard points aren't hashed: hashed points can land side by side, and then the page's ring is unreadable.
    // Instead each new shard's point halves the biggest slice, the way Cassandra assigned tokens by hand:
    // shard 0 at 0, shard 1 at half a turn, shard 2 at a quarter, shard 3 at three quarters. That's the shard's bits reversed.
    public static uint RingPoint(int shard)
    {
        uint point = 0;
        for (var bit = 0; shard >> bit != 0; bit++) point |= (uint)(shard >> bit & 1) << (31 - bit);
        return point;
    }
    public static uint Score(string userId, int shard) => Hash($"{userId}/{shard}");

    // string.GetHashCode is randomized per process, so hash with something stable.
    public static uint Hash(string s) => BitConverter.ToUInt32(MD5.HashData(Encoding.UTF8.GetBytes(s)));
}
