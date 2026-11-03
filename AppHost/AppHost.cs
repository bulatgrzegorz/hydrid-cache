var builder = DistributedApplication.CreateBuilder(args);

const int shards = 3; // instances per service; the page scales between 1 and this many shards, the rest wait on standby
var redis = builder.AddRedis("redis");
var rabbit = builder.AddRabbitMQ("rabbit");
var mongo = builder.AddMongoDB("mongo");

var auth = builder.AddProject<Projects.AuthService>("auth")
    .WithHttpEndpoint(port: 5000)
    .WithReference(rabbit).WaitFor(rabbit)
    .WithReference(redis).WaitFor(redis);

// Demo page on http://localhost:5050, it calls auth and every shard
var web = builder.AddProject<Projects.Web>("web")
    .WithHttpEndpoint(port: 5050)
    .WithReference(redis).WaitFor(redis)
    .WithReference(auth);

AddShards<Projects.AccountService>("account", mongo.AddDatabase("accounts"), port: 5100);
AddShards<Projects.OrderService>("order", mongo.AddDatabase("orders"), port: 5200);

builder.Build().Run();

// One process per shard: account-0 on 5100, account-1 on 5101, ...
void AddShards<T>(string name, IResourceBuilder<MongoDBDatabaseResource> db, int port) where T : IProjectMetadata, new()
{
    for (var i = 0; i < shards; i++)
    {
        var shard = builder.AddProject<T>($"{name}-{i}")
            .WithHttpEndpoint(port: port + i)
            .WithReference(db).WaitFor(db)
            .WithReference(redis).WaitFor(redis)
            .WithReference(rabbit).WaitFor(rabbit)
            .WithEnvironment("SHARD", i.ToString());
        web.WithReference(shard);
    }
}
