using System.Net.Http.Json;
#pragma warning disable ASPIREMONGODB001

var builder = DistributedApplication.CreateBuilder(args);

const int shards = 3; 

string[] regions = ["eu", "us"];

var web = builder
    .AddProject<Projects.Web>("web")
    .WithHttpEndpoint(port: 5050);

// shortcut: one Kafka and one Debezium for both regions.
// Production runs a Kafka per region, next to its MongoDB, and mirrors the topics across with MirrorMaker 2, placed next to the target cluster.
// The JVMs are kept small, so everything still fits in Podman's default 2 GB machine: small heaps, the serial collector, and
// the quick JIT only. The image's REST proxy, which nothing here uses, gets the least.
const string lean = "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=32m";

var kafka = BuildKafka(builder);
var kafkaServers = kafka.Resource.InternalEndpoint.Property(EndpointProperty.HostAndPort);

var debezium = BuildDebezium(builder)
    .WaitFor(kafka); 

// Every region's MongoDB up front: a region's shards and AuthService reach the others' too, for the users whose home is there.
var mongos = regions.ToDictionary(r => r, region => 
            builder
                .AddMongoDB($"{region}-mongo")
                .WithReplicaSet() // a replica set has a change stream, which Debezium reads
                .WithTlsMode(MongoDBTlsMode.PreferTls)); // the services keep TLS; Debezium's JVM doesn't trust Aspire's developer certificate

foreach (var (i, region) in regions.Index())
{
    AddRegion(region, offset: 500 * i);
}

builder.Build().Run();
return;

// One region: eu on the ports below, us on the same ports + 500. Resources are named after their region (eu-redis, us-account-0),
// but each region's services reach theirs by the plain names (redis, accounts). The exception is every region's MongoDB, as
// <region>-mongo: AuthService finds a user's home there, and shards read and write the data of users whose home is elsewhere.
void AddRegion(string region, int offset)
{
    var redis = builder.AddRedis($"{region}-redis");
    var rabbit = builder.AddRabbitMQ($"{region}-rabbit");
    var mongo = mongos[region];
    // Debezium streams this region's MongoDB into Kafka, one topic per collection: eu.accounts.accounts, eu.orders.orders.
    // Registered on every start, since the containers are fresh each run; a PUT creates the connector or updates it.
    debezium.OnResourceReady(async (_, _, ct) =>
    {
        var mongoPassword = Uri.EscapeDataString((await mongo.Resource.PasswordParameter!.GetValueAsync(ct))!);
        
        using var http = new HttpClient();
        http.BaseAddress = new Uri(debezium.GetEndpoint("http").Url);
        
        var response = await http.PutAsJsonAsync($"connectors/{region}-mongo/config", new Dictionary<string, string>
        {
            ["connector.class"] = "io.debezium.connector.mongodb.MongoDbConnector",
            // Direct: the replica set's one member calls itself localhost, which inside Debezium's container is Debezium.
            // Debezium still wants the set's name, which Aspire takes from the resource's.
            ["mongodb.connection.string"] = $"mongodb://admin:{mongoPassword}@{region}-mongo:27017/?authSource=admin&replicaSet={region}-mongo&directConnection=true",
            ["topic.prefix"] = region,
            ["collection.include.list"] = "accounts.accounts,orders.orders",
            ["capture.mode"] = "change_streams_update_full", // an update carries the whole document: the user id and writtenBy
            ["snapshot.mode"] = "no_data", // changes from now on, not the documents already there
            ["key.converter"] = "org.apache.kafka.connect.json.JsonConverter",
            ["key.converter.schemas.enable"] = "false",
            ["value.converter"] = "org.apache.kafka.connect.json.JsonConverter",
            ["value.converter.schemas.enable"] = "false",
        }, ct);
        
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"Debezium refused {region}-mongo: {await response.Content.ReadAsStringAsync(ct)}");
    });

    var project = builder
        .AddProject<Projects.AuthService>($"{region}-auth")
        .WithHttpEndpoint(port: 5000 + offset);
    
    var auth = InRegion(project);
    
    web
        .WithReference(redis)
        .WaitFor(redis)
        .WithReference(auth);

    AddShards<Projects.AccountService>("account", mongo.AddDatabase($"{region}-accounts", "accounts"), port: 5100 + offset);
    AddShards<Projects.OrderService>("order", mongo.AddDatabase($"{region}-orders", "orders"), port: 5200 + offset);

    // One process per shard: account-0 on 5100, account-1 on 5101, ...
    void AddShards<T>(string name, IResourceBuilder<MongoDBDatabaseResource> db, int port) where T : IProjectMetadata, new()
    {
        for (var i = 0; i < shards; i++)
        {
            var resourceBuilderProject = builder
                .AddProject<T>($"{region}-{name}-{i}")
                .WithHttpEndpoint(port: port + i);
            
            var shard = 
                InRegion(resourceBuilderProject)
                    .WithReference(db, db.Resource.DatabaseName)
                    .WaitFor(db)
                    .WithReference(kafka)
                    .WaitFor(kafka)
                    .WithEnvironment("SHARD", i.ToString());
            
            web.WithReference(shard);
        }
    }

    // What AuthService and every shard get: this region's Redis and RabbitMQ, and every region's MongoDB.
    IResourceBuilder<ProjectResource> InRegion(IResourceBuilder<ProjectResource> resourceBuilderProject)
    {
        resourceBuilderProject
            .WithEnvironment("REGION", region)
            .WithReference(redis, "redis").WaitFor(redis)
            .WithReference(rabbit, "rabbit").WaitFor(rabbit);

        foreach (var mongoResource in mongos.Values)
        {
            resourceBuilderProject
                .WithReference(mongoResource)
                .WaitFor(mongoResource);
        }
        
        return resourceBuilderProject;
    }
}

IResourceBuilder<KafkaServerResource> BuildKafka(IDistributedApplicationBuilder distributedApplicationBuilder)
{
    return distributedApplicationBuilder.AddKafka("kafka")
        .WithEnvironment("KAFKA_HEAP_OPTS", "-Xmx192m")
        .WithEnvironment("KAFKA_JVM_PERFORMANCE_OPTS", lean)
        .WithEnvironment("KAFKAREST_HEAP_OPTS", "-Xmx48m")
        .WithEnvironment("KAFKAREST_JVM_PERFORMANCE_OPTS", lean)
        .WithEnvironment("KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE", "16777216") // 128 MB by default, allocated up front
        .WithEnvironment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"); // consumer offsets for four groups, not fifty partitions' worth
}

IResourceBuilder<ContainerResource> BuildDebezium(IDistributedApplicationBuilder distributedApplicationBuilder)
{
    return distributedApplicationBuilder
        .AddContainer("debezium", "quay.io/debezium/connect", "3.7.0.Final")
        .WithHttpEndpoint(targetPort: 8083)
        .WithHttpHealthCheck("/connectors")
        .WithEnvironment("BOOTSTRAP_SERVERS", kafkaServers)
        .WithEnvironment("GROUP_ID", "debezium")
        .WithEnvironment("CONFIG_STORAGE_TOPIC", "debezium-configs")
        .WithEnvironment("OFFSET_STORAGE_TOPIC", "debezium-offsets")
        .WithEnvironment("STATUS_STORAGE_TOPIC", "debezium-status")
        .WithEnvironment("HEAP_OPTS", "-Xmx192m")
        .WithEnvironment("KAFKA_JVM_PERFORMANCE_OPTS", lean)
        .WithEnvironment("CONNECT_PLUGIN_DISCOVERY", "service_load");
}
