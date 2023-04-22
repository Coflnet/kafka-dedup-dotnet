using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Kafka.Dedup
{
    public class LoadTest
    {
        public async Task Run()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json", true).AddEnvironmentVariables().Build();
            var clientConfig = Deduper.GetClientConfig(config);
            using var p = new ProducerBuilder<string, Carrier>(clientConfig).SetValueSerializer(Serializer.Instance).Build();
            var topic = config["SOURCE_TOPIC"];
            var messageCount = int.Parse(config["MESSAGE_COUNT"] ?? "100000");
            var random = new Random();
            Console.WriteLine($"Sending {messageCount} messages to {topic}");
            for (int i = 0; i < messageCount; i++)
            {
                p.Produce(topic, new() { Key = random.Next(0, 1000000).ToString(), Value = new Carrier { content = System.Text.Encoding.UTF8.GetBytes("test" + random.Next(1000000)) } });
            }
            p.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine("Done sending messages, gonna call it a day, have a nice day and bye!");
        }
    }
}