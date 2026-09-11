using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;

namespace Coflnet.Kafka.Dedup.Test
{
    public class DedupTest
    {
        [Test]
        public void RepeatSalesHaveDistinctIdentitiesButKeepTheirRoutingKey()
        {
            var first = new Message<string, Carrier>
            {
                Key = "item-uuid",
                Headers = new Headers { { "deduplication-key", Encoding.UTF8.GetBytes("first-auction") } }
            };
            var second = new Message<string, Carrier>
            {
                Key = first.Key,
                Headers = new Headers { { "deduplication-key", Encoding.UTF8.GetBytes("second-auction") } }
            };

            var seen = new HashSet<string>();
            var kept = new[] { first, second, first }.Where(m => seen.Add(Deduper.GetDeduplicationKey(m))).ToList();
            Assert.That(kept, Is.EqualTo(new[] { first, second }));
            Assert.That(kept.Select(m => m.Key), Is.EqualTo(new[] { "item-uuid", "item-uuid" }));
            Assert.That(kept.Select(Deduper.GetDeduplicationKey), Is.EqualTo(new[] { "first-auction", "second-auction" }));
        }

        [Test]
        public void MessagesWithoutDeduplicationHeaderKeepTheirIdentity()
        {
            var message = new Message<string, Carrier> { Key = "auction-bid-count-end" };
            Assert.That(Deduper.GetDeduplicationKey(message), Is.EqualTo(message.Key));
            message.Headers = new Headers();
            Assert.That(Deduper.GetDeduplicationKey(message), Is.EqualTo(message.Key));
        }

        [Test]
        public void LoadTest()
        {
            SimplerConfig.Config.Instance = new MockConfig();
        }


    }

    public class MockConfig : SimplerConfig.ISimplerConfig
    {

        static Dictionary<string, string> Settings = new Dictionary<string, string>() {
             { "KAFKA_HOST", "localhost:9092" }
        };
        public string this[string key] => Settings[key];

        public string[] StartArgs { set => throw new System.NotImplementedException(); }
    }
}
