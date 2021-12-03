using System.Collections.Generic;
using NUnit.Framework;

namespace Coflnet.Kafka.Dedup.Test
{
    public class DedupTest
    {
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