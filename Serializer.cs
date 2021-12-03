using System;
using Confluent.Kafka;

namespace Coflnet.Kafka.Dedup
{
    class Serializer : ISerializer<Carrier>
    {
        public static Serializer Instance = new Serializer();
        public byte[] Serialize(Carrier data, SerializationContext context)
        {
            return data.content;
        }
    }


    class Carrier
    {
        public byte[] content;
    }

    class Deserializer : IDeserializer<Carrier>
    {
        public static Deserializer Instance = new Deserializer();

        public Carrier Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return new Carrier() { content = data.ToArray() };
        }
    }
}