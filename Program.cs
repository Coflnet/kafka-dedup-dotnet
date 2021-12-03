using System.Collections;
using System.Threading.Tasks;

namespace Coflnet.Kafka.Dedup
{
    class Program
    {


        static async Task Main(string[] args)
        {
            await new Deduper().Run();
        }



    }
}