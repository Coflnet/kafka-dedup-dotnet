using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;

namespace Coflnet.Kafka.Dedup
{
    class Program
    {


        static async Task Main(string[] args)
        {
            var stopSource = new CancellationTokenSource();
            
            Console.CancelKeyPress += delegate
            {
                stopSource.Cancel();
                Console.WriteLine("stopping");
                Thread.Sleep(500);
            };

            await new Deduper().Run(stopSource.Token);
        }



    }
}