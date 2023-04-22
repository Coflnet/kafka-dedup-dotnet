using System;
using System.Linq;
using Prometheus;
using System.Threading;
using System.Threading.Tasks;

namespace Coflnet.Kafka.Dedup
{
    class Program
    {


        static async Task Main(string[] args)
        {
            var stopSource = new CancellationTokenSource();
            var server = new MetricServer(port: 8000);
            server.Start();
            
            Console.CancelKeyPress += delegate
            {
                stopSource.Cancel();
                Console.WriteLine("stopping");
                Thread.Sleep(500);
            };
            if(args.Contains("--test"))
            {
                await new LoadTest().Run();
                return;
            }

            await new Deduper().Run(stopSource.Token);
        }



    }
}