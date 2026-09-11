using System;
using System.Linq;
using Prometheus;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Security.OpenBao;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Kafka.Dedup
{
    class Program
    {


        static async Task Main(string[] args)
        {
            LoadOpenBaoIntoEnvironment();

            if (!StartupChecks.Run())
            {
                Environment.Exit(StartupChecks.ConfigErrorExitCode);
                return;
            }

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
                Console.WriteLine("continueing with deduping");
            }

            await new Deduper().Run(stopSource.Token);
        }

        /// <summary>
        /// Pulls secrets from OpenBao using the shared Coflnet.Core loader (the same one SkyApi uses)
        /// and projects them into environment variables, because the rest of this app reads its
        /// configuration through SimplerConfig (i.e. environment variables), not IConfiguration.
        /// When OpenBao is disabled (no OPENBAO__ADDR / OPENBAO__ENABLED) the loader is a no-op and
        /// the app runs purely on the environment — which is what local development relies on.
        /// </summary>
        private static void LoadOpenBaoIntoEnvironment()
        {
            // AddOpenBaoFromEnvironment() throws on a failed *required* load (OPENBAO__OPTIONAL=false),
            // matching the previous behaviour. StartupChecks reports the same problems in a friendlier
            // way and stays the source of user-facing remediation; the loader's own stderr lines
            // explain transport/auth failures.
            var configuration = new ConfigurationBuilder()
                .AddOpenBaoFromEnvironment()
                .Build();

            foreach (var pair in configuration.AsEnumerable())
            {
                if (pair.Value is null)
                    continue; // section nodes (e.g. "KAFKA") carry no value — skip them.

                // The loader stores KV keys with ':' as the hierarchy separator (a secret named
                // KAFKA__PASSWORD becomes "KAFKA:PASSWORD"); SimplerConfig/StartupChecks expect the
                // original '__' env-var form, so convert back.
                Environment.SetEnvironmentVariable(pair.Key.Replace(":", "__"), pair.Value);
            }
        }



    }
}
