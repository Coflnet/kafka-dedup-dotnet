using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Coflnet.Kafka.Dedup
{
    /// <summary>
    /// Validates the effective configuration once OpenBao has (optionally) populated the
    /// environment, and prints a human-readable report. Designed so that a misconfigured pod
    /// tells you exactly what is missing and how to fix it instead of dying with an opaque
    /// Kafka/Redis exception. Works the same in development (env vars only) and in the cluster
    /// (secrets supplied through OpenBao).
    /// </summary>
    internal static class StartupChecks
    {
        // sysexits.h EX_CONFIG — "configuration error".
        public const int ConfigErrorExitCode = 78;

        /// <returns>true when the app may start, false when it must not.</returns>
        public static bool Run()
        {
            var problems = new List<string>();

            var openbaoEnabled = IsTrue(Get("OPENBAO__ENABLED")) || !string.IsNullOrWhiteSpace(Get("OPENBAO__ADDR"));

            Console.WriteLine("========================================================================");
            Console.WriteLine(" kafka-dedup — startup configuration check");
            Console.WriteLine("========================================================================");

            // --- OpenBao -----------------------------------------------------------------
            if (openbaoEnabled)
            {
                var addr = Get("OPENBAO__ADDR");
                Console.WriteLine($"OpenBao : enabled  addr={addr}  role={Get("OPENBAO__ROLE")}  path={Get("OPENBAO__MOUNT") ?? "kv"}/{Get("OPENBAO__PATH")}");
                if (addr != null && addr.StartsWith("https://"))
                    CheckFile("OpenBao CA (OPENBAO__CACERT)", Get("OPENBAO__CACERT"), required: true, problems,
                        howto: "OpenBao on talos uses a private CA. Set OPENBAO__CACERT and mount the openbao-ca ConfigMap (chart already does this on talos).");
            }
            else
            {
                Console.WriteLine("OpenBao : disabled — all configuration must come from environment variables (development mode).");
            }

            // --- Required runtime configuration ------------------------------------------
            Require("SOURCE_TOPIC", problems, "the Kafka topic to read from, e.g. sky-bazaar-raw");
            Require("TARGET_TOPIC", problems, "the Kafka topic to write deduplicated messages to, e.g. sky-bazaar");
            Require("REDIS_HOST", problems, "the Redis used for the dedup window, e.g. localhost (a sidecar runs in-pod)");
            Require("KAFKA__BROKERS", problems, "comma-separated Kafka brokers, e.g. host1:9092,host2:9092", secretBacked: true);

            // --- Kafka authentication / transport ----------------------------------------
            var user = Get("KAFKA__USERNAME");
            var hasKey = !string.IsNullOrWhiteSpace(Get("KAFKA__TLS__KEY_LOCATION"));
            string protocol;
            if (!string.IsNullOrWhiteSpace(user))
            {
                protocol = hasKey ? "SaslSsl" : "SaslPlaintext";
                if (string.IsNullOrWhiteSpace(Get("KAFKA__PASSWORD")))
                    problems.Add(Problem("KAFKA__PASSWORD",
                        "KAFKA__USERNAME is set (SASL auth) but KAFKA__PASSWORD is empty.", secretBacked: true));
            }
            else
            {
                protocol = hasKey ? "Ssl" : "Plaintext";
            }
            Console.WriteLine($"Kafka   : brokers={Mask(Get("KAFKA__BROKERS"))}  user={user ?? "(none)"}  security={protocol}");

            // TLS material is read from files by librdkafka; verify the files actually exist.
            CheckFile("Kafka TLS CA   (KAFKA__TLS__CA_LOCATION)", Get("KAFKA__TLS__CA_LOCATION"), required: hasKey, problems,
                howto: "Mount the redpanda ConfigMap at /tls/cm (ca.crt).");
            CheckFile("Kafka TLS cert (KAFKA__TLS__CERTIFICATE_LOCATION)", Get("KAFKA__TLS__CERTIFICATE_LOCATION"), required: hasKey, problems,
                howto: "Mount the redpanda ConfigMap at /tls/cm (skyblock.crt).");
            CheckFile("Kafka TLS key  (KAFKA__TLS__KEY_LOCATION)", Get("KAFKA__TLS__KEY_LOCATION"), required: false, problems,
                howto: "Mount the redpanda Secret at /tls/secret (skyblock.key).");

            // --- Informational -----------------------------------------------------------
            Console.WriteLine($"Topics  : {Get("SOURCE_TOPIC") ?? "?"} -> {Get("TARGET_TOPIC") ?? "?"}  (batch {Get("BATCH_SIZE") ?? "50"}, group {Get("GROUP_ID") ?? "deduper"})");
            Console.WriteLine($"Redis   : {Get("REDIS_HOST") ?? "?"}");
            Console.WriteLine("========================================================================");

            if (problems.Count == 0)
            {
                Console.WriteLine("✓ configuration OK — starting.");
                Console.WriteLine("========================================================================");
                return true;
            }

            Console.Error.WriteLine($"✗ MISCONFIGURED — refusing to start. {problems.Count} problem(s):");
            foreach (var p in problems)
                Console.Error.WriteLine(p);
            Console.Error.WriteLine("------------------------------------------------------------------------");
            Console.Error.WriteLine("In development: export the missing variables before running.");
            Console.Error.WriteLine("In the cluster: store them in OpenBao at the path shown above (OPENBAO__PATH);");
            Console.Error.WriteLine("                if values are still missing, the pod's OpenBao login failed —");
            Console.Error.WriteLine("                check the [OpenBao] log lines above for the reason.");
            Console.Error.WriteLine("========================================================================");
            return false;
        }

        private static void Require(string key, List<string> problems, string description, bool secretBacked = false)
        {
            if (string.IsNullOrWhiteSpace(Get(key)))
                problems.Add(Problem(key, $"{key} is not set — {description}.", secretBacked));
        }

        private static void CheckFile(string label, string path, bool required, List<string> problems, string howto)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                if (required)
                    problems.Add($"  • {label} is not set. {howto}");
                return;
            }
            if (File.Exists(path))
                Console.WriteLine($"  ✓ {label}: {path}");
            else
                problems.Add($"  • {label} points at '{path}' but no file exists there. {howto}");
        }

        private static string Problem(string key, string message, bool secretBacked)
        {
            var fix = secretBacked
                ? $"      → dev:  export {key}=...\n      → prod: store {key} in OpenBao at {Get("OPENBAO__MOUNT") ?? "kv"}/{Get("OPENBAO__PATH") ?? "sky/kafka-dedup"}"
                : $"      → set {key} (chart value / env var)";
            return $"  • {message}\n{fix}";
        }

        private static string Mask(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "(unset)";
            // brokers are not secret, but keep output compact
            var hosts = value.Split(',', StringSplitOptions.RemoveEmptyEntries);
            return hosts.Length <= 1 ? value : $"{hosts[0]} (+{hosts.Length - 1} more)";
        }

        private static string Get(string key) => Environment.GetEnvironmentVariable(key);
        private static bool IsTrue(string value) => bool.TryParse(value, out var parsed) && parsed;
    }
}
