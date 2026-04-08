/*
 * Lean Algorithmic Trading Engine - Fidelity Brokerage Factory
 */

using System;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Brokerages.Fidelity
{
    /// <summary>
    /// Factory for creating <see cref="FidelityBrokerage"/> instances.
    ///
    /// Configuration (environment variables or lean config):
    ///   fidelity-sidecar-url   - URL of the Python sidecar (default: http://127.0.0.1:5198)
    ///   fidelity-account       - Fidelity account number (Z-prefixed for BrokerageLink)
    ///   fidelity-account-type  - Optional override: Cash or Margin
    ///   fidelity-http-timeout-seconds - Optional sidecar HTTP timeout override (default: 120)
    /// </summary>
    public class FidelityBrokerageFactory : BrokerageFactory
    {
        /// <summary>
        /// Creates a new instance of FidelityBrokerageFactory
        /// </summary>
        public FidelityBrokerageFactory()
            : base(typeof(FidelityBrokerage))
        {
        }

        /// <summary>
        /// Gets the brokerage data required to run the Fidelity brokerage
        /// </summary>
        public override Dictionary<string, string> BrokerageData
        {
            get
            {
                return new Dictionary<string, string>
                {
                    { "fidelity-sidecar-url", Config.Get("fidelity-sidecar-url", "http://127.0.0.1:5198") },
                    { "fidelity-account", Config.Get("fidelity-account", "") },
                    { "fidelity-account-type", Config.Get("fidelity-account-type", "") },
                    { "fidelity-http-timeout-seconds", Config.Get("fidelity-http-timeout-seconds", "120") }
                };
            }
        }

        /// <summary>
        /// Gets the Fidelity brokerage model
        /// </summary>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            var account = Config.Get("fidelity-account", "");
            var configuredAccountType = Config.Get("fidelity-account-type", "");
            return new FidelityBrokerageModel(GetAccountType(account, configuredAccountType));
        }

        /// <summary>
        /// Creates a new FidelityBrokerage instance
        /// </summary>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var sidecarUrl = Read<string>(job.BrokerageData, "fidelity-sidecar-url", errors);
            var account = Read<string>(job.BrokerageData, "fidelity-account", errors);
            var httpTimeoutSeconds = Read<int>(job.BrokerageData, "fidelity-http-timeout-seconds", errors);

            if (string.IsNullOrWhiteSpace(sidecarUrl))
            {
                sidecarUrl = "http://127.0.0.1:5198";
            }

            if (httpTimeoutSeconds <= 0)
            {
                httpTimeoutSeconds = 120;
            }

            if (errors.Count > 0)
            {
                throw new InvalidOperationException(
                    "FidelityBrokerageFactory: " + string.Join("; ", errors));
            }

            return new FidelityBrokerage(algorithm, sidecarUrl, account, httpTimeoutSeconds);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public override void Dispose()
        {
        }

        private static AccountType GetAccountType(string account, string configuredAccountType)
        {
            if (!string.IsNullOrWhiteSpace(configuredAccountType)
                && Enum.TryParse(configuredAccountType, true, out AccountType accountType))
            {
                return accountType;
            }

            return IsBrokerageLinkAccount(account) ? AccountType.Cash : AccountType.Margin;
        }

        private static bool IsBrokerageLinkAccount(string account)
        {
            return !string.IsNullOrWhiteSpace(account)
                && account.Trim().StartsWith("Z", StringComparison.OrdinalIgnoreCase);
        }
    }
}
