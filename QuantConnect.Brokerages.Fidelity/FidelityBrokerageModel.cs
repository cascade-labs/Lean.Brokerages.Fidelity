/*
 * Lean Algorithmic Trading Engine - Fidelity Brokerage Model
 *
 * Brokerage model for Fidelity BrokerageLink / 401k accounts.
 * Supports equity/ETF trading with Market and Limit orders.
 */

using System;
using System.Collections.Generic;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Brokerages.Fidelity
{
    /// <summary>
    /// Brokerage model for Fidelity, supporting equity/ETF trading
    /// via BrokerageLink (401k) and standard brokerage accounts.
    /// Fidelity charges $0 commission on US stock and ETF trades.
    /// </summary>
    public class FidelityBrokerageModel : DefaultBrokerageModel
    {
        private readonly HashSet<OrderType> _supportedOrderTypes = new HashSet<OrderType>
        {
            OrderType.Market,
            OrderType.Limit
        };

        private readonly Type[] _supportedTimeInForces =
        {
            typeof(DayTimeInForce),
            typeof(GoodTilCanceledTimeInForce)
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="FidelityBrokerageModel"/> class.
        /// BrokerageLink / 401k accounts are cash accounts.
        /// </summary>
        /// <param name="accountType">The account type (default Cash for BrokerageLink)</param>
        public FidelityBrokerageModel(AccountType accountType = AccountType.Cash) : base(accountType)
        {
        }

        /// <summary>
        /// Gets a map of the default markets to be used for each security type
        /// </summary>
        public override IReadOnlyDictionary<SecurityType, string> DefaultMarkets { get; } = GetDefaultMarkets();

        /// <summary>
        /// Returns true if the brokerage could accept this order.
        /// Fidelity supports Market and Limit orders for US equities only.
        /// </summary>
        public override bool CanSubmitOrder(Security security, Order order, out BrokerageMessageEvent message)
        {
            message = null;

            // Fidelity BrokerageLink: equities/ETFs only
            if (security.Type != SecurityType.Equity)
            {
                message = new BrokerageMessageEvent(BrokerageMessageType.Warning, "NotSupported",
                    Messages.DefaultBrokerageModel.UnsupportedSecurityType(this, security));
                return false;
            }

            if (!_supportedOrderTypes.Contains(order.Type))
            {
                message = new BrokerageMessageEvent(BrokerageMessageType.Warning, "NotSupported",
                    Messages.DefaultBrokerageModel.UnsupportedOrderType(this, order, _supportedOrderTypes));
                return false;
            }

            if (!Array.Exists(_supportedTimeInForces, t => t == order.TimeInForce.GetType()))
            {
                message = new BrokerageMessageEvent(BrokerageMessageType.Warning, "NotSupported",
                    Messages.DefaultBrokerageModel.UnsupportedTimeInForce(this, order));
                return false;
            }

            return true;
        }

        /// <summary>
        /// Returns true if the brokerage can execute this order at this time
        /// </summary>
        public override bool CanExecuteOrder(Security security, Order order)
        {
            if (security.Type != SecurityType.Equity)
            {
                return false;
            }

            if (!Array.Exists(_supportedTimeInForces, t => t == order.TimeInForce.GetType()))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Returns true if the brokerage would allow updating the order
        /// </summary>
        public override bool CanUpdateOrder(Security security, Order order, UpdateOrderRequest request, out BrokerageMessageEvent message)
        {
            // Fidelity web automation doesn't support order modifications
            message = new BrokerageMessageEvent(BrokerageMessageType.Warning, "NotSupported",
                "Fidelity brokerage does not support order updates. Cancel and re-place instead.");
            return false;
        }

        /// <summary>
        /// Fidelity: no leverage for BrokerageLink/401k (cash account)
        /// </summary>
        public override decimal GetLeverage(Security security)
        {
            return 1m;
        }

        /// <summary>
        /// Fidelity charges $0 commission on US stock and ETF trades
        /// </summary>
        public override IFeeModel GetFeeModel(Security security)
        {
            return new ConstantFeeModel(0m);
        }

        private static IReadOnlyDictionary<SecurityType, string> GetDefaultMarkets()
        {
            var map = new Dictionary<SecurityType, string>(DefaultMarketMap);
            map[SecurityType.Equity] = Market.USA;
            return map.ToReadOnlyDictionary();
        }
    }
}
