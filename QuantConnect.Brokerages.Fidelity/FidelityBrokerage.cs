/*
 * Lean Algorithmic Trading Engine - Fidelity Brokerage Integration
 *
 * Connects to the Fidelity Python sidecar (fidelity_sidecar.py), which owns
 * Fidelity authentication and exposes a local HTTP trading interface.
 *
 * Supports BrokerageLink (401k) and standard brokerage accounts.
 * Equity/ETF trading only, Market and Limit orders, $0 commissions.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Securities;

namespace QuantConnect.Brokerages.Fidelity
{
    /// <summary>
    /// Fidelity brokerage implementation that communicates with a Python sidecar
    /// service that owns Fidelity authentication and trading APIs.
    /// </summary>
    [BrokerageFactory(typeof(FidelityBrokerageFactory))]
    public class FidelityBrokerage : Brokerage
    {
        private static readonly TimeSpan DefaultLiveMarketOrderFillTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan MarketOrderFillTimeoutBuffer = TimeSpan.FromSeconds(5);

        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly string _account;
        private readonly int _httpTimeoutSeconds;
        private readonly IAlgorithm _algorithm;
        private bool _connected;

        /// <summary>
        /// Creates a new FidelityBrokerage instance
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        /// <param name="sidecarUrl">Base URL of the Python sidecar (default: http://127.0.0.1:5198)</param>
        /// <param name="account">Fidelity account number to trade in</param>
        /// <param name="httpTimeoutSeconds">HTTP timeout used for sidecar requests</param>
        public FidelityBrokerage(IAlgorithm algorithm, string sidecarUrl, string account, int httpTimeoutSeconds)
            : base("Fidelity")
        {
            _algorithm = algorithm;
            _baseUrl = sidecarUrl.TrimEnd('/');
            _account = account;
            _httpTimeoutSeconds = httpTimeoutSeconds > 0 ? httpTimeoutSeconds : 120;
            _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(_httpTimeoutSeconds) };

            ConfigureLiveMarketOrderFillTimeout();
        }

        /// <summary>
        /// Returns true if the sidecar is connected and logged in
        /// </summary>
        public override bool IsConnected => _connected;

        /// <summary>
        /// USD-based account
        /// </summary>
        public override string AccountBaseCurrency => Currencies.USD;

        /// <summary>
        /// Connects to Fidelity via the sidecar.
        /// </summary>
        public override void Connect()
        {
            ConfigureLiveMarketOrderFillTimeout();

            Log.Trace("FidelityBrokerage.Connect(): Connecting to sidecar...");
            var authState = JObject.Parse(Get("/auth-state"));
            var authenticated = authState["authenticated"]?.Value<bool>() ?? false;
            if (!authenticated)
            {
                var response = Post("/reauth", new
                {
                    account = string.IsNullOrWhiteSpace(_account) ? null : _account
                });
                authState = JObject.Parse(response);
                authenticated = authState["authenticated"]?.Value<bool>() ?? false;
            }
            if (!authenticated)
            {
                throw new InvalidOperationException(
                    $"FidelityBrokerage: Sidecar is not authenticated: {authState}");
            }
            if (!string.IsNullOrWhiteSpace(_account))
            {
                var accounts = JArray.Parse(Get(BuildAccountScopedPath("/accounts")));
                if (accounts.Count == 0)
                {
                    throw new InvalidOperationException(
                        $"FidelityBrokerage: Sidecar could not access account '{_account}'.");
                }
            }
            _connected = true;
            Log.Trace("FidelityBrokerage.Connect(): Connected successfully");
        }

        /// <summary>
        /// Disconnects from the sidecar.
        /// </summary>
        public override void Disconnect()
        {
            _connected = false;
        }

        /// <summary>
        /// Places an order via the Fidelity sidecar
        /// </summary>
        public override bool PlaceOrder(Order order)
        {
            Log.Trace($"FidelityBrokerage.PlaceOrder(): {order}");

            var action = order.Quantity > 0 ? "buy" : "sell";
            var quantity = Math.Abs(order.Quantity);

            decimal? limitPrice = null;
            if (order is LimitOrder limitOrder)
            {
                limitPrice = limitOrder.LimitPrice;
            }

            try
            {
                EnsureConnectedSession();

                var payload = new
                {
                    symbol = order.Symbol.Value,
                    quantity = (double)quantity,
                    action,
                    account = _account,
                    dry_run = false,
                    limit_price = limitPrice.HasValue ? (double?)((double)limitPrice.Value) : null,
                    time_in_force = MapTimeInForce(order.TimeInForce)
                };

                var response = Post("/order", payload);
                var json = JObject.Parse(response);
                var success = json["success"]?.Value<bool>() ?? false;
                var message = json["message"]?.Value<string>() ?? "";
                var orderId = json["order_id"]?.Value<string>() ?? $"FID-{DateTime.UtcNow.Ticks}";

                if (success)
                {
                    order.BrokerId.Add(orderId);

                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = OrderStatus.Submitted,
                        Message = message
                    });

                    // Fidelity sidecar doesn't give real-time fill callbacks,
                    // so we assume market orders fill immediately
                    if (order.Type == OrderType.Market)
                    {
                        var fillPrice = order.Symbol.HasUnderlying
                            ? 0m
                            : GetLastPrice(order.Symbol);

                        OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                        {
                            Status = OrderStatus.Filled,
                            FillQuantity = order.Quantity,
                            FillPrice = fillPrice,
                            Message = "Filled via Fidelity sidecar"
                        });
                    }
                    return true;
                }
                else
                {
                    OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                    {
                        Status = OrderStatus.Invalid,
                        Message = $"Fidelity order rejected: {message}"
                    });
                    return false;
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "FidelityBrokerage.PlaceOrder()");
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.Invalid,
                    Message = $"Fidelity order error: {e.Message}"
                });
                return false;
            }
        }

        /// <summary>
        /// Order updates are not supported by Fidelity.
        /// </summary>
        public override bool UpdateOrder(Order order)
        {
            OnMessage(new BrokerageMessageEvent(
                BrokerageMessageType.Warning, "NotSupported",
                "Fidelity does not support order updates. Cancel and re-place instead."));
            return false;
        }

        /// <summary>
        /// Cancels an order via the Fidelity sidecar.
        /// </summary>
        public override bool CancelOrder(Order order)
        {
            Log.Trace($"FidelityBrokerage.CancelOrder(): {order}");

            if (order.BrokerId == null || order.BrokerId.Count == 0)
            {
                Log.Trace("FidelityBrokerage.CancelOrder(): Unable to cancel order without BrokerId.");
                return false;
            }

            try
            {
                EnsureConnectedSession();

                var response = Post("/cancel", new
                {
                    order_id = order.BrokerId.First(),
                    account = string.IsNullOrWhiteSpace(_account) ? null : _account
                });
                var json = JObject.Parse(response);
                if (json["success"]?.Value<bool>() != true)
                {
                    OnMessage(new BrokerageMessageEvent(
                        BrokerageMessageType.Warning,
                        "CancelFailed",
                        json["message"]?.Value<string>() ?? "Fidelity order cancellation failed."));
                    return false;
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.Canceled,
                    Message = json["message"]?.Value<string>() ?? $"Fidelity order {order.BrokerId.First()} cancelled."
                });
                return true;
            }
            catch (Exception exception)
            {
                Log.Error(exception, "FidelityBrokerage.CancelOrder()");
                OnMessage(new BrokerageMessageEvent(
                    BrokerageMessageType.Error,
                    "CancelFailed",
                    $"Fidelity order cancellation failed: {exception.Message}"));
                return false;
            }
        }

        /// <summary>
        /// Gets open Fidelity orders from the sidecar.
        /// </summary>
        public override List<Order> GetOpenOrders()
        {
            try
            {
                EnsureConnectedSession();
                var response = Get(BuildAccountScopedPath("/orders/open"));
                var orders = JArray.Parse(response);
                var result = new List<Order>();

                foreach (var item in orders)
                {
                    var order = ConvertToLeanOrder(item);
                    if (order != null)
                    {
                        result.Add(order);
                    }
                }

                return result;
            }
            catch (Exception exception)
            {
                Log.Error(exception, "FidelityBrokerage.GetOpenOrders()");
                return new List<Order>();
            }
        }

        /// <summary>
        /// Gets account holdings from the Fidelity sidecar
        /// </summary>
        public override List<Holding> GetAccountHoldings()
        {
            try
            {
                EnsureConnectedSession();
                var response = Get(BuildAccountScopedPath("/holdings"));
                var items = JArray.Parse(response);
                var holdings = new List<Holding>();

                foreach (var item in items)
                {
                    var symbol = item["symbol"]?.Value<string>();
                    if (string.IsNullOrEmpty(symbol))
                        continue;

                    // Skip money market / core positions (they are cash)
                    var upper = NormalizeTicker(symbol);
                    if (upper == "SPAXX" || upper == "FDRXX" || upper == "FCASH"
                        || upper == "FZFXX" || upper == "SPRXX"
                        || upper.Contains("PENDING"))
                        continue;

                    var qty = item["quantity"]?.Value<decimal>() ?? 0;
                    var price = item["last_price"]?.Value<decimal>() ?? 0;

                    holdings.Add(new Holding
                    {
                        Symbol = Symbol.Create(symbol, SecurityType.Equity, Market.USA),
                        Quantity = qty,
                        AveragePrice = price,
                        MarketPrice = price,
                        CurrencySymbol = "$"
                    });
                }
                return holdings;
            }
            catch (Exception e)
            {
                Log.Error(e, "FidelityBrokerage.GetAccountHoldings()");
                return new List<Holding>();
            }
        }

        /// <summary>
        /// Gets cash balance from the sidecar
        /// </summary>
        public override List<CashAmount> GetCashBalance()
        {
            try
            {
                EnsureConnectedSession();
                var response = Get(BuildAccountScopedPath("/cash"));
                var items = JArray.Parse(response);
                return items.Select(item => new CashAmount(
                    item["amount"]?.Value<decimal>() ?? 0,
                    item["currency"]?.Value<string>() ?? Currencies.USD
                )).ToList();
            }
            catch (Exception e)
            {
                Log.Error(e, "FidelityBrokerage.GetCashBalance()");
                return new List<CashAmount>
                {
                    new CashAmount(0, Currencies.USD)
                };
            }
        }

        /// <summary>
        /// No history available via web automation
        /// </summary>
        public override IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            return Enumerable.Empty<BaseData>();
        }

        /// <summary>
        /// Clean up HTTP client
        /// </summary>
        public override void Dispose()
        {
            Disconnect();
            _httpClient?.Dispose();
            base.Dispose();
        }

        // ---------------------------------------------------------------
        // HTTP helpers
        // ---------------------------------------------------------------

        private string Get(string path)
        {
            var url = _baseUrl + path;
            var result = _httpClient.GetAsync(url).GetAwaiter().GetResult();
            var body = result.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            if (!result.IsSuccessStatusCode)
            {
                throw new HttpRequestException(
                    $"Fidelity sidecar GET {path} returned {result.StatusCode}: {body}");
            }
            return body;
        }

        private string Post(string path, object payload)
        {
            var url = _baseUrl + path;
            var json = JsonConvert.SerializeObject(payload);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var result = _httpClient.PostAsync(url, content).GetAwaiter().GetResult();
            var body = result.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            if (!result.IsSuccessStatusCode)
            {
                throw new HttpRequestException(
                    $"Fidelity sidecar POST {path} returned {result.StatusCode}: {body}");
            }
            return body;
        }

        private void EnsureConnectedSession()
        {
            if (_connected)
            {
                return;
            }

            Connect();
        }

        private string BuildAccountScopedPath(string path)
        {
            if (string.IsNullOrWhiteSpace(_account))
            {
                return path;
            }

            var separator = path.Contains("?") ? "&" : "?";
            return $"{path}{separator}account={Uri.EscapeDataString(_account)}";
        }

        private static string NormalizeTicker(string symbol)
        {
            if (string.IsNullOrEmpty(symbol))
            {
                return string.Empty;
            }

            return new string(symbol
                .ToUpperInvariant()
                .Where(char.IsLetterOrDigit)
                .ToArray());
        }

        private Order ConvertToLeanOrder(JToken token)
        {
            if (token == null)
            {
                return null;
            }

            var orderId = token["order_id"]?.Value<string>();
            var symbol = token["symbol"]?.Value<string>();
            var side = token["side"]?.Value<string>();
            var orderType = token["order_type"]?.Value<string>();
            var tif = token["time_in_force"]?.Value<string>();
            var quantity = token["quantity"]?.Value<decimal>() ?? 0m;
            var limitPrice = token["limit_price"]?.Value<decimal?>();

            if (string.IsNullOrWhiteSpace(orderId)
                || string.IsNullOrWhiteSpace(symbol)
                || quantity <= 0m)
            {
                return null;
            }

            var leanSymbol = Symbol.Create(symbol, SecurityType.Equity, Market.USA);
            var signedQuantity = string.Equals(side, "sell", StringComparison.OrdinalIgnoreCase)
                ? -quantity
                : quantity;
            var timeInForce = string.Equals(tif, "gtc", StringComparison.OrdinalIgnoreCase)
                ? (TimeInForce)new GoodTilCanceledTimeInForce()
                : new DayTimeInForce();
            var properties = new OrderProperties
            {
                TimeInForce = timeInForce
            };

            Order order;
            if (string.Equals(orderType, "limit", StringComparison.OrdinalIgnoreCase) && limitPrice.HasValue)
            {
                order = new LimitOrder(leanSymbol, signedQuantity, limitPrice.Value, DateTime.UtcNow, null, properties);
            }
            else
            {
                order = new MarketOrder(leanSymbol, signedQuantity, DateTime.UtcNow, null, properties);
            }

            order.BrokerId.Add(orderId);
            return order;
        }

        private static string MapTimeInForce(TimeInForce timeInForce)
        {
            switch (timeInForce)
            {
                case GoodTilCanceledTimeInForce:
                    return "gtc";
                default:
                    return "day";
            }
        }

        private void ConfigureLiveMarketOrderFillTimeout()
        {
            if (_algorithm?.LiveMode != true || _algorithm.Transactions == null)
            {
                return;
            }

            var currentTimeout = _algorithm.Transactions.MarketOrderFillTimeout;
            if (currentTimeout != DefaultLiveMarketOrderFillTimeout
                && currentTimeout != TimeSpan.MinValue)
            {
                return;
            }

            var configuredTimeout = TimeSpan.FromSeconds(_httpTimeoutSeconds) + MarketOrderFillTimeoutBuffer;
            _algorithm.Transactions.MarketOrderFillTimeout = configuredTimeout;

            Log.Trace($"FidelityBrokerage.ConfigureLiveMarketOrderFillTimeout(): MarketOrderFillTimeout set to {configuredTimeout.TotalSeconds:0} seconds to accommodate sidecar latency.");
        }

        private decimal GetLastPrice(Symbol symbol)
        {
            try
            {
                if (_algorithm?.Securities != null &&
                    _algorithm.Securities.TryGetValue(symbol, out var security))
                {
                    return security.Price;
                }
            }
            catch
            {
                // Swallow - best effort
            }
            return 0m;
        }
    }
}
