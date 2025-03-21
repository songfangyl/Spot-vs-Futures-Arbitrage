"""
okx_trader.py

A Python module for managing trading operations on OKX through CCXT,
including account management, order execution, and synchronization.
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import pandas as pd
import ccxt

# Load environment variables
load_dotenv()

# Import SAFE_MARGIN from config
from config import SAFE_MARGIN, IS_SIMULATION, COIN_LIST
from logger_config import setup_logger

# Configure logging
logger = setup_logger(__name__)

class OKXTrader:
    """
    OKXTrader handles all trading-related operations, including account management,
    order placement, cancellation, and synchronization with the OKX exchange via CCXT.
    """

    def __init__(self):
        """
        Initializes the trader instance with authentication credentials and sets up internal attributes.
        Loads credentials from environment variables if not provided explicitly.
        """

        # Load credentials from environment if not provided
        self.api_key = os.getenv("OKX_API_KEY")
        self.api_secret = os.getenv("OKX_API_SECRET_KEY")
        self.passphrase = os.getenv("OKX_API_PASSPHRASE")
        logger.info(f"API Key: {self.api_key}")
        logger.info(f"API Secret: {self.api_secret}")
        logger.info(f"Passphrase: {self.passphrase}")

        

        # Initialize CCXT OKX exchange instance
        # For demonstration, we override the default OKX base URLs with https://my.okx.com/
        self.exchange = ccxt.myokx({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'password': self.passphrase,  # OKX passphrase -> 'password' in CCXT
            'enableRateLimit': True,
            'urls': {
                'api': {
                    'public': 'https://my.okx.com',
                    'private': 'https://my.okx.com',
                }
            }
        })
        # Initialize balance, holdings, and active_orders
        self.balance = self.get_account_balance()
        self.active_orders = self.get_open_orders()

        # Log initialization
        logger.info("OKXTrader initialized with API credentials.")

    def get_account_balance(self):
        """
        Retrieves and returns the current account balance.
        Updates self.balance and self.holdings.
        """
        logger.info("Fetching account balance...")
        try:
            balance_info = self.exchange.fetch_balance()
            # For spot/cash trading, 'total' may contain asset-by-asset balances
            total_balances = balance_info.get('total', {})
            self.holdings = total_balances
            # Some users treat the 'USD' or 'USDT' as the main quote currency
            # Depending on your usage, you might sum up the total in your preferred currency
            # For simplicity, let's just store the total USDT or USD balance
            self.balance = total_balances.get('USDT', 0.0) or total_balances.get('USD', 0.0)
            logger.info(f"Account balance fetched: {self.balance}")
            return self.balance
        except Exception as e:
            logger.error(f"Error fetching account balance: {e}")
            return None

    def get_open_orders(self, start_date: str = None, end_date: str = None):
        """
        Retrieve all open orders in the given date range.
        """
        logger.info("Fetching open orders...")
        try:
            since = None
            if start_date:
                since = self.exchange.parse8601(start_date)

            open_orders = self.exchange.fetchOpenOrders(symbol=None, since=since)
            # If needed, we can filter by end_date manually
            if end_date:
                end_timestamp = self.exchange.parse8601(end_date)
                open_orders = [order for order in open_orders if order['timestamp'] <= end_timestamp]

            return open_orders
        except Exception as e:
            logger.error(f"Error fetching open orders: {e}")
            return []

    def get_closed_orders(self, start_date: str = None, end_date: str = None):
        """
        Retrieve all closed orders in the given date range.
        """
        logger.info("Fetching closed orders...")
        try:
            since = None
            if start_date:
                since = self.exchange.parse8601(start_date)

            closed_orders = self.exchange.fetchClosedOrders(symbol=None, since=since)
            # If needed, filter by end_date
            if end_date:
                end_timestamp = self.exchange.parse8601(end_date)
                closed_orders = [order for order in closed_orders if order['timestamp'] <= end_timestamp]

            return closed_orders
        except Exception as e:
            logger.error(f"Error fetching closed orders: {e}")
            return []

    def get_last_closed_order(self, instrument_id: str, start_date: str = None, end_date: str = None):
        """
        Get the last closed order for a specific instrument.
        """
        logger.info(f"Fetching last closed order for instrument {instrument_id}...")
        try:
            since = None
            if start_date:
                since = self.exchange.parse8601(start_date)

            closed_orders = self.exchange.fetchClosedOrders(symbol=instrument_id, since=since)
            # Filter by end_date if provided
            if end_date:
                end_timestamp = self.exchange.parse8601(end_date)
                closed_orders = [order for order in closed_orders if order['timestamp'] <= end_timestamp]

            # Sort by timestamp descending
            closed_orders = sorted(closed_orders, key=lambda x: x['timestamp'], reverse=True)
            return closed_orders[0] if closed_orders else None
        except Exception as e:
            logger.error(f"Error fetching last closed order for {instrument_id}: {e}")
            return None

    def get_past_orders(self, start_date: str, end_date: str):
        """
        Retrieves order history (open + closed) within the specified date range.
        (Combines fetchOpenOrders and fetchClosedOrders for demonstration.)
        """
        logger.info("Fetching past orders (open + closed)...")
        try:
            open_orders = self.get_open_orders(start_date, end_date)
            closed_orders = self.get_closed_orders(start_date, end_date)
            return {
                "open_orders": open_orders,
                "closed_orders": closed_orders
            }
        except Exception as e:
            logger.error(f"Error fetching past orders: {e}")
            return {}

    def _internal_place_order(self, order_type: str, instrument_id: str, quantity: float,
                          price: float = None):
        """
        Places a basic limit (or market) order on OKX (through CCXT).
        If order_type = 'BUY' or 'SELL', it determines the side.
        """
        if IS_SIMULATION:
            print(f"[SIMULATION] calling OKXTrader.place_limit_order('BUY', {instrument_id}, {quantity}, {price})")
            return None
        #logger.info("Placing limit order...")
        try:
            side = 'buy' if order_type.lower() == 'buy' else 'sell'
            quantity = float(self.exchange.amount_to_precision(instrument_id, quantity))
            if price:
                price = float(self.exchange.price_to_precision(instrument_id, price))
            #print(f"Placing order with: {instrument_id}, {quantity}, {side}, {price}")
            # Check if we have enough balance (minus SAFE_MARGIN) - simplistic check
            if self.balance < (price * quantity if price else 0) + SAFE_MARGIN:
                raise ValueError("Not enough balance to place this order.")

            # Construct CCXT order params
            order_type_ccxt = 'limit' if price else 'market'
            print(f"Placing order with: {instrument_id}, {quantity}, {side}, {order_type_ccxt}, {price}")
            order = self.exchange.create_order(
                symbol=instrument_id,
                type=order_type_ccxt,
                side=side,
                amount=quantity,
                price=price if price else None,
                params={}
            )
            # Update active orders and log
            self.active_orders.append(order)
            logger.info(f"Placed limit order: {order}")
            return order
        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return None

    def place_limit_order(self, order_type: str, instrument_id: str, quantity: float, price: float):
        """
        Place a limit order.
        """
        logger.info("Placing limit order...")
        time.sleep(0.1)

        return self._internal_place_order(order_type, instrument_id, quantity, price)
    
    def place_market_order(self, side: str, symbol: str, size: float) -> dict:
        """
        Places a market order.
        
        Args:
            side: 'BUY' or 'SELL'
            symbol: Trading pair (e.g., 'BTC-USDT')
            size: Amount to buy/sell in base currency units
        
        Returns:
            Order details dictionary
        """
        logger.info("Placing market order...")
        
        try:
            # Ensure size is a positive number
            if size is None or size <= 0:
                logger.error(f"Invalid size for market order: {size}")
                return {'error': 'Invalid size', 'filled': 0, 'price': 0}
            
            # Format size to appropriate precision
            size = float(f"{size:.6f}")
            
            # Get current price for logging purposes
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker['last'] if ticker and 'last' in ticker else None
            
            if current_price is None:
                logger.warning(f"Could not fetch current price for {symbol}")
                # We can still proceed with the market order
            
            # Place the order
            side = side.lower()  # CCXT expects lowercase
            order = self.exchange.create_market_order(symbol, side, size)
            
            # Process the response
            order_id = order.get('id')
            
            # For market orders, we may need to fetch the filled details
            filled_order = self.exchange.fetch_order(order_id, symbol)
            time.sleep(0.1)
            
            # Extract relevant information
            result = {
                'id': order_id,
                'symbol': symbol,
                'side': side,
                'type': 'market',
                'filled': float(filled_order.get('filled', 0)),
                'price': float(filled_order.get('price', current_price or 0)),
                'timestamp': filled_order.get('timestamp', int(datetime.now().timestamp() * 1000))
            }
            
            # Add to active orders if it's not fully filled
            if filled_order.get('status') != 'closed':
                self.active_orders.append(result)
            
            logger.info(f"Market {side.upper()} order placed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            # Return a minimal response that won't cause downstream errors
            return {'error': str(e), 'filled': 0, 'price': 0}

    def place_stop_loss_order(self, order_type: str, instrument_id: str, quantity: float,
                              slTriggerPx: float = None):
        """
        Place a stop loss order. If slTriggerPx is None, no SL is placed.
        """
        if IS_SIMULATION:
            print(f"[SIMULATION] calling OKXTrader.place_stop_loss_order('SELL', {instrument_id}, {quantity}, {slTriggerPx})")
            return None
        logger.info("Placing stop loss order...")
        try:
            quantity = float(self.exchange.amount_to_precision(instrument_id, quantity))
            if slTriggerPx is None:
                logger.warning("No slTriggerPx provided; not placing a stop loss.")
                return None

            side = 'buy' if order_type.lower() == 'buy' else 'sell'
            if side != 'sell':
                print(f"as we only long, thus we will only sell to place a stop loss")
                raise ValueError("Stop loss order must be a sell order.")
            params = {
                "tdMode": "cash",
                "stopPx": slTriggerPx
            }
            slOrdPrice = float(self.exchange.price_to_precision(instrument_id, slTriggerPx*0.999))
            params["px"] = slOrdPrice

            # We'll place a "dummy" limit order with trigger parameters
            print(f"Placing stop loss order with: {params}")
            order = self.exchange.create_order(
                symbol=instrument_id,
                type='limit',
                side=side,
                amount=quantity,
                price=slOrdPrice if slOrdPrice else slTriggerPx,  # fallback
                params=params
            )
            self.active_orders.append(order)
            logger.info(f"Placed stop loss order: {order}")
            return order
        except Exception as e:
            logger.error(f"Error placing stop loss order: {e}")
            return None

    def place_take_profit_order(self, order_type: str, instrument_id: str, quantity: float,
                                tpTriggerPx: float = None):
        """
        Place a take profit order. If tpTriggerPx is None, no TP is placed.
        """
        if IS_SIMULATION:
            print(f"[SIMULATION] calling OKXTrader.place_take_profit_order('SELL', {instrument_id}, {quantity}, {tpTriggerPx})")
            return None
        logger.info("Placing take profit order...")
        try:
            quantity = float(self.exchange.amount_to_precision(instrument_id, quantity))
            if tpTriggerPx is None:
                logger.warning("No tpTriggerPx provided; not placing a take profit.")
                return None

            side = 'buy' if order_type.lower() == 'buy' else 'sell'
            if side != 'sell':
                print(f"as we only long, thus we will only sell to place a take profit")
                raise ValueError("Take profit order must be a sell order.")
            params = {
                "tdMode": "cross",
                "tpTriggerPx": str(tpTriggerPx)
            }
            formatted_price = float(self.exchange.price_to_precision(instrument_id, tpTriggerPx*1.001))
            tpOrdPrice = formatted_price
            params["tpOrdPx"] = str(tpOrdPrice)

            print(f"Placing take profit order with: {params}")
            order = self.exchange.create_order(
                symbol=instrument_id,
                type='limit',
                side=side,
                amount=quantity,
                price=tpOrdPrice if tpOrdPrice else tpTriggerPx,  # fallback
                params=params
            )
            self.active_orders.append(order)
            logger.info(f"Placed take profit order: {order}")
            return order
        except Exception as e:
            logger.error(f"Error placing take profit order: {e}")
            return None

    def get_order_status(self, order_id: str, coin: str) -> str:
        """
        Get the status of an order.
        :param order_id: The ID of the order to check
        :param coin: The coin symbol (e.g., "BTC-USDT")
        :return: The status of the order ('open', 'closed', 'canceled', etc.)
        """
        logger.info(f"Fetching order status for order ID {order_id}...")
        try:
            # Convert coin format from "BTC-USDT" to "BTC/USDT" for CCXT
            ccxt_symbol = coin.replace('-', '/')
            order = self.exchange.fetch_order(order_id, ccxt_symbol)
            return order['status']
        except Exception as e:
            logger.error(f"Error fetching order status for {order_id}: {str(e)}")
            return None

    def get_orders_by_date(self, start_date: str, end_date: str, status: str = None):
        """
        Retrieves orders within a date range, optionally filtered by status.
        """
        logger.info(f"Fetching orders by date from {start_date} to {end_date}, status={status}...")
        try:
            since = self.exchange.parse8601(start_date)
            all_orders = []

            # fetchOpenOrders for 'open' or 'active' statuses
            if status in ['open', 'active']:
                orders = self.exchange.fetch_open_orders(symbol=None, since=since)
                all_orders.extend(orders)

            # fetchClosedOrders for 'closed' or any
            if status in [None, 'closed']:
                orders = self.exchange.fetch_closed_orders(symbol=None, since=since)
                all_orders.extend(orders)

            if end_date:
                end_timestamp = self.exchange.parse8601(end_date)
                all_orders = [o for o in all_orders if o['timestamp'] <= end_timestamp]

            return all_orders
        except Exception as e:
            logger.error(f"Error fetching orders by date: {e}")
            return []

    def cancel_order(self, order_id: str):
        """
        Cancels a single order using its order ID.
        """
        time.sleep(0.1)
        logger.info(f"Cancelling order {order_id}...")
        try:
            # Find the order in active_orders to get its symbol
            order_info = next((o for o in self.active_orders if o.get('id') == order_id), None)
            
            if not order_info:
                # If not found in active_orders, try to fetch it from the exchange
                try:
                    # Note: This might fail if the order doesn't exist anymore
                    order_info = self.exchange.fetch_order(order_id)
                except Exception as e:
                    logger.warning(f"Could not fetch order {order_id} details: {e}")
            
            # Extract the symbol from the order info
            symbol = order_info.get('symbol') if order_info else None
            
            if not symbol:
                logger.error(f"Cannot cancel order {order_id}: symbol information not found")
                return None
            
            response = self.exchange.cancel_order(order_id, symbol)
            logger.info(f"Cancelled order {order_id}: {response}")
            # Remove from active_orders
            self.active_orders = [o for o in self.active_orders if o.get('id') != order_id]
            return response
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return None

    def cancel_all_orders(self):
        """
        Cancels all active orders.
        """
        logger.info("Cancelling all active orders...")
        try:
            # fetch open orders from the exchange
            open_orders = self.get_open_orders()
            cancelled_count = 0
            failed_count = 0
            
            for order in open_orders:
                order_id = order['id']
                symbol = order.get('symbol')
                
                if not symbol:
                    logger.info(f"Cannot cancel order {order_id}: missing symbol information")
                    failed_count += 1
                    continue
                
                try:
                    self.exchange.cancel_order(order_id, symbol)
                    cancelled_count += 1
                    print(f"Cancelled order {order_id}")
                except Exception as e:
                    logger.info(f"Failed to cancel order {order_id}: {e}")
                    failed_count += 1

            # Clear local active_orders
            self.active_orders.clear()
            logger.info(f"Cancelled {cancelled_count} orders, failed to cancel {failed_count} orders.")
            
            # Get the open orders again, now should be empty
            open_orders = self.get_open_orders()
            if len(open_orders) == 0:
                logger.info("All active orders have been cancelled.")
            else:
                logger.warning(f"{len(open_orders)} orders still remain active.")
            return True
        except Exception as e:
            logger.info(f"Error cancelling all orders: {e}")
            return False

    def sync_account_info(self):
        """
        Updates internal attributes with current account details, including
        balances, holdings, and active orders.
        """
        logger.info("Synchronizing account info with exchange...")
        try:
            # Fetch latest account data
            balance_info = self.exchange.fetch_balance()
            self.holdings = balance_info.get('total', {})
            self.balance = self.holdings.get('USDT', 0.0) or self.holdings.get('USD', 0.0)

            open_orders = self.get_open_orders()
            self.active_orders = open_orders

            logger.info(f"Synchronized account: balance={self.balance}, holdings={self.holdings}")
        except Exception as e:
            logger.error(f"Error synchronizing account info: {e}")

    def save_to_json(self, filename: str = "okx_account.json"):
        """
        Saves all relevant account information (balance, holdings, active orders) to a JSON file.
        """
        logger.info(f"Saving account info to {filename}...")
        try:
            data = {
                "balance": self.balance,
                "holdings": self.holdings,
                "active_orders": self.active_orders
            }
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Account info saved to {filename}.")
        except Exception as e:
            logger.error(f"Error saving account info to JSON: {e}")

    def print_account_info(self):
        """
        Prints all attributes of the trader instance.
        """
        print("----- OKXTrader Account Info -----")
        print(f"API Key: {self.api_key}")
        print(f"API Secret: {'*' * len(self.api_secret) if self.api_secret else 'Not Set'}")
        print(f"Passphrase: {'*' * len(self.passphrase) if self.passphrase else 'Not Set'}")
        print(f"Balance: {self.balance}")
        print(f"Holdings: {self.holdings}")
        print(f"Active Orders: {len(self.active_orders)}")
        print("----------------------------------")

    def get_minimum_investment_by_coin(self, coin: str) -> float:

        #market = self.exchange.load_market(coin)
        #replace - with /
        coin = coin.replace('-', '/')
        markets=self.exchange.load_markets()
        market = markets[coin]
        min_order_size = float(market['limits']['amount']['min'])
        ticker = self.exchange.fetch_ticker(coin)
        
        current_price = ticker['last']

        # Calculate the minimum order value in the quote currency
        min_order_value = min_order_size * current_price

        # Display the results
        print(f"Minimum order size for {coin}: {min_order_size} {market['base']}")
        print(f"Equivalent to: {min_order_value:.2f} {market['quote']}")
        return min_order_value
    
    def get_minimum_investment_by_coin_list(self) -> float:
        #to record the max of the minimum investment among all coins
        min_investment =0
        for coin in COIN_LIST:
            min_investment = max(min_investment, self.get_minimum_investment_by_coin(coin))
        return min_investment
    
    def get_current_price(self, coin: str) -> float:
        """
        Get the current price of a coin.
        """
        ticker = self.exchange.fetch_ticker(coin)
        return float(ticker['last'])
    
    def calculate_pnl(self, start_date: str, end_date: str):
        """
        Calculate profit and loss for transactions between start_date and end_date.
        
        :param start_date: Start date in ISO format (e.g., "2023-01-01T00:00:00Z")
        :param end_date: End date in ISO format (e.g., "2023-12-31T23:59:59Z")
        :return: Dictionary with PnL information by coin and total
        """
        logger.info(f"Calculating transactions PnL from {start_date} to {end_date}...")
        
        try:
            # Convert dates to milliseconds timestamp for OKX API
            start_timestamp = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp() * 1000)
            end_timestamp = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp() * 1000)
            
            filled_orders = self.get_closed_orders(start_date, end_date)
            
            # Filter to only include filled/closed orders
            
            
            # Calculate PnL by coin
            pnl_by_coin = {}
            for order in filled_orders:
                symbol = order['symbol']
                coin = symbol.split('/')[0]  # Extract base currency (e.g., BTC from BTC/USDT)
                
                if coin not in pnl_by_coin:
                    pnl_by_coin[coin] = {
                        'buy_volume': 0.0,
                        'buy_value': 0.0,
                        'sell_volume': 0.0,
                        'sell_value': 0.0,
                        'fee': 0.0,
                        'realized_pnl': 0.0
                    }
                
                # Add order data to the coin's records
                side = order['side']
                price = float(order['price'])
                amount = float(order['amount'])
                cost = float(order['cost'])
                fee = float(order.get('fee', {}).get('cost', 0))
                
                if side == 'buy':
                    pnl_by_coin[coin]['buy_volume'] += amount
                    pnl_by_coin[coin]['buy_value'] += cost
                elif side == 'sell':
                    pnl_by_coin[coin]['sell_volume'] += amount
                    pnl_by_coin[coin]['sell_value'] += cost
                
                pnl_by_coin[coin]['fee'] += fee
            
            # Calculate realized PnL for each coin
            total_pnl = 0.0
            for coin, data in pnl_by_coin.items():
                # Calculate average buy and sell prices
                avg_buy_price = data['buy_value'] / data['buy_volume'] if data['buy_volume'] > 0 else 0
                avg_sell_price = data['sell_value'] / data['sell_volume'] if data['sell_volume'] > 0 else 0
                
                # Calculate realized PnL (from closed positions)
                min_volume = min(data['buy_volume'], data['sell_volume'])
                data['realized_pnl'] = (avg_sell_price - avg_buy_price) * min_volume - data['fee']
                data['realized_pnl'] = data['realized_pnl'] - data['fee']
                #percentage of pnl
                data['pnl_percent'] = (data['realized_pnl'] / data['buy_value']) * 100

                total_pnl += data['realized_pnl']
                print(f"PnL for {coin}: {data['realized_pnl']}, fee: {data['fee']}, buy_volume: {data['buy_volume']}, sell_volume: {data['sell_volume']}, avg_buy_price: {avg_buy_price}, avg_sell_price: {avg_sell_price}, pnl_percent: {data['pnl_percent']}")
            
            # Add total to the result
            pnl_by_coin['TOTAL'] = {'realized_pnl': total_pnl}
            print(f"Total PnL: {total_pnl}")
            
            return pnl_by_coin
            
        except Exception as e:
            logger.error(f"Error calculating PnL: {e}")
            return {'error': str(e)}

    def print_portfolio_pnl(self):
        """
        Print the profit and loss for each coin in the portfolio and the total portfolio PnL.
        
        :return: Dictionary with current portfolio PnL information
        """
        logger.info("Calculating current portfolio PnL...")
        
        try:
            # Get current balances
            self.sync_account_info()
            
            # Get current market prices
            portfolio_pnl = {}
            total_portfolio_value = 0.0
            total_cost_basis = 0.0
            
            # Calculate PnL for each coin with non-zero balance
            for currency, balance in self.holdings.items():
                if currency == 'USDT' or currency == 'USD' or currency == 'USDC' or currency == 'SGD' or float(balance) <= 0:
                    continue
                    
                symbol = f"{currency}/USDT"
                okx_symbol = f"{currency}-USDT"  # OKX format
                
                if symbol not in self.exchange.markets:
                    continue
                    
                try:
                    # Get current market price
                    ticker = self.exchange.fetch_ticker(symbol)
                    current_price = float(ticker['last'])
                    
                    # Get position cost basis (if available)
                    cost_basis = 0.0
                    
                    # For OKX, we'll estimate cost basis from recent orders instead of positions
                    try:
                        # Use the unified CCXT API to get orders
                        orders = self.exchange.fetch_my_trades(symbol, limit=100)
                        buy_orders = [o for o in orders if o['side'] == 'buy']
                        
                        if buy_orders:
                            # Calculate weighted average cost
                            total_cost = sum(float(o['cost']) for o in buy_orders)
                            total_amount = sum(float(o['amount']) for o in buy_orders)
                            avg_price = total_cost / total_amount if total_amount > 0 else 0
                            cost_basis = avg_price * float(balance)
                        else:
                            # If no buy orders found, use current price as estimate
                            cost_basis = current_price * float(balance)
                        
                        logger.debug(f"Cost basis for {currency}: {cost_basis}")
                    except Exception as e:
                        logger.debug(f"Could not fetch trades for {currency}: {e}")
                        # If we can't get cost basis, use current price as estimate
                        cost_basis = current_price * float(balance)
                    
                    print(f"Cost basis for {currency}: {cost_basis}")
                    # Calculate current value and unrealized PnL
                    current_value = current_price * float(balance)
                    unrealized_pnl = current_value - cost_basis
                    pnl_percent = (unrealized_pnl / cost_basis * 100) if cost_basis > 0 else 0
                    
                    portfolio_pnl[currency] = {
                        'balance': float(balance),
                        'current_price': current_price,
                        'cost_basis': cost_basis,
                        'current_value': current_value,
                        'unrealized_pnl': unrealized_pnl,
                        'pnl_percent': pnl_percent
                    }
                    
                    total_portfolio_value += current_value
                    total_cost_basis += cost_basis
                    
                except Exception as e:
                    logger.warning(f"Could not calculate PnL for {currency}: {e}")
            
            # Add USDT balance
            usdt_balance = float(self.holdings.get('USDT', 0))
            total_portfolio_value += usdt_balance
            total_cost_basis += usdt_balance
            
            # Calculate total portfolio PnL
            total_unrealized_pnl = total_portfolio_value - total_cost_basis
            total_pnl_percent = (total_unrealized_pnl / total_cost_basis * 100) if total_cost_basis > 0 else 0
            
            portfolio_pnl['TOTAL'] = {
                'usdt_balance': usdt_balance,
                'portfolio_value': total_portfolio_value,
                'cost_basis': total_cost_basis,
                'unrealized_pnl': total_unrealized_pnl,
                'pnl_percent': total_pnl_percent
            }
            
            # Print the results
            print("\n===== PORTFOLIO PNL SUMMARY =====")
            print(f"USDT Balance: ${usdt_balance:.2f}")
            print("\nCoin Holdings:")
            for coin, data in sorted(portfolio_pnl.items()):
                if coin == 'TOTAL':
                    continue
                print(f"{coin}: {data['balance']:.8f} (${data['current_value']:.2f}) | " +
                      f"PnL: ${data['unrealized_pnl']:.2f} ({data['pnl_percent']:.2f}%)")
            
            print("\n===== TOTAL PORTFOLIO =====")
            print(f"Total Value: ${portfolio_pnl['TOTAL']['portfolio_value']:.2f}")
            print(f"Total PnL: ${portfolio_pnl['TOTAL']['unrealized_pnl']:.2f} " +
                  f"({portfolio_pnl['TOTAL']['pnl_percent']:.2f}%)")
            print("===============================\n")
            
            return portfolio_pnl
            
        except Exception as e:
            logger.error(f"Error calculating portfolio PnL: {e}")
            print(f"Error: {e}")
            return {'error': str(e)}

    # Part 1
    def fetch_triangle_market_data(self):
        """
        Fetches market data required for triangle arbitrage between BTC/USDT, ETH/USDT, and ETH/BTC.
        Returns a dictionary with:
          - The 'last' price and base volume for each pair.
          - A timestamp indicating when the data was fetched.
        """
        try:
            ticker_btc_usdt = self.exchange.fetch_ticker("BTC/USDT")
            ticker_eth_usdt = self.exchange.fetch_ticker("ETH/USDT")
            ticker_eth_btc = self.exchange.fetch_ticker("ETH/BTC")
            
            data = {
                "timestamp": datetime.now().isoformat(),
                "BTC/USDT": {
                    "last": ticker_btc_usdt.get("last"),
                    "volume": ticker_btc_usdt.get("baseVolume")
                },
                "ETH/USDT": {
                    "last": ticker_eth_usdt.get("last"),
                    "volume": ticker_eth_usdt.get("baseVolume")
                },
                "ETH/BTC": {
                    "last": ticker_eth_btc.get("last"),
                    "volume": ticker_eth_btc.get("baseVolume")
                }
            }
            logger.info(f"Fetched triangle market data: {data}")
            return data
        except Exception as e:
            logger.error(f"Error fetching triangle market data: {e}")
            return None

    def store_triangle_data_to_json(self, data, filename="triangle_market_data.json"):
        """
        Stores the fetched triangle market data into a JSON file using a Pandas DataFrame.
        
        Parameters:
          data (dict): The triangle market data.
          filename (str): The output JSON filename.
        
        Returns:
          A Pandas DataFrame if successful; otherwise, None.
        """
        try:
            if data is None:
                logger.error("No triangle data to store.")
                return None
            # Wrap the data in a list to create a single-row DataFrame.
            df = pd.DataFrame([data])
            df.to_json(filename, orient='records', date_format='iso', indent=4)
            logger.info(f"Triangle market data saved to {filename}.")
            return df
        except Exception as e:
            logger.error(f"Error saving triangle market data to JSON: {e}")
            return None

    # Part 2
    def check_triangle_arbitrage(self, threshold=0.002, data=None):
        """
        Checks for triangle arbitrage opportunities using the three spot pairs.
        If 'data' is provided, it is used; otherwise, live data is fetched.
        
        Returns a dictionary with computed cycle factors and opportunity flags.
        """
        try:
            if data is None:
                data = self.fetch_triangle_market_data()
                if data is None:
                    logger.error("No market data available for triangle arbitrage check.")
                    return None

            if not isinstance(data, dict):
                logger.error("Expected data to be a dictionary but got a different type.")
                return None

            # For live data, values might be dicts with a "last" key.
            def get_price(val):
                return val.get("last") if isinstance(val, dict) else val

            btc_usdt = get_price(data.get("BTC/USDT"))
            eth_usdt = get_price(data.get("ETH/USDT"))
            eth_btc = get_price(data.get("ETH/BTC"))

            if not (btc_usdt and eth_usdt and eth_btc):
                logger.error("Missing one or more ticker prices in the fetched data.")
                return None

            cycle1 = eth_usdt / (btc_usdt * eth_btc)
            cycle2 = (btc_usdt * eth_btc) / eth_usdt

            result = {
                "timestamp": data.get("timestamp"),
                "BTC/USDT": btc_usdt,
                "ETH/USDT": eth_usdt,
                "ETH/BTC": eth_btc,
                "Cycle1_factor": cycle1,
                "Cycle1_opportunity": cycle1 > (1 + threshold),
                "Cycle2_factor": cycle2,
                "Cycle2_opportunity": cycle2 > (1 + threshold),
                "threshold": threshold
            }
            logger.info(f"Triangle arbitrage signal: {result}")
            return result
        except Exception as e:
            logger.error(f"Error checking triangle arbitrage: {e}")
            return None


     # ---------------------------
    # Historical Data Functions (Single Function for Entire Period)
    # ---------------------------
    def fetch_all_historical_triangle_data_incremental(self, start_dt, end_dt, timeframe="1m", limit=100, chunk_minutes=100, filename="triangle_market_data_historical.json"):
        """
        Incrementally fetches historical triangle data for BTC/USDT, ETH/USDT, and ETH/BTC from start_dt until end_dt.
        Because each API call returns at most 'limit' candles, the function divides the period into chunks of
        'chunk_minutes' minutes. For each chunk, it fetches the data for each pair, merges them by common timestamps,
        and immediately appends the merged records to a JSON file.
        
        Parameters:
            start_dt (datetime): The starting datetime (UTC).
            end_dt (datetime): The ending datetime (UTC).
            timeframe (str): The timeframe for OHLCV data (default "1m").
            limit (int): Maximum number of candles per API call (default 100).
            chunk_minutes (int): Size of each chunk in minutes (default 100).
            filename (str): The output JSON file name.
            
        Returns:
            None. Data is written to the specified JSON file.
        """
        try:
            symbols = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]
            # Open file and write the opening bracket for a JSON array.
            with open(filename, 'w') as f:
                f.write("[\n")
                first_record = True  # For proper comma handling.
                current_start = start_dt
                while current_start < end_dt:
                    current_end = current_start + timedelta(minutes=chunk_minutes)
                    if current_end > end_dt:
                        current_end = end_dt
                    logger.info(f"Fetching data from {current_start.isoformat()} to {current_end.isoformat()}")
                    # For each symbol, fetch candles within this window.
                    data_by_symbol = {}
                    for sym in symbols:
                        candles = []
                        since = self.exchange.parse8601(current_start.isoformat() + "Z")
                        end_timestamp = self.exchange.parse8601(current_end.isoformat() + "Z")
                        while since < end_timestamp:
                            batch = self.exchange.fetch_ohlcv(sym, timeframe=timeframe, since=since, limit=limit)
                            if not batch:
                                break
                            for candle in batch:
                                if candle[0] >= end_timestamp:
                                    break
                                candles.append(candle)
                            since = batch[-1][0] + 1
                            if len(batch) < limit:
                                break
                        # Map each candle's timestamp to its close price.
                        rec_dict = {datetime.utcfromtimestamp(candle[0]/1000).isoformat() + "Z": candle[4] for candle in candles}
                        data_by_symbol[sym] = rec_dict
                        logger.info(f"Fetched {len(rec_dict)} candles for {sym} in this window.")
                    # Find common timestamps across all symbols.
                    common_ts = set(data_by_symbol[symbols[0]].keys())
                    for sym in symbols[1:]:
                        common_ts = common_ts.intersection(set(data_by_symbol[sym].keys()))
                    common_ts = sorted(common_ts)
                    # For each common timestamp, merge the data.
                    for ts in common_ts:
                        record = {"timestamp": ts}
                        for sym in symbols:
                            record[sym] = data_by_symbol[sym][ts]
                        # Write record as a JSON object. If it's not the first record, prepend a comma.
                        if not first_record:
                            f.write(",\n")
                        else:
                            first_record = False
                        json.dump(record, f, indent=4)
                    current_start = current_end
                # Close the JSON array.
                f.write("\n]")
            logger.info(f"All historical triangle data has been saved to {filename}.")
        except Exception as e:
            logger.error(f"Error fetching all historical triangle data incrementally: {e}")

    # ---------------------------
    # Backtesting Function
    # ---------------------------
    def backtest_triangle_arbitrage_minute(self, historical_data, trade_fraction=0.1, threshold=0.002):
        """
        Backtests triangle arbitrage using historical minute data.
        
        Parameters:
            historical_data (list): A list of merged records. Each record is a dict with keys:
                "timestamp", "BTC/USDT", "ETH/USDT", "ETH/BTC" (values are the close prices).
            trade_fraction (float): Fraction of the portfolio to use per trade (default 0.1).
            threshold (float): Minimum arbitrage excess over 1 required to trigger a trade (default 0.002, or 0.2%).
        
        Simulation:
            - Start with an initial portfolio (e.g., 10,000 USDT).
            - For each record (representing one minute), compute arbitrage signal using check_triangle_arbitrage.
            - If either cycle factor exceeds 1 + threshold, simulate a trade:
                  new_portfolio = current_portfolio * [1 + trade_fraction * (selected_factor - 1)]
            - Track the portfolio value and trade returns.
        
        Returns:
            dict: Contains:
                - portfolio_history: List of portfolio values over time.
                - cumulative_return: Overall portfolio return.
                - average_return: Average return per trade.
                - std_return: Standard deviation of trade returns.
                - sharpe_ratio: Annualized Sharpe ratio (using sqrt(525600) for minute data).
                - max_drawdown: Maximum drawdown.
        """
        try:
            if not historical_data or len(historical_data) == 0:
                logger.error("No historical data provided for backtesting.")
                return None

            initial_portfolio = 10000.0
            current_portfolio = initial_portfolio
            portfolio_history = [current_portfolio]
            trade_returns = []

            for i, record in enumerate(historical_data):
                arb_signal = self.check_triangle_arbitrage(threshold=threshold, data=record)
                if arb_signal is None:
                    logger.warning(f"Record {i}: No arbitrage signal (data issue).")
                    trade_returns.append(0)
                    portfolio_history.append(current_portfolio)
                    continue

                cycle1 = arb_signal.get("Cycle1_factor", 0)
                cycle2 = arb_signal.get("Cycle2_factor", 0)
                opp1 = arb_signal.get("Cycle1_opportunity", False)
                opp2 = arb_signal.get("Cycle2_opportunity", False)

                if opp1 or opp2:
                    selected_factor = max(cycle1 if opp1 else 0, cycle2 if opp2 else 0)
                    profit_pct = selected_factor - 1.0
                    trade_profit = current_portfolio * trade_fraction * profit_pct
                    current_portfolio += trade_profit
                    risked = current_portfolio - trade_profit if (current_portfolio - trade_profit) != 0 else 1
                    trade_return = trade_profit / risked
                    logger.info(f"Record {i}: Trade executed (factor: {selected_factor:.4f}, profit_pct: {profit_pct:.4f}), new portfolio: {current_portfolio:.2f}")
                else:
                    trade_return = 0
                    logger.info(f"Record {i}: No arbitrage opportunity detected.")
                
                trade_returns.append(trade_return)
                portfolio_history.append(current_portfolio)

            cumulative_return = (current_portfolio / initial_portfolio) - 1
            avg_return = sum(trade_returns) / len(trade_returns) if trade_returns else 0
            std_return = (sum((r - avg_return) ** 2 for r in trade_returns) / len(trade_returns)) ** 0.5 if trade_returns else 0
            sharpe_ratio = (avg_return / std_return * (525600 ** 0.5)) if std_return != 0 else float('inf')
            peak = portfolio_history[0]
            max_drawdown = 0
            for value in portfolio_history:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

            result = {
                "portfolio_history": portfolio_history,
                "cumulative_return": cumulative_return,
                "average_return": avg_return,
                "std_return": std_return,
                "sharpe_ratio": sharpe_ratio,
                "max_drawdown": max_drawdown
            }
            logger.info(f"Backtest result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error during backtesting: {e}")
            return None

def main():
    """
    Main function to fetch essential account details and orders.
    """
    logger.info("Starting main workflow...")
    # Instantiate the trader
    trader = OKXTrader()

    # Fetch balances and holdings
    balance = trader.get_account_balance()

    # For demonstration, let's pick some date range
    start_date = "2025-01-01T00:00:00Z"
    end_date = "2025-02-24T23:59:59Z"

    # Fetch active (open) orders
    active_orders = trader.get_orders_by_date(start_date, end_date, status='active')

    logger.info(f"Balance: {balance}")
    logger.info(f"Active Orders within {start_date} - {end_date}: {active_orders}")

    # Print to console as well
    trader.print_account_info()

    # Part 1: Fetch triangle market data and store it
    triangle_data = trader.fetch_triangle_market_data()
    df_triangle = trader.store_triangle_data_to_json(triangle_data)
    if df_triangle is not None:
        print("Triangle market data stored:")
        print(df_triangle)
    
    # Part 2: Generate triangle arbitrage signal using the fetched data
    arb_signal = trader.check_triangle_arbitrage(threshold=0.002, data=triangle_data)
    if arb_signal:
        print("Triangle arbitrage signal:")
        print(arb_signal)
    else:
        print("No triangle arbitrage signal or error encountered.")
    

    # --- HISTORICAL DATA SECTION ---
    # Define the period
    # end_dt = datetime.utcnow()
    # start_dt = end_dt - timedelta(days=30)
    # print(f"Fetching historical triangle minute data from {start_dt.isoformat()} to {end_dt.isoformat()}")
    
    # # Use the incremental fetching function.
    # trader.fetch_all_historical_triangle_data_incremental(
    #     start_dt=start_dt,
    #     end_dt=end_dt,
    #     timeframe="1m",
    #     limit=100,
    #     chunk_minutes=100,
    #     filename="triangle_market_data_historical.json"
    # )
    
    # Load the stored data from JSON for backtesting.
    try:
        historical_data = pd.read_json("triangle_market_data_historical.json", orient="records")
        historical_data = historical_data.to_dict(orient="records")
        print("Historical Triangle Market Data (first 5 records):")
        print(pd.DataFrame(historical_data).head())
    except Exception as e:
        logger.error(f"Error reading historical triangle data from JSON: {e}")
        return

    # Run backtest using the fetched historical data.
    backtest_result = trader.backtest_triangle_arbitrage_minute(
        historical_data=historical_data,
        trade_fraction=0.1,
        threshold=0.002
    )
    if backtest_result:
        print("Backtest Results:")
        print(f"Cumulative Return: {backtest_result['cumulative_return']*100:.2f}%")
        print(f"Average Return per Trade: {backtest_result['average_return']*100:.2f}%")
        print(f"Standard Deviation of Returns: {backtest_result['std_return']*100:.2f}%")
        print(f"Annualized Sharpe Ratio: {backtest_result['sharpe_ratio']:.2f}")
        print(f"Maximum Drawdown: {backtest_result['max_drawdown']*100:.2f}%")
    else:
        print("Backtest failed or no data available.")
    

def test_orders():
    """
    Test the orders functions.
    """
    # trader = OKXTrader()
    #trader.place_limit_order('BUY', 'BTC-USDT', 0.0001, 80000)
    #trader.place_stop_loss_order('SELL', 'BTC-USDT', 0.0001, 70000)
    #trader.place_take_profit_order('SELL', 'BTC-USDT', 0.001, 120000)
    #trader.place_market_order('BUY', 'ETH-USDT', 0.01)
    #print(trader.place_limit_order('BUY', 'BTC-USDT', 0.01, 2000))
    #print(trader.place_limit_order('BUY', 'ETH-USDT', 0.01, 2000))
    #print(trader.place_limit_order('BUY', 'ETH-USDT', 0.01, 2000))
    #print(trader.place_limit_order('BUY', 'ETH-USDT', 0.01, 2000))
    #print(trader.place_limit_order('BUY', 'ETH-USDT', 0.01, 2000))
    #print(trader.cancel_all_orders())
    #trader.place_limit_order_with_limit_sl_tp('BUY', 'ETH-USDT', 0.01, 2000, 2500, 1900)
    #start_date = "2025-01-01T00:00:00Z"
    #end_date = "2025-02-24T23:59:59Z"
    #trader.get_open_orders(start_date, end_date)
    #trader.get_closed_orders(start_date, end_date)
    # Print to console as well
    #trader.print_account_info()

if __name__ == "__main__":
    main()

#test_orders()
#trader = OKXTrader()
#trader.cancel_all_orders()

#trader = OKXTrader()
#print(trader.get_minimum_investment_by_coin('ADA-USDT'))
#print(trader.get_minimum_investment_by_coin_list())

#trader = OKXTrader()
#trader.calculate_pnl('2025-01-01T00:00:00Z', '2025-02-27T23:59:59Z')

#trader.print_portfolio_pnl()
