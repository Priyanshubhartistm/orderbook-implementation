import heapq
from collections import defaultdict
import time

class Order:
    def __init__(self, order_id, side, price, quantity, timestamp=None):
        self.order_id = order_id
        self.side = side  # 'buy' or 'sell'
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp if timestamp else time.time()
    
    def __str__(self):
        return f"Order({self.order_id}, {self.side}, {self.price}, {self.quantity})"

class OrderBook:
    def __init__(self):
        self.buy_orders = []  # max heap (negative prices)
        self.sell_orders = []  # min heap
        self.orders = {}  # order_id -> Order
        self.price_levels_buy = defaultdict(list)  # price -> list of orders
        self.price_levels_sell = defaultdict(list)  # price -> list of orders
        
    def add_order(self, order_id, side, price, quantity):
        if order_id in self.orders:
            return False
        
        order = Order(order_id, side, price, quantity)
        self.orders[order_id] = order
        
        if side == 'buy':
            heapq.heappush(self.buy_orders, (-price, order.timestamp, order_id))
            self.price_levels_buy[price].append(order)
        else:
            heapq.heappush(self.sell_orders, (price, order.timestamp, order_id))
            self.price_levels_sell[price].append(order)
        
        return True
    
    def cancel_order(self, order_id):
        if order_id not in self.orders:
            return False
        
        order = self.orders[order_id]
        
        if order.side == 'buy':
            self.price_levels_buy[order.price].remove(order)
            if not self.price_levels_buy[order.price]:
                del self.price_levels_buy[order.price]
        else:
            self.price_levels_sell[order.price].remove(order)
            if not self.price_levels_sell[order.price]:
                del self.price_levels_sell[order.price]
        
        del self.orders[order_id]
        return True
    
    def modify_order(self, order_id, new_price=None, new_quantity=None):
        if order_id not in self.orders:
            return False
        
        order = self.orders[order_id]
        old_price = order.price
        
        if new_price is not None:
            if order.side == 'buy':
                self.price_levels_buy[old_price].remove(order)
                if not self.price_levels_buy[old_price]:
                    del self.price_levels_buy[old_price]
                
                order.price = new_price
                self.price_levels_buy[new_price].append(order)
                heapq.heappush(self.buy_orders, (-new_price, order.timestamp, order_id))
            else:
                self.price_levels_sell[old_price].remove(order)
                if not self.price_levels_sell[old_price]:
                    del self.price_levels_sell[old_price]
                
                order.price = new_price
                self.price_levels_sell[new_price].append(order)
                heapq.heappush(self.sell_orders, (new_price, order.timestamp, order_id))
        
        if new_quantity is not None:
            order.quantity = new_quantity
        
        return True
    
    def get_best_bid(self):
        while self.buy_orders:
            neg_price, timestamp, order_id = self.buy_orders[0]
            if order_id in self.orders:
                return -neg_price
            heapq.heappop(self.buy_orders)
        return None
    
    def get_best_ask(self):
        while self.sell_orders:
            price, timestamp, order_id = self.sell_orders[0]
            if order_id in self.orders:
                return price
            heapq.heappop(self.sell_orders)
        return None
    
    def get_spread(self):
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid is not None and best_ask is not None:
            return best_ask - best_bid
        return None
    
    def get_order_book_snapshot(self, depth=5):
        bids = []
        asks = []
        
        seen_bid_prices = set()
        temp_buy_orders = self.buy_orders[:]
        
        while temp_buy_orders and len(bids) < depth:
            neg_price, timestamp, order_id = heapq.heappop(temp_buy_orders)
            price = -neg_price
            
            if price not in seen_bid_prices and order_id in self.orders:
                seen_bid_prices.add(price)
                total_quantity = sum(order.quantity for order in self.price_levels_buy[price])
                bids.append((price, total_quantity))
        
        seen_ask_prices = set()
        temp_sell_orders = self.sell_orders[:]
        
        while temp_sell_orders and len(asks) < depth:
            price, timestamp, order_id = heapq.heappop(temp_sell_orders)
            
            if price not in seen_ask_prices and order_id in self.orders:
                seen_ask_prices.add(price)
                total_quantity = sum(order.quantity for order in self.price_levels_sell[price])
                asks.append((price, total_quantity))
        
        return {"bids": bids, "asks": asks}
    
    def match_orders(self):
        matches = []
        
        while True:
            best_bid = self.get_best_bid()
            best_ask = self.get_best_ask()
            
            if best_bid is None or best_ask is None or best_bid < best_ask:
                break
            
            buy_orders_at_price = self.price_levels_buy[best_bid]
            sell_orders_at_price = self.price_levels_sell[best_ask]
            
            if not buy_orders_at_price or not sell_orders_at_price:
                break
            
            buy_order = buy_orders_at_price[0]
            sell_order = sell_orders_at_price[0]
            
            match_quantity = min(buy_order.quantity, sell_order.quantity)
            match_price = sell_order.price  # Price priority to seller
            
            matches.append({
                "buy_order_id": buy_order.order_id,
                "sell_order_id": sell_order.order_id,
                "price": match_price,
                "quantity": match_quantity
            })
            
            buy_order.quantity -= match_quantity
            sell_order.quantity -= match_quantity
            
            if buy_order.quantity == 0:
                self.cancel_order(buy_order.order_id)
            
            if sell_order.quantity == 0:
                self.cancel_order(sell_order.order_id)
        
        return matches
    
    def display_order_book(self, depth=5):
        snapshot = self.get_order_book_snapshot(depth)
        
        print("ORDER BOOK")
        print("=" * 30)
        print("ASKS (Sell Orders)")
        for price, quantity in reversed(snapshot["asks"]):
            print(f"  {price:8.2f} | {quantity:6d}")
        
        print("-" * 30)
        
        print("BIDS (Buy Orders)")
        for price, quantity in snapshot["bids"]:
            print(f"  {price:8.2f} | {quantity:6d}")
        
        spread = self.get_spread()
        if spread is not None:
            print(f"\nSpread: {spread:.2f}")
        print("=" * 30)


def test_order_book():
    ob = OrderBook()
    
    print("Testing Order Book Implementation")
    print("=" * 50)
    
    print("\n1. Adding orders...")
    ob.add_order("buy1", "buy", 100.0, 10)
    ob.add_order("buy2", "buy", 99.5, 5)
    ob.add_order("buy3", "buy", 101.0, 8)
    ob.add_order("sell1", "sell", 102.0, 12)
    ob.add_order("sell2", "sell", 103.0, 7)
    ob.add_order("sell3", "sell", 101.5, 15)
    
    ob.display_order_book()
    
    print("\n2. Adding overlapping orders (should trigger matches)...")
    ob.add_order("buy4", "buy", 102.5, 6)
    
    matches = ob.match_orders()
    print(f"Matches found: {len(matches)}")
    for match in matches:
        print(f"  {match}")
    
    ob.display_order_book()
    
    print("\n3. Testing order cancellation...")
    ob.cancel_order("buy2")
    print("Cancelled buy2")
    
    ob.display_order_book()
    
    print("\n4. Testing order modification...")
    ob.modify_order("buy1", new_price=99.0, new_quantity=15)
    print("Modified buy1: price=99.0, quantity=15")
    
    ob.display_order_book()
    
    print("\n5. Adding more orders for matching...")
    ob.add_order("sell4", "sell", 99.0, 10)
    
    matches = ob.match_orders()
    print(f"New matches: {len(matches)}")
    for match in matches:
        print(f"  {match}")
    
    ob.display_order_book()


if __name__ == "__main__":
    test_order_book()