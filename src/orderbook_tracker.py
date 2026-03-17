"""
Orderbook Tracker - Track large orders and detect spoofing behavior
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import math

HISTORY_DIR = "/root/.okx-paper/data/orderbook_history"
CURRENT_SNAPSHOT = f"{HISTORY_DIR}/current_snapshot.json"
ORDER_HISTORY = f"{HISTORY_DIR}/order_history.json"

# Ensure directory exists
os.makedirs(HISTORY_DIR, exist_ok=True)


class OrderTracker:
    """Track large orders over time and detect spoofing patterns"""
    
    def __init__(self, history_file: str = ORDER_HISTORY):
        self.history_file = history_file
        self.order_history = self._load_history()
        self.min_large_order_size = 10.0  # BTC
        # More reasonable thresholds
        self.spoof_threshold_seconds = 10  # Orders lasting < 10s are suspicious (lowered from 30)
        self.cancel_threshold = 5  # >5 cancels = likely spoofing (raised from 3)
        self.price_zone_size = 100  # Group prices within $100 range
        
    def _load_history(self) -> Dict[str, Any]:
        """Load order history from file"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_history(self):
        """Save order history to file"""
        with open(self.history_file, 'w') as f:
            json.dump(self.order_history, f, indent=2)
    
    def _get_price_key(self, price: float, tick_size: float = 0.1) -> str:
        """Normalize price to key using price zone (group within $100 range)"""
        zone = int(price / self.price_zone_size) * self.price_zone_size
        return f"{zone:.0f}"
    
    def update_orders(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], 
                      current_time: Optional[float] = None) -> Dict[str, Any]:
        """
        Update order tracking with new orderbook snapshot
        Returns analysis of current large orders with credibility scores
        """
        if current_time is None:
            current_time = time.time()
        
        current_orders = set()
        analysis = {
            "timestamp": current_time,
            "large_bids": [],
            "large_asks": [],
            "spoofing_signals": [],
            "credible_orders": []
        }
        
        # Process bids
        for price, volume in bids:
            if volume >= self.min_large_order_size:
                price_key = self._get_price_key(price)
                order_id = f"bid_{price_key}"
                current_orders.add(order_id)
                
                # Check if this order existed before
                if order_id in self.order_history:
                    # Order still exists, update duration
                    order = self.order_history[order_id]
                    order["duration"] = current_time - order["first_seen"]
                    order["last_seen"] = current_time
                    order["volume_history"].append(volume)
                    order["current_volume"] = volume
                    
                    # Check for volume changes (partial cancel/fill)
                    if len(order["volume_history"]) >= 2:
                        vol_change = abs(volume - order["volume_history"][-2])
                        if vol_change > order["volume_history"][-2] * 0.3:
                            # Volume changed > 30%
                            order["change_count"] = order.get("change_count", 0) + 1
                else:
                    # New order
                    self.order_history[order_id] = {
                        "side": "bid",
                        "price": price,
                        "first_seen": current_time,
                        "last_seen": current_time,
                        "duration": 0,
                        "volume_history": [volume],
                        "current_volume": volume,
                        "cancel_count": 0,
                        "change_count": 0
                    }
                
                order = self.order_history[order_id]
                credibility = self._calculate_credibility(order)
                
                analysis["large_bids"].append({
                    "price": price,
                    "volume": volume,
                    "duration": order["duration"],
                    "credibility": credibility,
                    "cancel_count": order.get("cancel_count", 0),
                    "is_suspicious": credibility < 0.5
                })
        
        # Process asks
        for price, volume in asks:
            if volume >= self.min_large_order_size:
                price_key = self._get_price_key(price)
                order_id = f"ask_{price_key}"
                current_orders.add(order_id)
                
                if order_id in self.order_history:
                    order = self.order_history[order_id]
                    order["duration"] = current_time - order["first_seen"]
                    order["last_seen"] = current_time
                    order["volume_history"].append(volume)
                    order["current_volume"] = volume
                    
                    if len(order["volume_history"]) >= 2:
                        vol_change = abs(volume - order["volume_history"][-2])
                        if vol_change > order["volume_history"][-2] * 0.3:
                            order["change_count"] = order.get("change_count", 0) + 1
                else:
                    self.order_history[order_id] = {
                        "side": "ask",
                        "price": price,
                        "first_seen": current_time,
                        "last_seen": current_time,
                        "duration": 0,
                        "volume_history": [volume],
                        "current_volume": volume,
                        "cancel_count": 0,
                        "change_count": 0
                    }
                
                order = self.order_history[order_id]
                credibility = self._calculate_credibility(order)
                
                analysis["large_asks"].append({
                    "price": price,
                    "volume": volume,
                    "duration": order["duration"],
                    "credibility": credibility,
                    "cancel_count": order.get("cancel_count", 0),
                    "is_suspicious": credibility < 0.5
                })
        
        # Detect orders that disappeared (canceled)
        for order_id in list(self.order_history.keys()):
            if order_id not in current_orders:
                order = self.order_history[order_id]
                time_since_last_seen = current_time - order["last_seen"]
                
                if time_since_last_seen > 5:  # Gone for >5 seconds
                    order["cancel_count"] = order.get("cancel_count", 0) + 1
                    order["cancel_time"] = current_time
                    
                    # If order came back after cancellation, it's suspicious
                    if order["cancel_count"] >= self.cancel_threshold:
                        analysis["spoofing_signals"].append({
                            "type": "frequent_cancel",
                            "order_id": order_id,
                            "price": order["price"],
                            "side": order["side"],
                            "cancel_count": order["cancel_count"],
                            "severity": "high" if order["cancel_count"] >= 5 else "medium"
                        })
        
        # Clean up old orders (keep last hour)
        cutoff_time = current_time - 3600
        for order_id in list(self.order_history.keys()):
            if self.order_history[order_id]["last_seen"] < cutoff_time:
                del self.order_history[order_id]
        
        # Find credible orders
        for bid in analysis["large_bids"]:
            if bid["credibility"] >= 0.7:
                analysis["credible_orders"].append({
                    "side": "bid",
                    "price": bid["price"],
                    "volume": bid["volume"],
                    "credibility": bid["credibility"]
                })
        for ask in analysis["large_asks"]:
            if ask["credibility"] >= 0.7:
                analysis["credible_orders"].append({
                    "side": "ask", 
                    "price": ask["price"],
                    "volume": ask["volume"],
                    "credibility": ask["credibility"]
                })
        
        # Save history
        self._save_history()
        
        return analysis
    
    def _calculate_credibility(self, order: Dict[str, Any]) -> float:
        """
        Calculate credibility score for an order (0-1)
        
        Factors:
        - Duration: longer = more credible
        - Cancel count: more cancels = less credible
        - Volume stability: stable volume = more credible
        """
        score = 1.0
        
        # Duration factor (more lenient)
        duration = order.get("duration", 0)
        if duration < self.spoof_threshold_seconds:
            # Short duration orders are slightly suspicious
            score *= 0.5 + (duration / self.spoof_threshold_seconds) * 0.4
        elif duration < 60:
            # Medium duration - acceptable
            score *= 0.8 + min((duration - 10) / 50, 0.2)
        # else: long duration = full score
        
        # Cancel count factor (more lenient)
        cancel_count = order.get("cancel_count", 0)
        if cancel_count >= 10:
            score *= 0.2  # Very suspicious
        elif cancel_count >= self.cancel_threshold:
            score *= 0.5
        elif cancel_count >= 2:
            score *= 0.8
        
        # Volume stability factor
        change_count = order.get("change_count", 0)
        if change_count >= 10:
            score *= 0.4
        elif change_count >= 5:
            score *= 0.6
        
        # Boost for very stable orders
        if duration > 120 and cancel_count == 0:
            score = min(1.0, score * 1.2)
        
        return max(0.0, min(1.0, score))
    
    def get_adjusted_volume(self, orders: List[Dict[str, Any]], side: str) -> Tuple[float, float]:
        """
        Get credibility-weighted volume for a side
        Returns: (raw_volume, credibility_adjusted_volume)
        """
        raw_volume = sum(o.get("volume", 0) for o in orders if o.get("side") == side)
        adjusted_volume = sum(
            o.get("volume", 0) * o.get("credibility", 1.0) 
            for o in orders if o.get("side") == side
        )
        return raw_volume, adjusted_volume
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of tracking statistics"""
        total_orders = len(self.order_history)
        suspicious_orders = sum(
            1 for o in self.order_history.values() 
            if self._calculate_credibility(o) < 0.5
        )
        credible_orders = total_orders - suspicious_orders
        
        return {
            "total_tracked_orders": total_orders,
            "credible_orders": credible_orders,
            "suspicious_orders": suspicious_orders,
            "spoofing_rate": suspicious_orders / max(1, total_orders)
        }


# Singleton instance
_tracker_instance = None

def get_tracker() -> OrderTracker:
    """Get or create tracker instance"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = OrderTracker()
    return _tracker_instance
