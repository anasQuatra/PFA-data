"""
BinanceDataFetcher: Module pour récupérer les données Bitcoin en temps réel depuis l'API Binance.

Fournit une interface pour :
- Récupérer les données historiques (OHLCV)
- Obtenir le carnet d'ordres en temps réel (order book)
- Formater les données pour la classe DataHandler
"""

from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
from datetime import datetime, timezone
from typing import Dict, List


class BinanceDataFetcher:
    """
    Fetcher pour récupérer les données Bitcoin depuis Binance API (sans clés).
    
    L'API publique Binance ne nécessite pas d'authentification pour :
    - Les données de marché (prix, volume)
    - L'historique OHLCV
    - Le carnet d'ordres (order book)
    
    Attention : Sans clés, certains endpoints sont limités en débit (1200 req/min).
    """
    
    def __init__(self, symbol: str = "BTCUSDT", testnet: bool = False):
        """
        Initialiser le fetcher Binance.
        
        Args:
            symbol (str): Paire de trading (ex: "BTCUSDT" pour Bitcoin/USDT)
            testnet (bool): Utiliser le testnet Binance (pour test sans frais)
        """
        self.symbol = symbol
        self.testnet = testnet
        
        # Client Binance sans clés (API publique uniquement)
        self.client = Client()
        if testnet:
            self.client.API_URL = 'https://testnet.binance.vision/api'
        
        self.last_fetch_time = 0
        self.min_interval = 0.1  # 100ms minimum entre requêtes
    
    def _throttle(self):
        """Rate limiting simple pour respecter les limites Binance"""
        elapsed = time.time() - self.last_fetch_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_fetch_time = time.time()
    
    def get_order_book(self, limit: int = 5) -> Dict:
        """
        Récupérer le carnet d'ordres (order book) en temps réel.
        
        Args:
            limit (int): Nombre de niveaux de prix (5, 10, 20, 50, 100, 500, 1000)
        
        Returns:
            dict: Format {"bids": [(price, volume), ...], "asks": [(price, volume), ...]}
        """
        try:
            self._throttle()
            order_book = self.client.get_order_book(symbol=self.symbol, limit=limit)
            
            # Convertir en tuples (prix, volume) pour compatibilité DataHandler
            bids = [(float(price), float(qty)) for price, qty in order_book['bids']]
            asks = [(float(price), float(qty)) for price, qty in order_book['asks']]
            
            return {
                "bids": bids,
                "asks": asks,
                "timestamp": order_book.get('E', None)
            }
        except BinanceAPIException as e:
            print(f"Erreur Binance: {e}")
            raise
    
    def get_klines(self, interval: str = "1m", limit: int = 500) -> List[Dict]:
        """
        Récupérer l'historique OHLCV (candlesticks).
        
        Args:
            interval (str): Intervalle ("1m", "5m", "1h", "1d", etc.)
            limit (int): Nombre de candlesticks (max 1000 par requête)
        
        Returns:
            list[dict]: Liste d'objets JSON-like, une bougie par dictionnaire
        """
        try:
            self._throttle()
            klines = self.client.get_klines(symbol=self.symbol, interval=interval, limit=limit)

            candles: List[Dict] = []
            for item in klines:
                open_time_ms = int(item[0])
                close_time_ms = int(item[6])

                candles.append({
                    "symbol": self.symbol,
                    "interval": interval,
                    "open_time_ms": open_time_ms,
                    "open_time": datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).isoformat(),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "close_time_ms": close_time_ms,
                    "close_time": datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc).isoformat(),
                    "quote_asset_volume": float(item[7]),
                    "number_of_trades": int(item[8]),
                    "taker_buy_base_asset_volume": float(item[9]),
                    "taker_buy_quote_asset_volume": float(item[10])
                })

            return candles
        
        except BinanceAPIException as e:
            print(f"Erreur Binance: {e}")
            raise
    
    def get_ticker(self) -> Dict:
        """
        Récupérer les infos de prix actuelles (24h stats).
        
        Returns:
            dict: Prix, variation 24h, volume, etc.
        """
        try:
            self._throttle()
            ticker = self.client.get_symbol_info(self.symbol)
            stats = self.client.get_ticker(symbol=self.symbol)
            
            return {
                "symbol": self.symbol,
                "price": float(stats['lastPrice']),
                "bid": float(stats['bidPrice']),
                "ask": float(stats['askPrice']),
                "high_24h": float(stats['highPrice']),
                "low_24h": float(stats['lowPrice']),
                "volume_24h": float(stats['volume']),
                "change_24h_percent": float(stats['priceChangePercent']),
                "timestamp": int(stats['closeTime'])
            }
        except BinanceAPIException as e:
            print(f"Erreur Binance: {e}")
            raise
    
    def stream_order_book_snapshots(self, count: int = 10, interval_ms: int = 1000):
        """
        Générer des snapshots du carnet d'ordres à intervalles réguliers.
        Utile pour simuler un flux de données en temps réel.
        
        Args:
            count (int): Nombre de snapshots à générer
            interval_ms (int): Intervalle entre snapshots (ms)
        
        Yields:
            dict: Snapshot du carnet d'ordres avec timestamp
        """
        for i in range(count):
            try:
                snapshot = self.get_order_book(limit=5)
                yield snapshot
                
                if i < count - 1:
                    time.sleep(interval_ms / 1000.0)
            except Exception as e:
                print(f"Erreur lors de la récupération du snapshot {i}: {e}")
                break

