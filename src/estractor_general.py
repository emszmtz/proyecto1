import os
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from typing import List, Optional
import json

# Cargar variables de entorno
load_dotenv()


@dataclass
class StockData:
    """Clase para representar datos uniformes de acciones"""
    fecha: pd.Timestamp
    symbol: str
    open: float
    close: float
    high: float
    low: float
    volume: int
    
    def to_dict(self):
        """Convierte el objeto a diccionario para crear DataFrame"""
        return {
            'fecha': self.fecha,
            'symbol': self.symbol,
            'open': self.open,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'volume': self.volume
        }


class AlphaVantageExtractor:
    """Extractor de datos desde Alpha Vantage API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
    
    def extract(self, symbols: List[str], fecha_limite: datetime) -> List[StockData]:
        """Extrae datos de Alpha Vantage para los símbolos dados"""
        results = []
        
        for i, symbol in enumerate(symbols):
            print(f"\n[Alpha Vantage] Descargando {symbol}...")
            
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'outputsize': 'full',
                'apikey': self.api_key
            }
            
            try:
                response = requests.get(self.base_url, params=params, timeout=10)
                data = response.json()
                
                # Buscar clave de Time Series
                ts_key = next((k for k in data.keys() if "Time Series" in k), None)
                
                if not ts_key:
                    print(f"   ⚠️ No se encontró Time Series para {symbol}")
                    continue
                
                time_series = data[ts_key]
                
                for date_str, values in time_series.items():
                    date = pd.to_datetime(date_str)
                    
                    if date >= fecha_limite:
                        stock_data = StockData(
                            fecha=date,
                            symbol=symbol,
                            open=float(values['1. open']),
                            close=float(values['4. close']),
                            high=float(values['2. high']),
                            low=float(values['3. low']),
                            volume=int(values['5. volume'])
                        )
                        results.append(stock_data)
                
                print(f"   ✅ {symbol}: {sum(1 for r in results if r.symbol == symbol)} filas")
                
                # Esperar para no exceder límite de API (5 por minuto)
                if i < len(symbols) - 1:
                    time.sleep(12)
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return results


class FinnhubExtractor:
    """Extractor de datos desde Finnhub API (solo precio actual)"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://finnhub.io/api/v1"
    
    def extract(self, symbols: List[str], fecha_limite: datetime) -> List[StockData]:
        """
        Extrae solo el precio actual de Finnhub (quote endpoint).
        La versión gratuita no tiene acceso a datos históricos completos.
        """
        results = []
        fecha_hoy = pd.Timestamp.now().normalize()
        
        for symbol in symbols:
            print(f"\n[Finnhub] Descargando {symbol}...")
            
            url = f"{self.base_url}/quote"
            params = {
                'symbol': symbol,
                'token': self.api_key
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                data = response.json()
                
                # Finnhub quote solo devuelve precio actual
                precio_actual = data.get('c')  # current price
                precio_apertura = data.get('o')  # open price
                precio_high = data.get('h')  # high price
                precio_low = data.get('l')  # low price
                
                if precio_actual is not None:
                    # Crear un registro con el precio actual
                    # Nota: Finnhub gratis no da volumen histórico
                    stock_data = StockData(
                        fecha=fecha_hoy,
                        symbol=symbol,
                        open=float(precio_apertura) if precio_apertura else 0.0,
                        close=float(precio_actual),
                        high=float(precio_high) if precio_high else 0.0,
                        low=float(precio_low) if precio_low else 0.0,
                        volume=0  # No disponible en versión gratuita
                    )
                    results.append(stock_data)
                    print(f"   ✅ {symbol}: Precio actual ${precio_actual:.2f}")
                else:
                    print(f"   ⚠️ No se pudo obtener precio para {symbol}")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return results


class FMPExtractor:
    """Extractor de datos desde Financial Modeling Prep API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
    
    def extract(self, symbols: List[str], fecha_limite: datetime) -> List[StockData]:
        """
        Extrae datos de FMP para los símbolos dados.
        Usa el endpoint básico que funciona con la versión gratuita.
        """
        results = []
        
        for i, symbol in enumerate(symbols):
            print(f"\n[FMP] Descargando {symbol}...")
            
            # Endpoint simplificado para versión gratuita
            url = f"{self.base_url}/historical-chart/1day/{symbol}"
            params = {
                'apikey': self.api_key
            }
            
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                
                if not data or not isinstance(data, list):
                    print(f"   ⚠️ No hay datos para {symbol}")
                    continue
                
                # FMP devuelve una lista de diccionarios
                for item in data:
                    # Parsear la fecha (viene como string)
                    date_str = item.get('date')
                    if not date_str:
                        continue
                    
                    date = pd.to_datetime(date_str)
                    
                    # Filtrar solo últimos 3 meses
                    if date < fecha_limite:
                        continue
                    
                    stock_data = StockData(
                        fecha=date,
                        symbol=symbol,
                        open=float(item.get('open', 0)),
                        close=float(item.get('close', 0)),
                        high=float(item.get('high', 0)),
                        low=float(item.get('low', 0)),
                        volume=int(item.get('volume', 0))
                    )
                    results.append(stock_data)
                
                count = sum(1 for r in results if r.symbol == symbol)
                print(f"   ✅ {symbol}: {count} filas")
                
                # Esperar para no exceder límite (4 req/segundo en free tier)
                if i < len(symbols) - 1:
                    time.sleep(0.3)
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    print(f"   ❌ Error 403: Límite de API alcanzado o endpoint no disponible en tier gratuito")
                else:
                    print(f"   ❌ Error HTTP {e.response.status_code}: {e}")
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return results


def main():
    """Función principal para ejecutar todos los extractores"""
    
    print("=" * 70)
    print("EXTRACTOR UNIFICADO DE DATOS FINANCIEROS")
    print("=" * 70)
    
    # Configuración
    symbols = ['NVDA', 'MSFT', 'TSLA']
    fecha_limite = datetime.now() - timedelta(days=90)
    
    print(f"\nSímbolos: {', '.join(symbols)}")
    print(f"Periodo: Últimos 3 meses (desde {fecha_limite.date()})")
    print(f"Fecha actual: {datetime.now().date()}")
    
    # Diccionario para almacenar resultados por fuente
    resultados = {}
    
    # Alpha Vantage
    print("\n" + "=" * 70)
    print("FUENTE 1: ALPHA VANTAGE")
    print("=" * 70)
    alpha_key = os.getenv("Alpha_key")
    if alpha_key:
        extractor_alpha = AlphaVantageExtractor(alpha_key)
        data_alpha = extractor_alpha.extract(symbols, fecha_limite)
        if data_alpha:
            df_alpha = pd.DataFrame([d.to_dict() for d in data_alpha])
            df_alpha = df_alpha.sort_values(['symbol', 'fecha'])
            resultados['Alpha Vantage'] = df_alpha
            print(f"\n✅ Total Alpha Vantage: {len(df_alpha)} registros")
    else:
        print("⚠️ No se encontró Alpha_key en .env")
    
    # Finnhub
    print("\n" + "=" * 70)
    print("FUENTE 2: FINNHUB")
    print("=" * 70)
    finnhub_key = os.getenv("Finnhub_key")
    if finnhub_key:
        extractor_finnhub = FinnhubExtractor(finnhub_key)
        data_finnhub = extractor_finnhub.extract(symbols, fecha_limite)
        if data_finnhub:
            df_finnhub = pd.DataFrame([d.to_dict() for d in data_finnhub])
            df_finnhub = df_finnhub.sort_values(['symbol', 'fecha'])
            resultados['Finnhub'] = df_finnhub
            print(f"\n✅ Total Finnhub: {len(df_finnhub)} registros")
    else:
        print("⚠️ No se encontró Finnhub_key en .env")
    
    # FMP
    print("\n" + "=" * 70)
    print("FUENTE 3: FINANCIAL MODELING PREP (FMP)")
    print("=" * 70)
    fmp_key = os.getenv("FMP_key")
    if fmp_key:
        extractor_fmp = FMPExtractor(fmp_key)
        data_fmp = extractor_fmp.extract(symbols, fecha_limite)
        if data_fmp:
            df_fmp = pd.DataFrame([d.to_dict() for d in data_fmp])
            df_fmp = df_fmp.sort_values(['symbol', 'fecha'])
            resultados['FMP'] = df_fmp
            print(f"\n✅ Total FMP: {len(df_fmp)} registros")
    else:
        print("⚠️ No se encontró FMP_key en .env")
    
    # Mostrar resumen de todas las fuentes
    print("\n" + "=" * 70)
    print("RESUMEN DE DATOS OBTENIDOS")
    print("=" * 70)
    
    for fuente, df in resultados.items():
        print(f"\n{'=' * 70}")
        print(f"FUENTE: {fuente}")
        print(f"{'=' * 70}")
        print(f"Total de registros: {len(df)}")
        print(f"\nEstadísticas por símbolo:")
        print(df.groupby('symbol').agg({
            'fecha': ['count', 'min', 'max'],
            'close': ['mean', 'min', 'max']
        }).round(2))
        
        print(f"\n📊 Primeros 10 registros:")
        print(df.head(10).to_string(index=False))
        
        # Guardar en CSV
        filename = f"datos_{fuente.lower().replace(' ', '_')}.csv"
        df.to_csv(filename, index=False)
        print(f"\n💾 Datos guardados en: {filename}")
    
    print("\n" + "=" * 70)
    print("PROCESO COMPLETADO")
    print("=" * 70)
    
    return resultados


if __name__ == "__main__":
    resultados = main()