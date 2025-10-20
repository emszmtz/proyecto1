import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import yfinance as yf
from twelvedata import TDClient
from dataclasses import dataclass, asdict

# ===================================================================
# 0. DATACLASS PARA DATOS DE ACCIONES
# ===================================================================
@dataclass
class StockData:
    symbol: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    fuente_datos: str = ""

# ===================================================================
# 1. CONFIGURACIÓN INICIAL
# ===================================================================
load_dotenv()
ALPHA_KEY = os.getenv("alpha_key")
TWELVE_DATA_KEY = os.getenv("12data_key")
if not ALPHA_KEY or not TWELVE_DATA_KEY:
    raise SystemExit("❌ No se encontraron las claves 'alpha_key' o '12data_key' en el .env")

TICKERS = ["GOOG", "AAPL"]
end_date = datetime.today()
start_date = end_date - timedelta(days=60)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

output_dir = "salida_datos"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "datos_consolidados_final.csv")

# ===================================================================
# 2. FUNCIONES DE DESCARGA DE DATOS (RETORNAN LISTA DE StockData)
# ===================================================================
def descargar_alpha_vantage(symbol):
    print(f"  -> Obteniendo datos de Alpha Vantage para {symbol}...")
    data_list = []
    try:
        r = requests.get(
            f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}"
            f"&outputsize=full&apikey={ALPHA_KEY}", timeout=15
        )
        r.raise_for_status()
        data = r.json()
        key = next((k for k in data if "Time Series" in k), None)
        if not key:
            print(f"  ⚠️  {symbol} (Alpha Vantage): No se encontró 'Time Series'.")
            return []
        df = pd.DataFrame.from_dict(data[key], orient='index', dtype=float)
        df.index = pd.to_datetime(df.index)
        df = df.rename(columns={
            '1. open': 'open', '2. high': 'high', '3. low': 'low',
            '4. close': 'close', '5. volume': 'volume'
        })
        df = df[df.index >= start_date]
        for idx, row in df.iterrows():
            data_list.append(StockData(
                symbol=symbol,
                date=idx.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                fuente_datos="Alpha Vantage"
            ))
        print(f"  ✅ {symbol} (Alpha Vantage): {len(data_list)} filas descargadas.")
        return data_list
    except Exception as e:
        print(f"  ❌ {symbol} (Alpha Vantage): Ocurrió un error - {e}")
        return []

def descargar_yfinance(symbol):
    print(f"  -> Obteniendo datos de Yahoo Finance para {symbol}...")
    data_list = []
    try:
        df = yf.download(symbol, start=start_date_str, end=end_date_str,
                         progress=False, auto_adjust=False)
        if df.empty:
            print(f"  ⚠️  {symbol} (Yahoo Finance): No se encontraron datos.")
            return []
        
        # Aplanar MultiIndex si existe
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        if 'adj close' in df.columns:
            df = df.rename(columns={'adj close': 'adj_close'})
        
        for _, row in df.iterrows():
            data_list.append(StockData(
                symbol=symbol,
                date=row.date.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                fuente_datos="Yahoo Finance"
            ))
        print(f"  ✅ {symbol} (Yahoo Finance): {len(data_list)} filas descargadas.")
        return data_list
    except Exception as e:
        print(f"  ❌ {symbol} (Yahoo Finance): Ocurrió un error - {e}")
        return []

def descargar_twelve_data(symbol):
    print(f"  -> Obteniendo datos de Twelve Data para {symbol}...")
    data_list = []
    try:
        td = TDClient(apikey=TWELVE_DATA_KEY)
        ts = td.time_series(
            symbol=symbol, interval="1day",
            start_date=start_date_str, end_date=end_date_str, outputsize=5000
        )
        if ts is None:
            print(f"  ⚠️  {symbol} (Twelve Data): No se encontraron datos.")
            return []
        df = ts.as_pandas().iloc[::-1].reset_index().rename(columns={'datetime': 'date'})
        for _, row in df.iterrows():
            data_list.append(StockData(
                symbol=symbol,
                date=row.date.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                fuente_datos="Twelve Data"
            ))
        print(f"  ✅ {symbol} (Twelve Data): {len(data_list)} filas descargadas.")
        return data_list
    except Exception as e:
        print(f"  ❌ {symbol} (Twelve Data): Ocurrió un error - {e}")
        return []

# ===================================================================
# 3. PROCESO PRINCIPAL Y GUARDADO
# ===================================================================
all_records: list[StockData] = []
for i, ticker in enumerate(TICKERS):
    print(f"\n[{i+1}/{len(TICKERS)}] Procesando ticker: {ticker}")
    all_records += descargar_yfinance(ticker)
    all_records += descargar_alpha_vantage(ticker)
    all_records += descargar_twelve_data(ticker)

if all_records:
    df_final = pd.DataFrame([asdict(rec) for rec in all_records])
    
    print(f"\n📊 Distribución de datos por fuente en el DataFrame final:")
    print(df_final['fuente_datos'].value_counts())
    
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final = df_final.sort_values(by=['symbol', 'date', 'fuente_datos']).reset_index(drop=True)
    df_final.to_csv(output_file, index=False, date_format='%Y-%m-%d')
    
    print(f"\n🎉 ¡Proceso completado!")
    print(f"✅ Datos guardados en: {output_file}")
    print(f"Total de filas consolidadas: {len(df_final)}")
else:
    print("\n❌ No se pudo descargar ningún dato.")
