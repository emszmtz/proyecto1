import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import yfinance as yf
from twelvedata import TDClient

# ==============================================================================
# 1. CONFIGURACIÓN INICIAL
# ==============================================================================

# Cargar claves desde .env
load_dotenv()
ALPHA_KEY = os.getenv("alpha_key")
TWELVE_DATA_KEY = os.getenv("12data_key")

if not ALPHA_KEY or not TWELVE_DATA_KEY:
    raise SystemExit("❌ No se encontraron las claves 'alpha_key' o '12data_key' en el .env")

# --- PARÁMETROS ---
TICKERS = ["GOOG", "AAPL"]
end_date = datetime.today()
start_date = end_date - timedelta(days=60)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# --- RUTA DE SALIDA ---
output_dir = "salida_datos"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
output_file = os.path.join(output_dir, "datos_consolidados_final.csv")

# ==============================================================================
# 2. FUNCIONES DE DESCARGA DE DATOS
# ==============================================================================

def descargar_alpha_vantage(symbol):
    """Descarga datos diarios de Alpha Vantage."""
    print(f"  -> Obteniendo datos de Alpha Vantage para {symbol}...")
    try:
        r = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={ALPHA_KEY}", timeout=15)
        r.raise_for_status()
        data = r.json()
        key = next((k for k in data.keys() if "Time Series" in k), None)
        if not key:
            print(f"  ⚠️  {symbol} (Alpha Vantage): No se encontró 'Time Series'.")
            return None
        df = pd.DataFrame.from_dict(data[key], orient='index', dtype=float)
        df.index = pd.to_datetime(df.index)
        df = df.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'})
        df = df[df.index >= start_date].reset_index().rename(columns={'index': 'date'})
        df['symbol'] = symbol
        df['source'] = 'Alpha Vantage'
        print(f"  ✅ {symbol} (Alpha Vantage): {len(df)} filas descargadas.")
        return df
    except Exception as e:
        print(f"  ❌ {symbol} (Alpha Vantage): Ocurrió un error - {e}")
        return None

def descargar_yfinance(symbol):
    """Descarga y limpia datos diarios de Yahoo Finance, corrigiendo el orden de las operaciones."""
    print(f"  -> Obteniendo datos de Yahoo Finance para {symbol}...")
    try:
        df = yf.download(symbol, start=start_date_str, end=end_date_str, 
                         progress=False, auto_adjust=False)
        
        if df.empty:
            print(f"  ⚠️  {symbol} (Yahoo Finance): No se encontraron datos.")
            return None
        
        # Aplanar MultiIndex si existe
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        # --- CORRECCIÓN DE ORDEN ---
        # 1. Resetear el índice PRIMERO para que 'Date' se convierta en una columna.
        df = df.reset_index()
        
        # 2. AHORA, convertir todos los nombres de columnas a minúsculas.
        df.columns = df.columns.str.lower()
        
        # Renombrar 'adj close' si existe
        if 'adj close' in df.columns:
            df = df.rename(columns={'adj close': 'adj_close'})

        # Añadir columnas de metadatos
        df['symbol'] = symbol
        df['source'] = 'Yahoo Finance'
        
        print(f"  ✅ {symbol} (Yahoo Finance): {len(df)} filas descargadas.")
        return df
    except Exception as e:
        print(f"  ❌ {symbol} (Yahoo Finance): Ocurrió un error - {e}")
        return None

def descargar_twelve_data(symbol):
    """Descarga datos diarios de Twelve Data."""
    print(f"  -> Obteniendo datos de Twelve Data para {symbol}...")
    try:
        td = TDClient(apikey=TWELVE_DATA_KEY)
        ts = td.time_series(symbol=symbol, interval="1day", start_date=start_date_str, end_date=end_date_str, outputsize=5000)
        if ts is None: return None
        df = ts.as_pandas().iloc[::-1].reset_index().rename(columns={'datetime': 'date'})
        df['symbol'] = symbol
        df['source'] = 'Twelve Data'
        print(f"  ✅ {symbol} (Twelve Data): {len(df)} filas descargadas.")
        return df
    except Exception as e:
        print(f"  ❌ {symbol} (Twelve Data): Ocurrió un error - {e}")
        return None

# ==============================================================================
# 3. PROCESO PRINCIPAL
# ==============================================================================

all_dataframes = []
for i, ticker in enumerate(TICKERS):
    print(f"\n[{i+1}/{len(TICKERS)}] Procesando ticker: {ticker}")
    
    df_yf = descargar_yfinance(ticker)
    if df_yf is not None: all_dataframes.append(df_yf)
        
    df_av = descargar_alpha_vantage(ticker)
    if df_av is not None: all_dataframes.append(df_av)

    df_td = descargar_twelve_data(ticker)
    if df_td is not None: all_dataframes.append(df_td)

# ==============================================================================
# 4. GUARDADO DE DATOS
# ==============================================================================

if all_dataframes:
    df_final = pd.concat(all_dataframes, ignore_index=True)

    print(f"\n📊 Distribución de datos por fuente en el DataFrame final:")
    print(df_final['source'].value_counts())
    
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final = df_final.sort_values(by=['symbol', 'date', 'source']).reset_index(drop=True)
    
    # Seleccionar solo las columnas deseadas para el archivo final
    cols_ordered = ['symbol', 'date', 'source', 'open', 'high', 'low', 'close', 'volume']
    
    # Filtrar solo a las columnas que existen en el df final para evitar errores
    cols_to_keep = [col for col in cols_ordered if col in df_final.columns]
    df_final = df_final[cols_to_keep]

    df_final.to_csv(output_file, index=False, date_format='%Y-%m-%d')
    
    print(f"\n🎉 ¡Proceso completado!")
    print(f"✅ Datos guardados en: {output_file}")
    print(f"Total de filas consolidadas: {len(df_final)}")
else:
    print("\n❌ No se pudo descargar ningún dato.")