import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import finnhub
import yfinance as yf
from twelvedata import TDClient

# --- 1. CONFIGURACIÓN Y CARGA DE KEYS ---
load_dotenv()

# Obtiene las API Keys de .env
TWELVE_DATA_KEY = os.getenv("12data_key")
FINNHUB_KEY = os.getenv("finnhub_key")

# Símbolos de las acciones
TICKERS = ["AAPL", "TSLA", "GOOG", "MSFT"]

# Parámetros de tiempo
TODAY = datetime.now()
SIX_MONTHS_AGO = TODAY - timedelta(days=6 * 30) # Aproximación de 6 meses
START_DATE = SIX_MONTHS_AGO.strftime('%Y-%m-%d')
END_DATE = TODAY.strftime('%Y-%m-%d')
OUTPUT_FILENAME = f"datos_historicos_multiples_fuentes_{START_DATE}_to_{END_DATE}.csv"
print(f"🗓️ Rango de fechas: {START_DATE} a {END_DATE}")

# --- 2. FUNCIONES DE DESCARGA POR FUENTE ---

def get_twelvedata(symbol):
    """Descarga datos EOD de Twelve Data y devuelve un DataFrame de pandas."""
    if not TWELVE_DATA_KEY:
        print("❌ ERROR: Twelve Data API Key no encontrada.")
        return pd.DataFrame()
    
    try:
        td = TDClient(apikey=TWELVE_DATA_KEY)
        ts = td.time_series(
            symbol=symbol,
            interval="1day",
            start_date=START_DATE,
            end_date=END_DATE,
        )
        df = ts.as_pandas()
        df = df[['close', 'open', 'high', 'low', 'volume']] # Seleccionar y ordenar columnas
        df.columns = [f'{col}_12data' for col in df.columns] # Renombrar para diferenciar
        return df
    except Exception as e:
        print(f"❌ ERROR Twelve Data para {symbol}: {e}")
        return pd.DataFrame()

def get_finnhub(symbol):
    """Descarga datos EOD de Finnhub y devuelve un DataFrame de pandas."""
    if not FINNHUB_KEY:
        print("❌ ERROR: Finnhub API Key no encontrada.")
        return pd.DataFrame()
    
    try:
        finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)
        
        # Finnhub usa timestamps UNIX para las fechas
        t_start = int(SIX_MONTHS_AGO.timestamp())
        t_end = int(TODAY.timestamp())

        # Descarga de velas (candlesticks) diarias
        res = finnhub_client.stock_candles(symbol, 'D', t_start, t_end)
        
        # Verifica si hay datos
        if res and 's' in res and res['s'] == 'ok':
            df = pd.DataFrame(res)
            df['t'] = pd.to_datetime(df['t'], unit='s') # Convertir timestamp a datetime
            df = df.rename(columns={'c': 'close_finnhub', 'o': 'open_finnhub', 
                                    'h': 'high_finnhub', 'l': 'low_finnhub', 
                                    'v': 'volume_finnhub'})
            df = df.set_index('t')
            df.index.name = 'datetime' # Renombrar el índice para coincidir
            df = df[['close_finnhub', 'open_finnhub', 'high_finnhub', 'low_finnhub', 'volume_finnhub']]
            return df
        else:
            print(f"⚠️ Finnhub no devolvió datos 'ok' para {symbol}. Mensaje: {res.get('s')}")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ ERROR Finnhub para {symbol}: {e}")
        return pd.DataFrame()

def get_yfinance(symbol):
    """Descarga datos EOD de Yahoo Finance (sin API Key)."""
    try:
        # yfinance trabaja mejor con fechas de inicio y fin como strings
        data = yf.download(symbol, start=START_DATE, end=END_DATE, interval='1d', progress=False)
        
        # Renombrar y seleccionar columnas
        data = data.rename(columns={'Close': 'close_yfinance', 'Open': 'open_yfinance', 
                                    'High': 'high_yfinance', 'Low': 'low_yfinance', 
                                    'Volume': 'volume_yfinance'})
        data.index.name = 'datetime'
        data = data[['close_yfinance', 'open_yfinance', 'high_yfinance', 'low_yfinance', 'volume_yfinance']]
        return data
    except Exception as e:
        print(f"❌ ERROR Yahoo Finance para {symbol}: {e}")
        return pd.DataFrame()

# --- 3. FUNCIÓN PRINCIPAL DE DESCARGA Y CONSOLIDACIÓN ---

def descargar_consolidar_y_guardar():
    """Ejecuta la descarga de las 3 fuentes, consolida los datos y guarda en un solo CSV."""
    
    # Lista para almacenar los DataFrames consolidados por símbolo
    all_combined_dfs = []
    
    print("-" * 60)
    for symbol in TICKERS:
        print(f"\n🚀 PROCESANDO SÍMBOLO: **{symbol}**")
        
        # 1. Descargar de las 3 fuentes
        df_12data = get_twelvedata(symbol)
        df_finnhub = get_finnhub(symbol)
        df_yfinance = get_yfinance(symbol)

        # 2. Combinar los DataFrames
        # Usamos un 'outer' join en el índice 'datetime' para incluir todas las fechas
        # Aunque lo normal es que las fechas EOD sean las mismas.
        combined_df = pd.DataFrame()
        
        # Combinar: YFinance (base) con 12Data
        combined_df = df_yfinance.merge(df_12data, 
                                        left_index=True, 
                                        right_index=True, 
                                        how='outer')
        
        # Combinar: El resultado anterior con Finnhub
        combined_df = combined_df.merge(df_finnhub, 
                                        left_index=True, 
                                        right_index=True, 
                                        how='outer',
                                        suffixes=('_yfinance', '_finnhub'))

        if not combined_df.empty:
            # Añadir las columnas de Ticker y Fuente antes de apilar
            combined_df['Ticker'] = symbol
            
            # Reordenar columnas para que Ticker esté primero y la fecha sea el índice
            cols = ['Ticker'] + [col for col in combined_df.columns if col != 'Ticker']
            combined_df = combined_df[cols]
            
            all_combined_dfs.append(combined_df)
            print(f"✅ Consolidación exitosa para {symbol}. Filas: {len(combined_df)}")
        else:
            print(f"⚠️ No se pudo consolidar ningún dato para {symbol}.")

    print("-" * 60)
    
    # 4. Concatenar todos los DataFrames y guardar el archivo final
    if all_combined_dfs:
        final_df = pd.concat(all_combined_dfs)
        
        # Guardar el DataFrame final en un solo archivo CSV
        final_df.to_csv(OUTPUT_FILENAME)
        
        print(f"🎉 **ÉXITO**: Todos los datos se han consolidado y guardado en: **{OUTPUT_FILENAME}**")
        print("💡 Recuerda revisar el CSV final para ver las posibles diferencias entre las fuentes.")
    else:
        print("❌ FALLO: No se pudo descargar ni consolidar datos de ninguna fuente.")

if __name__ == "__main__":
    descargar_consolidar_y_guardar()