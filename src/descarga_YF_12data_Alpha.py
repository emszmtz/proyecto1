import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import json
import yfinance as yf
from twelvedata import TDClient
from dataclasses import dataclass, asdict

# ==========================
# 0.1 GENERAMOS EL @DATACLASS
# ===========================
@dataclass
class StockData: #Cambio
    symbol: str
    date: date # nos interesa solo la fecha
    open: float 
    high: float 
    low: float
    close: float 
    volume: float 
    fuente_datos: str = ""

# ==============================================================================
# 1. CONFIGURACIÓN INICIAL
# ==============================================================================

# Cargar claves desde .env
load_dotenv()
ALPHA_KEY = os.getenv("alpha_key")
TWELVE_DATA_KEY = os.getenv("12data_key")

if not ALPHA_KEY or not TWELVE_DATA_KEY:
    #Print ("No se han encontrado las claves") -> mejor con SystemExit
    raise SystemExit("No se encontraron las API keys en el .env")


TICKERS = ["GOOG", "AAPL"] # !!(En un fututo aquí pedir los valores que se quieren analizar por pantalla)
end_date = datetime.today() #!!Quizá poner una opción si quieres hasta el día de hoy o en otra fecha
# !! aqui con la fecha, pedir introducir por pantalla los días con los que se quiere hacer por ahora he puesto 60


start_date = end_date - timedelta(days=60)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# --- RUTA DE SALIDA --- 
# Mantenemos la ruta absoluta fija como 'output_dir'
output_dir = "/Users/emiliosanchez/Desktop/MIAX/proyecto1_descarga_datos/proyecto1/salida_datos"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# generamos un nombre de archivo con el nombre de los activos y fecha y hora
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
tickers_str = '_'.join(TICKERS)  # transforma la lista de tickers en una string para añadirla al nombre
file_name = f"datos_{tickers_str}_{timestamp}.csv"

# Unimos la ruta de la carpeta con el nombre del archivo para tener la ruta de salida final
output_file = os.path.join(output_dir, file_name)

# ==============================================================================
# 2. FUNCIONES DE DESCARGA DE DATOS
# ==============================================================================


# 2.1 DESCARGA DE ALPHA VANTAGE
def descargar_alpha_vantage(symbol):
    """Descarga datos diarios de Alpha Vantage."""
    print(f"  -> Obteniendo datos de Alpha Vantage para {symbol}...")
    try:
        # Usamos el timeout desglosado (conexión, lectura) para más robustez #Cambio
        request= requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={ALPHA_KEY}", timeout=(5, 45)) #Cambio
        request.raise_for_status()
        data = request.json()
        #Buscamos el índice con un bucle 
        # Está un poco con pinzas, si cambia la forma de datos que proporciona Alpha vintage, todo se cae.
        #Hay que buscar una forma de hacerlo más robusto. (Con un Wraper de Alpha vintage?)
        key = None
        for k in data:
            if "Time Series" in k:
                key = k
                break # Detiene el bucle en cuanto encuentra el primer resultado

        # Corregido: El 'if not key' debe estar fuera del bucle #Cambio
        if not key: #Cambio
            print(f"  ⚠️  {symbol} (Alpha Vantage): No se encontró 'Time Series'.") #Cambio
            return None #Cambio
        #Esto es un poco flojo, pero no se me ocurre cómo hacerlo de otra forma. Dependo total de que Alpha me siga dando los datos como lo hace siempre.
        #De nuevo, si cambia la forma que me da los datos todo se cae.

        df = pd.DataFrame.from_dict(data[key], orient='index', dtype=float)
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= start_date]
        
        # Estandarizar nombres de columnas (Alpha Vantage usa "1. open", "2. high", etc.) #Cambio
        df.columns = df.columns.str.replace(r'^\d+\.\s*', '', regex=True).str.lower() #Cambio
        
        # Asegurarnos de que solo tenemos las columnas necesarias #Cambio
        df = df[['open', 'high', 'low', 'close', 'volume']] #Cambio
        
        # Añadir 'symbol' y 'date' como columnas estándar #Cambio
        df['symbol'] = symbol #Cambio
        df['date'] = df.index.to_pydatetime() #Cambio
        df = df.reset_index(drop=True) #Cambio
            
        print(f"  {symbol} (Alpha Vantage): {len(df)} filas descargadas.")
        return df
    except Exception as error:
        print(f"  {symbol} (Alpha Vantage): Ocurrió un error - {error}")
        return None
    

#2.2 DESCARGA DESDE YAHOO FINANCE
def descargar_yfinance(symbol):
    """Descarga y limpia datos diarios de Yahoo Finance, corrigiendo el orden de las operaciones."""
    print(f"  -> Obteniendo datos de Yahoo Finance para {symbol}...")
    try:
        df = yf.download(symbol, start=start_date_str, end=end_date_str, auto_adjust=True)
        
        if df.empty:
            print(f"  ⚠️  {symbol} (Yahoo Finance): No se encontraron datos.")
            return None
        
        # Aplanar MultiIndex, si no da problemas al hacer el pd.concatisinstance() 
        # es una función que comprueba si las columnas de tu tabla de datos (df.columns) son de tipo MultiIndex.
        # Si la respuesta es sí, la siguiente línea se ejecuta eliminando la columna
        #lo de preguntar es una mera formalidad vaya, porque en Yahoo es que sí
   
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        
        # 1. Resetear el índice PRIMERO para que 'Date' se convierta en una columna
        df = df.reset_index()
        
        # 2. AHORA, convertir todos los nombres de columnas a minúsculas.
        df.columns = df.columns.str.lower()
        
        # Renombrar 'adj close' si existe
        if 'adj close' in df.columns:
            df = df.rename(columns={'adj close': 'adj_close'})

        # Añadir columnas de metadatos
        df['symbol'] = symbol #Cambio
        # Eliminamos la lógica de 'data_list.append', la función solo debe devolver el DF #Cambio
        
        print(f"  {symbol} (Yahoo Finance): {len(df)} filas descargadas.")
        return df
    except Exception as e:
        print(f"   {symbol} (Yahoo Finance): Ocurrió un error - {e}")
        return None

# 2.3 DESCARGA DESDE 12 DATA
def descargar_twelve_data(symbol):
    """Descarga datos diarios de Twelve Data."""
    print(f"  -> Obteniendo datos de Twelve Data para {symbol}...")
    try:
        td = TDClient(apikey=TWELVE_DATA_KEY)
        ts = td.time_series(symbol=symbol, interval="1day", start_date=start_date_str, end_date=end_date_str, outputsize=5000)
        if ts is None: return None
        #La API de 12data nos da el órden cronológico inverso así que lo invierto con iloc[::-1]
        df = ts.as_pandas().iloc[::-1].reset_index().rename(columns={'datetime': 'date'})
        df['symbol'] = symbol #Cambio
        # Eliminamos la lógica de 'data_list.append', la función solo debe devolver el DF #Cambio

        print(f"   {symbol} (Twelve Data): {len(df)} filas descargadas.")
        return df
    except Exception as error:
        print(f"  {symbol} (Twelve Data): Ocurrió un error - {error}")
        return None

# ============================================================================== #Cambio
# 2.4 FUNCIÓN AUXILIAR PARA CONVERTIR DF A DATACLASS #Cambio
# ============================================================================== #Cambio
def df_a_dataclass(df, source_name): #Cambio
    """Convierte un DataFrame de Pandas estandarizado a una lista de objetos StockData.""" #Cambio
    data_records = [] #Cambio
    
    # Aseguramos que la fecha sea un objeto 'date' y no 'datetime' #Cambio
    df['date'] = pd.to_datetime(df['date']).dt.date #Cambio
    df['fuente_datos'] = source_name #Cambio
    
    # Columnas exactas que necesita el dataclass #Cambio
    cols_needed = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'fuente_datos'] #Cambio
    df_filtered = df[cols_needed] #Cambio

    for row in df_filtered.to_dict('records'): #Cambio
        data_records.append(StockData(**row)) #Cambio
    
    return data_records #Cambio

# ==============================================================================
# 3. PROCESO PRINCIPAL
# ==============================================================================
#Activamos las funciones del código
#redondeamos a 2 decimales, aunque el único que nos da más de 2 es yfinance.

all_data_records = [] #Cambio
for i, ticker in enumerate(TICKERS):
    print(f"\n[{i+1}/{len(TICKERS)}] Procesando ticker: {ticker}")
    
    # --- Yahoo Finance --- #Cambio
    df_yf = descargar_yfinance(ticker)
    if df_yf is not None: #Cambio
        df_yf = df_yf.round(2) #Cambio
        all_data_records.extend(df_a_dataclass(df_yf, "Yahoo Finance")) #Cambio
        
    # --- Alpha Vantage --- #Cambio
    df_av = descargar_alpha_vantage(ticker)
    if df_av is not None: #Cambio
        df_av = df_av.round(2) #Cambio
        all_data_records.extend(df_a_dataclass(df_av, "Alpha Vantage")) #Cambio

    # --- Twelve Data --- #Cambio
    df_td = descargar_twelve_data(ticker)
    if df_td is not None: #Cambio
        df_td = df_td.round(2) #Cambio
        all_data_records.extend(df_a_dataclass(df_td, "Twelve Data")) #Cambio

# ==============================================================================
# 4. GUARDADO DE DATOS
# ==============================================================================

if all_data_records: #Cambio
    # Convertimos la lista de objetos dataclass en un DataFrame final #Cambio
    df_final = pd.DataFrame([asdict(record) for record in all_data_records]) #Cambio

    #contamos cuantos datos nos ha devuelto cada plataforma
    # Aquí estaría bien un filtro para datos faltantes. 
    print(f"\n📊 Distribución de datos por fuente en el DataFrame final:")
    print(df_final['fuente_datos'].value_counts()) #Cambio
    
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final = df_final.sort_values(by=['symbol', 'date', 'fuente_datos']).reset_index(drop=True) #Cambio
    
    # Seleccionar solo las columnas deseadas para el archivo final
    cols_ordered = ['symbol', 'date', 'fuente_datos', 'open', 'high', 'low', 'close', 'volume'] #Cambio
    
    # Filtrar solo a las columnas que existen en el df final para evitar errores
    cols_to_keep = [col for col in cols_ordered if col in df_final.columns]
    df_final = df_final[cols_to_keep]

    df_final.to_csv(output_file, index=False, date_format='%Y-%m-%d')
    
    print(f"\n🎉 ¡Proceso completado!")
    print(f"✅ Datos guardados en: {output_file}")
    print(f"Total de filas consolidadas: {len(df_final)}")
else:
    print("\n❌ No se pudo descargar ningún dato.")