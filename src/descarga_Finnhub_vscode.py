import requests
import os
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
# Asegúrate de que la ruta al archivo .env sea la correcta.
# Si el script está en la misma carpeta que el .env, load_dotenv() es suficiente.
# Si no, proporciona la ruta completa como se muestra abajo.
dotenv_path = '/Users/emiliosanchez/Desktop/MIAX/proyecto1_descarga_datos/proyecto1/.env'
load_dotenv(dotenv_path=dotenv_path)

# ¡CORRECCIÓN AQUÍ!
# Usamos os.getenv() para obtener la clave desde las variables de entorno.
# El nombre debe coincidir con el que tienes en tu archivo .env.
API_KEY = os.getenv('Finnhub_key')

# Verificamos si la API key se cargó correctamente.
if not API_KEY:
    raise ValueError("API key no encontrada. Asegúrate de que el archivo .env está en la ruta correcta y la variable se llama 'Finnhub_key'.")

# Lista de los tickers de las 10 acciones más importantes del NASDAQ
# (basado en capitalización de mercado).
tickers_nasdaq = [
    'NVDA',
    'MSFT',
    'AAPL',
    'AMZN',
    'META',
    'GOOG',
    'GOOGL',
    'TSLA',
    'AVGO',
    'NFLX'
]

print("Obteniendo datos de precios para las 10 acciones más importantes del NASDAQ...")
print("--------------------------------------------------")

# Iteramos sobre la lista de tickers
for ticker in tickers_nasdaq:
    # URL del endpoint para obtener el precio de una acción
    url_precios = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={API_KEY}"

    try:
        # Realizamos la solicitud GET
        respuesta_precios = requests.get(url_precios)

        # Verificamos si la solicitud fue exitosa
        respuesta_precios.raise_for_status()

        # Convertimos la respuesta a JSON
        datos_precios = respuesta_precios.json()

        # Extraer el precio actual y la variación diaria en porcentaje
        precio_actual = datos_precios.get('c')
        variacion_diaria = datos_precios.get('dp')

        if precio_actual is not None and variacion_diaria is not None:
            print(f"Ticker: {ticker}")
            print(f"  Precio actual: ${precio_actual}")
            print(f"  Variación diaria: {variacion_diaria}%")
        else:
            print(f"Ticker: {ticker} - No se pudieron obtener los datos de precios. Posiblemente no existan datos para este ticker o la API no los devolvió.")

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos para {ticker}: {e}")
XF