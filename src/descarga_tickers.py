import pandas as pd
import requests  
import yfinance as yf

def obtener_tickers_wikipedia():
    """
    Obtiene los tickers del S&P 500 y NASDAQ-100 desde Wikipedia
    y los devuelve en una lista única sin duplicados.
    """
    
    # --- URLs de Wikipedia ---
    url_sp500 = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    url_nasdaq100 = 'https://en.wikipedia.org/wiki/Nasdaq-100'

    # ---  Cabecera para simular ser un navegador ---
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.124 Safari/537.36'
    }
    # ----------------------------------------------------

    print("Obteniendo tickers del S&P 500...")
    try:
        # --- Obtener S&P 500 ---
        
        
        respuesta_sp500 = requests.get(url_sp500, headers=headers)
        # Verificamos que la solicitud fue exitosa
        respuesta_sp500.raise_for_status() 

        # Ahora le pasamos el *texto* HTML a pandas para que lea la tabla
        tabla_sp500 = pd.read_html(respuesta_sp500.text)[0]
        # ---------------------------------------------------------
        
        tickers_sp500 = tabla_sp500['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"Encontrados {len(tickers_sp500)} tickers del S&P 500.")
        
    except Exception as e:
        print(f"Error al obtener S&P 500: {e}")
        tickers_sp500 = []

    print("\nObteniendo tickers del NASDAQ-100...")
    try:
        # --- Obtener NASDAQ-100 ---
        
        # <--- MODIFICADO: Usamos 'requests' también aquí ---
        respuesta_nasdaq100 = requests.get(url_nasdaq100, headers=headers)
        respuesta_nasdaq100.raise_for_status()

        # En la página del Nasdaq-100, la tabla de componentes es la 5ta (índice 4).
        tabla_nasdaq100 = pd.read_html(respuesta_nasdaq100.text)[4]
        # ---------------------------------------------------------
        
        tickers_nasdaq100 = tabla_nasdaq100['Ticker'].str.replace('.', '-', regex=False).tolist().tolist()
        print(f"Encontrados {len(tickers_nasdaq100)} tickers del NASDAQ-100.")
        
    except Exception as e:
        print(f"Error al obtener NASDAQ-100: {e}")
        tickers_nasdaq100 = []

    # --- Combinar y crear la lista 'tickers' ---
    combined_tickers_set = set(tickers_sp500 + tickers_nasdaq100)
    tickers = sorted(list(combined_tickers_set))

    return tickers

# --- Ejecución del Script ---
if __name__ == "__main__":
    
    tickers = obtener_tickers_wikipedia()
    
    print(f"\n--- Total de tickers únicos combinados: {len(tickers)} ---")
    

    print(tickers[:])
data=[]
start_date = "2025-01-01"
end_date = "2025-01-10"
data=yf.download(tickers=tickers,start=start_date,end=end_date)
print(data)
