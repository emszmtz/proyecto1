![Diagrama de Flujo del Proyecto](FossFlow.png)
                            

# PROYECTO 1 MIAX 14: ANALIZADOR DE CARTERA 

Este proyecto es un notebook  diseñado para la primera práctica del Master MIAX 14. Es un análisis de un portafolio de acciones elegidas por el ususario.

El script descarga datos de mercado  y datos fundamentales de una lista de tickers, calcula la cartera de mínima volatilidad con CVXPY y genera visualizaciones y un resumen.

---

##  Características Principales

* **Descarga de Datos:** Obtiene datos históricos de precios y datos fundamentales (como información de la empresa, accionistas, recomendaciones) usando la librería `yfinance`.
* **Análisis Cuantitativo:**
    * Calcula y grafica los retornos logarítmicos.
    * Muestra una matriz de covarianzas de los activos.
    * Compara el rendimiento normalizado de los precios.
* **Optimización de Cartera:** Utiliza `cvxpy` para encontrar y graficar la cartera de mínima volatilidad (sin posiciones cortas).
* **Análisis Fundamental:** Itera sobre cada ticker para extraer y mostrar:
    * Resumen del negocio 
    * Principales accionistas institucionales.
    * Recomendaciones de analistas.
* **Reporte:** Genera un resumen final en formato Markdown.

