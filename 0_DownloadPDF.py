import argparse
import os
import pandas as pd
import requests
import json # Usado para manejar errores en la columna del diccionario
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Desactivar advertencias de InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class ProcesadorParquetPDF:
    """
    Clase para procesar un fichero Parquet, descargar PDFs desde un diccionario de URLs
    y guardar el resultado y la ruta de cada operación.
    """
    
    # Constante para el mensaje de éxito
    MSG_DESCARGA_OK = "Descargado correctamente"

    def __init__(self, parquet_path: str, download_folder: str):
        """
        Constructor de la clase.

        Args:
            parquet_path (str): Ruta al fichero .parquet.
            download_folder (str): Carpeta donde se guardarán los PDFs.
        """
        self.parquet_path = parquet_path
        self.download_folder = download_folder
        
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            print(f"Directorio de descargas creado en: '{self.download_folder}'")

        try:
            print(f"Cargando el fichero Parquet desde: {self.parquet_path}")
            self.df = pd.read_parquet(parquet_path)
            print("Fichero cargado correctamente.")
            self._validar_y_preparar_dataframe()
        except FileNotFoundError:
            print(f"Error: El fichero '{self.parquet_path}' no fue encontrado.")
            exit()

    def _validar_y_preparar_dataframe(self):
        """
        Valida que el DataFrame tenga las columnas necesarias y añade las 
        columnas de resultado y path si no existen.
        """
        columnas_requeridas = {'id', 'url'}
        if not columnas_requeridas.issubset(self.df.columns):
            print("Error: El fichero Parquet debe contener las columnas 'id' y 'url'.")
            exit()
        
        # Columnas a crear para cada tipo de documento
        columnas_nuevas = {
            'resultado_tecnico': None, 'path_tecnico': None,
            'resultado_administrativo': None, 'path_administrativo': None
        }
        
        for col, default_val in columnas_nuevas.items():
            if col not in self.df.columns:
                print(f"Columna '{col}' no encontrada. Añadiéndola al DataFrame.")
                self.df[col] = default_val

    def _descargar_una_url(self, url: str, output_path: str) -> tuple[str, str]:
        """
        Lógica para descargar un único PDF desde una URL.
        
        Args:
            url (str): La URL del PDF a descargar.
            output_path (str): La ruta completa donde guardar el fichero.

        Returns:
            tuple: (mensaje de resultado, ruta de salida si éxito)
        """
        if pd.isna(url) or not isinstance(url, str) or not url.startswith('http'):
            return "Error: URL inválida o vacía", ""

        try:
            respuesta = requests.get(url, timeout=30, verify=False)
            if respuesta.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(respuesta.content)
                return self.MSG_DESCARGA_OK, output_path
            else:
                return f"Error HTTP: {respuesta.status_code}", ""
        except requests.exceptions.RequestException as e:
            return f"Error de conexión: {type(e).__name__}", ""
        except Exception as e:
            return f"Error inesperado: {str(e)}", ""

    def _procesar_fila(self, fila, continuar: bool):
        """
        Procesa una fila completa, gestionando la descarga de los documentos
        'tecnico' y 'administrativo'.
        """
        idx, pdf_id, url_dict = fila.Index, fila.id, fila.url
        
        # Resultados iniciales por si algo falla antes de la descarga
        res_t, path_t = fila.resultado_tecnico, fila.path_tecnico
        res_a, path_a = fila.resultado_administrativo, fila.path_administrativo

        # Validar que el contenido de la columna 'url' es un diccionario
        if not isinstance(url_dict, dict):
            try:
                # Intenta cargar un string que parece un diccionario
                url_dict = json.loads(url_dict.replace("'", "\""))
            except (json.JSONDecodeError, AttributeError):
                msg = "Error: La columna 'url' no contiene un diccionario válido."
                return idx, msg, "", msg, ""
        
        # --- Procesar documento TÉCNICO ---
        if not continuar or res_t != self.MSG_DESCARGA_OK:
            url_tecnico = url_dict.get('tecnico')
            ruta_salida_t = os.path.join(self.download_folder, f"{pdf_id}_tecnico.pdf")
            res_t, path_t = self._descargar_una_url(url_tecnico, ruta_salida_t)

        # --- Procesar documento ADMINISTRATIVO ---
        if not continuar or res_a != self.MSG_DESCARGA_OK:
            url_admin = url_dict.get('administrativo')
            ruta_salida_a = os.path.join(self.download_folder, f"{pdf_id}_administrativo.pdf")
            res_a, path_a = self._descargar_una_url(url_admin, ruta_salida_a)

        return idx, res_t, path_t, res_a, path_a

    def procesar_descargas(self, continuar: bool = False):
        """
        Orquesta el proceso de descarga de los PDFs de forma concurrente.
        """
        df_a_procesar = self.df
        
        if continuar:
            # Una fila necesita ser procesada si CUALQUIERA de sus documentos no está OK
            condicion = (self.df['resultado_tecnico'] != self.MSG_DESCARGA_OK) | \
                        (self.df['resultado_administrativo'] != self.MSG_DESCARGA_OK)
            df_a_procesar = self.df[condicion]
            print ("Vamos a procesar %s documentos" % (df_a_procesar.shape[0]))
            
            if df_a_procesar.empty:
                print("No hay ficheros pendientes o con error. ¡Todo está descargado!")
                return
            print(f"Modo 'continuar' activado. Se verificarán {len(df_a_procesar)} filas pendientes o con error.")
        else:
            print(f"Iniciando procesamiento de {len(df_a_procesar)} filas.")

        with ThreadPoolExecutor(max_workers=10) as executor:
            futuros = [executor.submit(self._procesar_fila, fila, continuar) for fila in df_a_procesar.itertuples()]
            
            for futuro in tqdm(as_completed(futuros), total=len(futuros), desc="Procesando filas"):
                try:
                    idx, res_t, path_t, res_a, path_a = futuro.result()
                    # Actualizar las cuatro columnas de forma atómica para esa fila
                    self.df.loc[idx, ['resultado_tecnico', 'path_tecnico', 'resultado_administrativo', 'path_administrativo']] = [res_t, path_t, res_a, path_a]
                except Exception as e:
                    print(f"\nOcurrió un error grave procesando una fila: {e}")

        print("\nProceso finalizado.")
        self.guardar_resultados()

    def guardar_resultados(self):
        """
        Guarda el DataFrame actualizado en el fichero Parquet.
        """
        try:
            print(f"Guardando resultados en: {self.parquet_path}")
            self.df.to_parquet(self.parquet_path, index=False)
            print("¡Resultados guardados con éxito!")
        except Exception as e:
            print(f"Error al guardar el fichero Parquet: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Descarga PDFs (técnico y administrativo) desde un fichero Parquet.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("parquet_file", help="Ruta al fichero de entrada .parquet")
    parser.add_argument("download_folder", help="Ruta a la carpeta donde se guardarán las descargas")
    parser.add_argument(
        "--continuar", action="store_true",
        help="Opcional: Reanuda la descarga, procesando solo los ficheros que fallaron."
    )
    
    args = parser.parse_args()
    
    procesador = ProcesadorParquetPDF(
        parquet_path=args.parquet_file,
        download_folder=args.download_folder
    )
    procesador.procesar_descargas(continuar=args.continuar)
