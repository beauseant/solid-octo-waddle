import argparse
import os
import pandas as pd
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class PDFTextExtractor:
    """
    Clase para leer un fichero Parquet, abrir los PDFs indicados en las columnas
    de rutas, extraer su texto y guardar el resultado en nuevas columnas.
    """
    # Mensajes estandarizados
    MSG_PDF_ESCANEO = "[ERROR: PDF sin texto extraíble (posiblemente escaneado)]"
    MSG_NO_EXISTE = "[ERROR: El fichero PDF no existe en la ruta especificada]"
    MSG_RUTA_INVALIDA = "[AVISO: La ruta al PDF es inválida o no se descargó]"

    def __init__(self, parquet_path: str):
        """
        Constructor de la clase.
        Carga el DataFrame y prepara las nuevas columnas.

        Args:
            parquet_path (str): Ruta al fichero .parquet de entrada.
        """
        self.parquet_path = parquet_path
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
        Valida que existan las columnas de rutas y crea las columnas
        de texto si es necesario.
        """
        columnas_requeridas = {'path_tecnico', 'path_administrativo'}
        if not columnas_requeridas.issubset(self.df.columns):
            print("Error: El Parquet no contiene las columnas 'path_tecnico' y/o 'path_administrativo'.")
            print("Asegúrate de ejecutar primero el script de descarga.")
            exit()
        
        columnas_texto = ['texto_tecnico', 'texto_administrativo']
        for col in columnas_texto:
            if col not in self.df.columns:
                print(f"Creando columna de resultado '{col}'.")
                self.df[col] = None

    def _extraer_texto_de_un_pdf(self, pdf_path: str) -> str:
        """
        Extrae el contenido de texto de un único fichero PDF.

        Args:
            pdf_path (str): Ruta al fichero PDF.

        Returns:
            str: El texto extraído o un mensaje de error/aviso.
        """
        # 1. Validar la ruta de entrada
        if pd.isna(pdf_path) or not pdf_path:
            return self.MSG_RUTA_INVALIDA
        
        if not os.path.exists(pdf_path):
            return self.MSG_NO_EXISTE
        
        # 2. Intentar extraer el texto
        try:
            documento = fitz.open(pdf_path)
            
            partes_texto = [page.get_text() for page in documento]
            texto_completo = "".join(partes_texto)
            
            documento.close()
            
            # 3. Comprobar si se extrajo texto real
            if not texto_completo.strip():
                return self.MSG_PDF_ESCANEO
            
            return texto_completo

        except Exception as e:
            return f"[ERROR: No se pudo procesar el PDF - {type(e).__name__}]"

    def _procesar_fila(self, fila):
        """
        Toma una fila del DataFrame y procesa los PDFs asociados a ella.
        """
        idx = fila.Index
        
        # Extraer texto de ambos documentos
        texto_t = self._extraer_texto_de_un_pdf(fila.path_tecnico)
        texto_a = self._extraer_texto_de_un_pdf(fila.path_administrativo)
        
        return idx, texto_t, texto_a

    def procesar_todos_los_pdfs(self):
        """
        Orquesta el proceso de extracción de texto para todas las filas
        del DataFrame de forma concurrente.
        """
        print(f"Iniciando extracción de texto para {len(self.df)} filas.")
        
        # Usamos ThreadPoolExecutor para paralelizar la lectura de ficheros
        with ThreadPoolExecutor(max_workers=1) as executor:
            futuros = [executor.submit(self._procesar_fila, fila) for fila in self.df.itertuples()]
            
            for futuro in tqdm(as_completed(futuros), total=len(futuros), desc="Extrayendo texto"):
                try:
                    idx, texto_t, texto_a = futuro.result()
                    # Actualizar las dos nuevas columnas en el DataFrame
                    self.df.loc[idx, ['texto_tecnico', 'texto_administrativo']] = [texto_t, texto_a]
                except Exception as e:
                    print(f"\nOcurrió un error grave procesando una fila: {e}")
        
        print("\nProceso de extracción finalizado.")
        self.guardar_resultados()

    def guardar_resultados(self):
        """
        Guarda el DataFrame actualizado, sobrescribiendo el fichero Parquet original.
        """
        try:
            print(f"Guardando resultados en: {self.parquet_path}")
            self.df.to_parquet(self.parquet_path, index=False)
            print("¡Fichero Parquet actualizado con el texto extraído!")
        except Exception as e:
            print(f"Error al guardar el fichero Parquet: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extrae texto de los PDFs referenciados en un fichero Parquet.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "parquet_file", 
        help="Ruta al fichero Parquet de entrada (el que fue generado por el script de descarga)."
    )
    
    args = parser.parse_args()
    
    extractor = PDFTextExtractor(parquet_path=args.parquet_file)
    extractor.procesar_todos_los_pdfs()
