# solid-octo-waddle
Descarga licitaciones PLACE
Veamos.... Los pasos que he seguido se pueden resumir en que he tomado el fichero parquet en "bruto" (la descarga de Manu) y:

     Filtrarlo para el año 2024, quitarle el multi índice, quedarnos sólo con las columnas que nos interesan, renombrar las que sean muy largas.
     Añadir una nueva columna, llamada url, que es un diccionario con dos datos, la url al pliego técnico y la url al pliego administrativo.
     Con un script en python se recorren todas esas urls y se intenta la descarga. El script se ejecuta en multhilo para hacerlo más rápido y crea cuatro nuevas columnas. Es capaz, además, de continuar con los que falten si ha habido un fallo o nos bloquean las descargas. En cuanto a las columnas, dos contienen las rutas locales a los PDFs y dos los resultados: resultado_tecnico y resultado_administrativo que pueden tener los siguiente valores:
            HTTP: {respuesta.status_connection}
            "Descargado correctamente.
    Mediante otro script en python, también multihilo, se procesan todos los paths válidos del punto anterior y se mira si el texto es OCR o es posible convertirlo a texto. Se hace una conversión rápida y sólo interesa saber el tipo:
                MSG_PDF_ESCANEO = "[AVISO: PDF sin texto extraíble (posiblemente escaneado)]"
                MSG_NO_EXISTE = "[ERROR: El fichero PDF no existe en la ruta especificada]"
                MSG_RUTA_INVALIDA = "[ERROR: La ruta al PDF es inválida o no se descargó]"
                MSG_PDF_TEXTO = '[CORRECTO: El pdf contiene texto]'
    Se divide el parquet anterior en parquets más pequeños que son bloques de 500 licitaciones para poder manejarlo con poca memoria.
    Se pasa otro script que, para cada uno de esos bloques, convierte a texto el PDF del punto 3.

El punto 1 y 2 se hacen mediante un notebook, que es el que usé para hacer exploración de datos, el notebook se llama: AnalisisDatosCobertura.ipynb
