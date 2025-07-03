
from src.MultithreadProcess import processPDF

import argparse
import time 

from datetime import datetime
import json
import time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convierte a texto los ficheros PDF pasados como parámetro",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "parquet_file", 
        help="Ruta al fichero Parquet de entrada (el que fue generado por el script de descarga)."
    )
    
    parser.add_argument(
        "save_path", 
        help="Ruta en la que salvar los documentos en formato texto"
    )

    parser.add_argument(
        '-w', '--workers', help='number of workers', default=10
    )

    args = parser.parse_args()
    

    process = processPDF (args.parquet_file, args.save_path, int(args.workers))

    now = datetime.now()
    start_time = time.time()
    data = process.processFiles ()
    end_time = time.time()
    execution_time = end_time - start_time

    logdata ={'datetime':now.strftime('%Y-%m-%d'),
              'executiontime':execution_time //60,
              'inputfile': args.parquet_file,
              'numerfiles': len (data),
              'failedfiles':len ([d for d in data if d['result']==False]),
              }
    with open('log.json', 'w') as fp:
        json.dump(logdata, fp)