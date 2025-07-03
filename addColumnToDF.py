import argparse
import pandas as  pd
import os
from pathlib import Path
from tqdm import tqdm        # asegúrate de: pip install tqdm

def unify_colname(col):
    return ".".join([el for el in col if el])

def procesar_parquet(df: pd.DataFrame, fichero: Path, destino: Path) -> None:
    df_RED = pd.read_parquet(fichero)
    ruta = destino / fichero.name

    #import ipdb ; ipdb.set_trace()

    df_merged = pd.merge(df, df_RED, on='place_id')
    df_merged.to_parquet (str(ruta))
    

    '''
        Comprobar resultados:
    
    place_id_prueba = str (df_RED.iloc[100]['place_id'])
    data_mer = df_merged[df_merged['place_id'] == place_id_prueba]
    data_raw = df_PLACE[df_PLACE['place_id'] == place_id_prueba]
    si es correcto = data_mer['title'] == data_raw['title']
    '''



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convierte a texto los ficheros PDF pasados como parámetro",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("parquet_origen_raw", help="Ruta al fichero Parquet de entrada, la descarga en bruto, ej: /export/data_ml4ds/NextProcurement/Junio_2025/pliegosPlace/DatosBruto_outsiders.parquet")
    parser.add_argument("parquet_dir_origen_red", help="Ruta al fichero Parquet de entrada, el reducido, ej:/export/data_ml4ds/NextProcurement/Junio_2025/pliegosPlace/red_data_outsiders_2024_chunks/part_0000.parquet")
    parser.add_argument("columna", help="Columna que queremos añadir")
    parser.add_argument("parquet_dir_destino", help="Ruta al fichero Parquet de salida, será el reducido con la columna")
    args = parser.parse_args()

    df_PLACE = pd.read_parquet(args.parquet_origen_raw)
    df_PLACE = df_PLACE.rename(columns={"id":"place_id"})

    df_PLACE.columns = [unify_colname(col) for col in df_PLACE.columns]
    df_PLACE = df_PLACE[['place_id', args.columna]]


    parquets = sorted(Path(args.parquet_dir_origen_red).glob("*.parquet"))        # lista de rutas .parquet

    destino = Path (args.parquet_dir_destino)
    destino.mkdir(parents=True, exist_ok=True)

    for fichero in tqdm(parquets,
                    desc="Procesando ficheros parquet",
                    unit="fichero"):
        procesar_parquet(df_PLACE, fichero, destino)

    '''
    df_PLACE = pd.read_parquet(args.parquet_origen_raw)
    df_PLACE = df_PLACE.rename(columns={"id":"place_id"})

    df_PLACE.columns = [unify_colname(col) for col in df_PLACE.columns]
    df_PLACE = df_PLACE[['place_id', args.columna]]

    df_RED = pd.read_parquet(args.parquet_origen_red)

    #import ipdb ; ipdb.set_trace()

    df_merged = pd.merge(df_PLACE, df_RED, on='place_id')
    df_merged.to_parquet (args.parquet_destino)
    '''

    '''
        Comprobar resultados:
    
    place_id_prueba = str (df_RED.iloc[100]['place_id'])
    data_mer = df_merged[df_merged['place_id'] == place_id_prueba]
    data_raw = df_PLACE[df_PLACE['place_id'] == place_id_prueba]
    si es correcto = data_mer['title'] == data_raw['title']
    '''

