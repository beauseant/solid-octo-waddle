import argparse
import pandas as  pd
import os


def df_to_parquet(df, target_dir, chunk_size=500, **parquet_wargs):
	for i in range(0, len(df), chunk_size):
    		slc = df.iloc[i : i + chunk_size]
    		chunk = int(i/chunk_size)
    		fname = os.path.join(target_dir, f"part_{chunk:04d}.parquet")
    		slc.to_parquet(fname, engine="pyarrow", **parquet_wargs)

if __name__ == "__main__":
   parser = argparse.ArgumentParser(
        description="Convierte a texto los ficheros PDF pasados como parámetro",
        formatter_class=argparse.RawTextHelpFormatter
    )
   parser.add_argument("parquet_file", help="Ruta al fichero Parquet de entrada (el que fue generado por el script de descarga).")
   parser.add_argument("save_path", help="Directorio destino donde salvar el dataframe split")
   args = parser.parse_args()
   df = pd.read_parquet(args.parquet_file)
   df_to_parquet (df, args.save_path)
