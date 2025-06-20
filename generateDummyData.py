import pandas as pd

def crear_parquet_de_prueba(nombre_fichero: str = "datos.parquet"):
    """
    Crea un fichero Parquet de prueba con datos de ejemplo.
    """
    print("Creando DataFrame de prueba...")

    # Define los datos para el DataFrame
    # - Una URL válida para que la descarga sea correcta.
    # - Una URL que probablemente dará un error 404 (No Encontrado).
    # - Una URL a un dominio que no existe para probar errores de conexión.
    # - Una entrada con una URL vacía/inválida para probar la validación.
    datos = {
        'id': [
            'id1', 
            'id2', 
        ],
        'url': [
            {'tecnico':'https://contractaciopublica.cat/portal-api/descarrega-document/300136132/80F62F2579EA4C2F2C18AA91062ABDF4','administrativo':'https://molletecontractaciopublica.cat/portal-api/descarrega-document/300136138/39BB0CBD1AD239EBD0D2ADC684AD1EC0'},
            {'tecnico': 'https://example.com/un_pdf_que_no_existe.pdf','administrativo':'https://contrataciondelestado.es/wps/wcm/connect/PLACE_es/Site/area/docAccCmpnt?srv=cmpnt&cmpntname=GetDocumentsById&source=library&DocumentIdParam=7695fbca-4c85-41c7-b4e6-efbe44df3900'},
        ]
    }

    # Crea el DataFrame
    df = pd.DataFrame(datos)

    # Guarda el DataFrame en formato Parquet
    try:
        df.to_parquet(nombre_fichero, index=False)
        print(f"¡Éxito! Fichero de prueba '{nombre_fichero}' creado correctamente.")
        print("\nContenido del fichero:")
        print(df)
    except Exception as e:
        print(f"Ocurrió un error al guardar el fichero: {e}")

if __name__ == "__main__":
    crear_parquet_de_prueba()