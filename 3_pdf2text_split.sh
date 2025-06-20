#!/bin/bash

# Comprueba si se ha proporcionado un directorio como argumento
if [ -z "$1" ]; then
  echo "Uso: $0 <directorio>"
  echo "Ejemplo: $0 /ruta/a/tu/directorio 'ls -l'"
  echo "  El segundo argumento es opcional, si no se provee, se ejecutará 'echo' por defecto."
  exit 1
fi

# El directorio a procesar
TARGET_DIR="$1"

# El comando a ejecutar (por defecto 'echo' si no se provee)
COMMAND_TO_RUN="${2:-python 2_ConvertPDF2Text.py}"

# Comprueba si el directorio existe
if [ ! -d "$TARGET_DIR" ]; then
  echo "Error: El directorio '$TARGET_DIR' no existe."
  exit 1
fi

echo "Procesando archivos en: $TARGET_DIR"
echo "Comando a ejecutar para cada archivo: $COMMAND_TO_RUN"
echo "---"

# Itera sobre cada archivo en el directorio
# '-f' asegura que solo procesamos archivos regulares y no directorios
for file in "$TARGET_DIR"/*; do
  if [ -f "$file" ]; then
    echo "Ejecutando '$COMMAND_TO_RUN \"$file\"' en: $file"
    # Ejecuta el comando con el nombre del archivo como argumento
    eval "$COMMAND_TO_RUN \"$file\""
    echo "---"
    clear
  fi
done

echo "Script finalizado."
