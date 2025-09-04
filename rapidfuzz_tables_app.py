from fuzzy_match_utils import execute_dynamic_matching
import pandas as pd
import os

def display_results(resultados, as_dataframe=True):
    if as_dataframe:
        df = pd.DataFrame(resultados)
        print(df)
        return df
    else:
        print(resultados)
        return resultados

def export_results(df, filename, filetype="csv", limit=None):
    # Validar resultados vacíos
    if df.empty:
        print("No hay datos para exportar. El archivo no se ha creado.")
        return False

    # Limitar número de filas si se especifica
    if limit is not None and isinstance(limit, int):
        if limit == 0:
            print("No es posible exportar 0 filas. Por favor, intenta de nuevo con un número mayor a 0.")
            return False
        df = df.head(limit)

    # Definir carpeta según tipo de archivo
    folder = "csv" if filetype == "csv" else "xlsx"
    base_filename = os.path.basename(filename)
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    final_path = os.path.join(folder, base_filename)
    if not final_path.endswith(f".{filetype}"):
        final_path += f".{filetype}"

    if filetype == "csv":
        df.to_csv(final_path, index=False)
        print(f"Archivo CSV '{final_path}' creado correctamente.")
    elif filetype == "xlsx":
        df.to_excel(final_path, index=False)
        print(f"Archivo Excel '{final_path}' creado correctamente.")
    else:
        print("Tipo de archivo no soportado.")
        return False
    return True

params_dict = {
    # Usuarios
    "server": "localhost",
    "database": "dbo",
    "username": "root",
    "password": "",
    # Clientes
    "server2": "localhost",
    "database2": "crm",
    "sourceSchema": "dbo",
    "sourceTable": "Usuarios",
    "destSchema": "crm",
    "destTable": "Clientes",
    "src_dest_mappings": {
        "email": "email"
    }
}

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

opcion = input("¿Mostrar resultados como DataFrame? (s/n): ").strip().lower()
mostrar_df = opcion == "s"
df = display_results(resultados, as_dataframe=mostrar_df)

filename = input("Escribe el nombre del archivo para exportar (ejemplo: resultados_fuzzy_match): ").strip()
filetype = input("¿Exportar como CSV o Excel? (csv/xlsx): ").strip().lower()

while True:
    try:
        limit_input = input("¿Cuántas filas quieres exportar? (Deja vacío para todas): ").strip()
        limit = int(limit_input) if limit_input else None
    except ValueError:
        limit = None

    success = export_results(df, filename, filetype=filetype, limit=limit)
    if success or (limit is None):
        break