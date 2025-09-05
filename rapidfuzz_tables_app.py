import fuzzy_match_utils
import pandas as pd

# -------------------------
# Configuración y ejecución
# -------------------------
params_dict = {
    "server": "localhost",
    "port": 3306,
    "username": "root",
    "password": "",
    "sourceDatabase": "crm",
    "sourceTable": "Clientes",
    "destDatabase": "dbo",
    "destTable": "Usuarios",
    "src_dest_mappings": {
        "nombre": "first_name",
        "apellido": "last_name",
        "email": "email"
    }
}
resultados = fuzzy_match_utils.execute_dynamic_matching(params_dict, score_cutoff=80)

user_choice = input("¿Cómo quieres mostrar los resultados? Escribe 'df' para DataFrame o 'dict' para diccionario: ").strip().lower()
as_dataframe = True if user_choice == 'df' else False

try:
    num_rows = int(input("¿Cuántas filas quieres mostrar/exportar? Ingresa un número: ").strip())
except ValueError:
    num_rows = None

if num_rows == 0:
    print("El archivo está vacío. No se mostrarán ni exportarán resultados.")
else:
    fuzzy_match_utils.display_results(resultados, as_dataframe=as_dataframe, num_rows=num_rows)

    # Mostrar columnas disponibles y pedir selección

    columnas_disponibles = list(pd.DataFrame(resultados).columns)
    print("\nColumnas disponibles para exportar:")
    print(", ".join(columnas_disponibles))

    columnas_input = input("¿Qué columnas quieres exportar? Usa 'columna:nombreNuevo' o solo 'columna'. Ejemplo: nombre:Nombre,email:Correo\n").strip()

    rename_map = {}
    if columnas_input:
        selected_columns = []
        for col in columnas_input.split(","):
            parts = col.strip().split(":")
            original = parts[0].strip()
            if original in columnas_disponibles:
                selected_columns.append(original)
                if len(parts) == 2:
                    rename_map[original] = parts[1].strip()
    else:
        selected_columns = None
        rename_map = None

    export_choice = input("¿Quieres exportar los resultados? Escribe 'csv' para CSV, 'xlsx' para Excel, o 'n' para no exportar: ").strip().lower()
    if export_choice == 'csv':
        filename = input("Escribe el nombre del archivo CSV (por defecto: resultados.csv): ").strip()
        if not filename:
            filename = "resultados.csv"
        fuzzy_match_utils.export_results_to_csv(resultados, filename, selected_columns=selected_columns, rename_map=rename_map, num_rows=num_rows)
    elif export_choice == 'xlsx':
        filename = input("Escribe el nombre del archivo Excel (por defecto: resultados.xlsx): ").strip()
        if not filename:
            filename = "resultados.xlsx"
        fuzzy_match_utils.export_results_to_xlsx(
            resultados,
            filename,
            selected_columns=selected_columns,
            rename_map=rename_map,
            num_rows=num_rows
        )

matched, unmatched = fuzzy_match_utils.separate_matched_records(resultados, threshold=97.0)

print("\nRegistros coincidentes (>=97%):")
print(matched)

print("\nRegistros no coincidentes (<97%):")
print(unmatched)

fuzzy_match_utils.export_matched_or_unmatched(
    resultados,
    selected_columns=selected_columns,
    rename_map=rename_map,
)

action = input("¿Quieres 'exportar' resultados o 'importar' un archivo a la base de datos? Escribe 'exportar' o 'importar': ").strip().lower()

if action == "exportar":
    fuzzy_match_utils.export_matched_or_unmatched(
        resultados,
        selected_columns=selected_columns,
        rename_map=rename_map
    )

elif action == "importar":
    file_path = input("Escribe la ruta del archivo a importar (CSV/XLSX): ").strip()

    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "crm"   # <-- cámbialo si usas otra base
    }

    fuzzy_match_utils.import_file_and_insert_to_db(file_path, db_config)

else:
    print("Opción inválida. Saliendo.")