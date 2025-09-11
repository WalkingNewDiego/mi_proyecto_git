import os
import pandas as pd
from rapidfuzz_table_app_2 import execute_dynamic_matching
import mysql.connector

CARPETA_DESTINO = "C:\\Users\\df2la\\Desktop\\mi_proyecto_git\\Resultados"  
os.makedirs(CARPETA_DESTINO, exist_ok=True)

def insert_from_csv(connection, table, columns, csv_file):
    cursor = connection.cursor()
    try:
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            col_names = ",".join(columns)
            values = []
            for col in columns:
                val = row[col]
                if pd.isna(val):
                    values.append("NULL")
                else:
                    values.append("'" + str(val).replace("'", "''") + "'")
            values_str = ",".join(values)

            cursor.callproc("sp_insert_csv_table", (table, col_names, values_str))
        connection.commit()
        print(f" Se insertaron {len(df)} filas en la tabla '{table}' usando SP.")
    except Exception as error:
        print(f" Error al insertar datos: {error}")
    finally:
        cursor.close()

def crear_tabla_desde_csv(connection, table_name, csv_file):
    cursor = connection.cursor()
    try:
        df = pd.read_csv(csv_file)
        columnas = df.columns.tolist()

        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        columnas_sql = ", ".join([f"`{col}` VARCHAR(255)" for col in columnas])
        cursor.execute(f"CREATE TABLE `{table_name}` ({columnas_sql});")
        connection.commit()

        print(f" Tabla '{table_name}' recreada correctamente.")
        return columnas
    except Exception as e:
        print(f" Error al crear la tabla: {e}")
        return []
    finally:
        cursor.close()

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

resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
matches_filtrados = [r for r in resultados if r.get('score', 0)]

try:
    conexion = mysql.connector.connect(
        host=params_dict["server"],
        user=params_dict["username"],
        password=params_dict["password"],
        database=params_dict["sourceDatabase"],
        port=params_dict["port"]
    )

    tabla_destino = "MatchedRecords"
    ruta_csv = "matched_temp.csv"

    pd.DataFrame(matches_filtrados).to_csv(ruta_csv, index=False)

    columnas_tabla = crear_tabla_desde_csv(conexion, tabla_destino, ruta_csv)

    if columnas_tabla:
        insert_from_csv(conexion, tabla_destino, columnas_tabla, ruta_csv)

    cursor = conexion.cursor()
    cursor.execute(f"SELECT * FROM {tabla_destino};")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_mostrado = pd.DataFrame(rows, columns=columns)
    print("\nRegistros coincidentes insertados en BD:")
    print(df_mostrado)

    exportar = input("\n¿Deseas exportar los resultados? (Si/No): ").strip().lower()
    if exportar == "si":
        formato = input("¿A qué formato deseas exportar? (CSV/XLSX): ").strip().lower()

        columnas_disponibles = df_mostrado.columns.tolist()
        print(f"\nColumnas disponibles: {', '.join(columnas_disponibles)}")
        columnas_elegidas = input("Escribe las columnas que quieras (separadas por coma) o presiona Enter para todas: ").strip()

        if columnas_elegidas:
            columnas_elegidas = [col.strip() for col in columnas_elegidas.split(",") if col.strip() in columnas_disponibles]
            df_exportar = df_mostrado[columnas_elegidas]
        else:
            df_exportar = df_mostrado

        nombre_archivo = input("Nombre del archivo (sin extensión): ").strip()
        if not nombre_archivo:
            nombre_archivo = "resultados"

        if formato == "csv":
            ruta = os.path.join(CARPETA_DESTINO, nombre_archivo + ".csv")
            df_exportar.to_csv(ruta, index=False)
            print(f" Resultados exportados a {ruta}")

        elif formato == "xlsx":
            ruta = os.path.join(CARPETA_DESTINO, nombre_archivo + ".xlsx")
            df_exportar.to_excel(ruta, index=False)
            print(f" Resultados exportados a {ruta}")

        else:
            print(" Formato no válido. Usa CSV o XLSX.")

    conexion.close()
except Exception as e:
    print(f" Error al conectar o insertar en MySQL: {e}")