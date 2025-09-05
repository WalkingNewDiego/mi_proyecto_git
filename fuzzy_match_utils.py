from rapidfuzz import process, fuzz
import mysql.connector
import pandas as pd
import os 

def connect_to_mysql(host, database, username, password, port=3306):
    connection = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        database=database,
        port=port
    )
    return connection

def fuzzy_match(queryRecord, choices, score_cutoff=0):
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    best_match = None
    best_score = score_cutoff

    for scorer in scorers:
        result = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if result:
            match_value, score, index = result
            if score >= best_score:
                matched_item = choices_data[index]
                best_match = {
                    'match_query': queryRecord,
                    'match_result': match_value,
                    'score': score,
                    'match_result_values': matched_item['match_record_values']
                }
        else:
            best_match = {
                'match_query': queryRecord,
                'match_result': None,
                'score': 0,
                'match_result_values': {}
            }
    return best_match

def execute_dynamic_matching(params_dict, score_cutoff=0):
    conn = connect_to_mysql(
        host=params_dict.get("server", "localhost"),
        database=params_dict.get("database", ""),
        username=params_dict.get("username", "root"),
        password=params_dict.get("password", ""),
        port=params_dict.get("port", 3306)
    )
    cursor = conn.cursor(dictionary=True)

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    db = params_dict.get("database", "")
    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceDatabase']}.{params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destDatabase']}.{params_dict['destTable']}"

    cursor.execute(sql_source)
    source_data = cursor.fetchall()

    cursor.execute(sql_dest)
    dest_data = cursor.fetchall()

    conn.close()

    matching_records = []

    for record in source_data:
        dict_query_records = {}
        query = ""

        for src_col in params_dict['src_dest_mappings'].keys():
            val = record.get(src_col)
            query += str(val) if val is not None else ""
            dict_query_records[src_col] = val

        fm = fuzzy_match(query, dest_data, score_cutoff)
        dict_query_records.update(fm)
        dict_query_records.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        matching_records.append(dict_query_records)

    return matching_records

def display_results(resultados, as_dataframe=True, selected_columns=None, num_rows=None):
    df = pd.DataFrame(resultados)

    # Filtrar por columnas elegidas
    if selected_columns:
        df = df[selected_columns]

    if num_rows is not None and num_rows > 0:
        df = df.head(num_rows)

    if as_dataframe:
        print(df)
    else:
        print(df.to_dict(orient="records"))


def display_results(resultados, as_dataframe=True, num_columns=None, num_rows=None):
    """
    Display resultados as a DataFrame or dictionary.
    :param resultados: The result data (list of dicts or dict)
    :param as_dataframe: If True, display as DataFrame; else as dictionary
    :param num_columns: Number of columns to display (int or None for all)
    :param num_rows: Number of rows to display (int or None for all)
    """
    df = pd.DataFrame(resultados)

    if num_columns is not None and num_columns > 0:
        df = df.iloc[:, :num_columns]

    if num_rows is not None and num_rows > 0:
        df = df.head(num_rows)

    if as_dataframe:
        print(df)
    else:
        print(df.to_dict(orient="records"))

def _prepare_dataframe(resultados, selected_columns=None, rename_map=None, force_all_rows=False):
    """Helper to prepare DataFrame with score %, full_name and selected/renamed columns"""
    if not resultados:
        return pd.DataFrame()

    df = pd.DataFrame(resultados)

    # Score como porcentaje si existe
    if "score" in df.columns:
        df["score"] = df["score"].astype(float) * 100

    # Crear columna full_name si existen nombre y apellido
    if "nombre" in df.columns and "apellido" in df.columns:
        df["full_name"] = df["nombre"].astype(str) + " " + df["apellido"].astype(str)

    # Filtrar columnas seleccionadas, más score y full_name
    if selected_columns:
        keep = []
        for col in selected_columns:
            if col in df.columns:
                keep.append(col)
        # Score y full_name son siempre incluidos
        if "score" in df.columns:
            keep.append("score")
        if "full_name" in df.columns:
            keep.append("full_name")
        df = df[keep]

    # Renombrar columnas
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Limitar filas solo si no es export matched/unmatched
    if not force_all_rows and "num_rows" in locals():
        pass  # se maneja desde caller

    return df


def export_results_to_csv(resultados, filename, selected_columns=None, rename_map=None, num_rows=None, force_all_rows=False):
    try:
        df = _prepare_dataframe(resultados, selected_columns, rename_map, force_all_rows)

        if df.empty or df.shape[1] == 0:
            print("No valid data to export.")
            return

        if not filename.endswith(".csv"):
            filename += ".csv"

        output_dir = "archivos csv"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        # Limitar filas solo si no es export matched/unmatched
        if not force_all_rows and num_rows and num_rows > 0:
            df = df.head(num_rows)

        df.to_csv(filepath, index=False)
        print(f"CSV exported successfully: {filepath}")

    except Exception as e:
        print(f"Error exporting CSV: {e}")


def export_results_to_xlsx(resultados, filename, selected_columns=None, rename_map=None, num_rows=None, force_all_rows=False):
    try:
        df = _prepare_dataframe(resultados, selected_columns, rename_map, force_all_rows)

        if df.empty or df.shape[1] == 0:
            print("No valid data to export.")
            return

        if not filename.endswith(".xlsx"):
            filename += ".xlsx"

        output_dir = "archivos xlsx"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        # Limitar filas solo si no es export matched/unmatched
        if not force_all_rows and num_rows and num_rows > 0:
            df = df.head(num_rows)

        df.to_excel(filepath, index=False, engine="openpyxl")
        print(f"Excel exported successfully: {filepath}")

    except ImportError:
        print("Missing dependency: install openpyxl (pip install openpyxl).")
    except Exception as e:
        print(f"Error exporting Excel: {e}")



# --------------------
# IMPORT FUNCTION
# --------------------

def separate_matched_records(resultados, threshold=97.0):
    df = pd.DataFrame(resultados)
    if "score" not in df.columns:
        print("No 'score' column found.")
        return pd.DataFrame(), pd.DataFrame()

    matched = df[df["score"] * 100 >= threshold]
    unmatched = df[df["score"] * 100 < threshold]

    return matched, unmatched


def export_matched_or_unmatched(resultados, selected_columns=None, rename_map=None, threshold=97.0):
    matched_df, unmatched_df = separate_matched_records(resultados, threshold)

    choice_group = input("Do you want to export 'matched' or 'unmatched' results? ").strip().lower()
    if choice_group == "matched":
        df_to_export = matched_df
        default_name_csv = "matched_results.csv"
        default_name_xlsx = "matched_results.xlsx"
    elif choice_group == "unmatched":
        df_to_export = unmatched_df
        default_name_csv = "unmatched_results.csv"
        default_name_xlsx = "unmatched_results.xlsx"
    else:
        print("Invalid choice. No file exported.")
        return

    if df_to_export.empty:
        print(f"No {choice_group} records found to export.")
        return

    export_choice = input("Do you want to export as 'csv' or 'xlsx'? ").strip().lower()

    if export_choice == "csv":
        filename = input(f"Enter the filename for CSV (default: {default_name_csv}): ").strip()
        if not filename:
            filename = default_name_csv
        export_results_to_csv(df_to_export.to_dict(orient="records"), filename,
                              selected_columns=selected_columns, rename_map=rename_map,
                              num_rows=None, force_all_rows=True)

    elif export_choice == "xlsx":
        filename = input(f"Enter the filename for Excel (default: {default_name_xlsx}): ").strip()
        if not filename:
            filename = default_name_xlsx
        export_results_to_xlsx(df_to_export.to_dict(orient="records"), filename,
                               selected_columns=selected_columns, rename_map=rename_map,
                               num_rows=None, force_all_rows=True)

    else:
        print("Invalid format. No file exported.")

def import_file_and_insert_to_db(file_path, db_config):
    table_name = "archivo_exportado"  # Tabla fija

    try:
        # Check file existence
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        # Read file
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            print("Unsupported file type. Only CSV and XLSX are allowed.")
            return

        if df.empty:
            print("The file is empty, no data imported.")
            return

        print(f"File imported successfully with {len(df)} rows and {len(df.columns)} columns.")

        # Connect to DB
        try:
            conn = mysql.connector.connect(
                host=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["database"]
            )
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            return

        cursor = conn.cursor()

        # Build CREATE TABLE dynamically
        create_cols = [f"`{col}` TEXT" for col in df.columns]
        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(create_cols)})"

        try:
            cursor.execute(create_stmt)
        except mysql.connector.Error as err:
            print(f"Error creating table `{table_name}`: {err}")
            conn.close()
            return

        # Truncate table before inserting new data
        try:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        except mysql.connector.Error as err:
            print(f"Error truncating table `{table_name}`: {err}")
            conn.close()
            return

        # Insert data row by row
        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_stmt = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

        try:
            for _, row in df.iterrows():
                cursor.execute(insert_stmt, tuple(row.fillna("").values))  # Replace NaN with ""
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            conn.rollback()
            cursor.close()
            conn.close()
            return

        cursor.close()
        conn.close()
        print(f"Data inserted into table `{table_name}` successfully!")

    except Exception as e:
        print(f"Unexpected error during import: {e}")