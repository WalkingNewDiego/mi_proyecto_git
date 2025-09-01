from fuzzy_match_utils import execute_dynamic_matching
import pandas as pd

def display_results(resultados, as_dataframe=True):
    if as_dataframe:
        df = pd.DataFrame(resultados)
        print(df)
        return df
    else:
        print(resultados)
        return resultados

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

# Preguntar al usuario
opcion = input("¿Mostrar resultados como DataFrame? (s/n): ").strip().lower()
mostrar_df = opcion == "s"

display_results(resultados, as_dataframe=mostrar_df)