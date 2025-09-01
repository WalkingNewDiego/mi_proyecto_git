from fuzzy_match_utils import execute_dynamic_matching
import pandas as pd

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

# Convertir resultados a DataFrame
df = pd.DataFrame(resultados)
print(df)