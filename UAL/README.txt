Dentro del archivo python, este archivo contine una libreria (rapidfuzz) que sirve para 
hacer comparaciones rapidas entre cadenas de texto, una vez dicho esto, el archivo permite encontrar y 
separar los datos de manera eficiente.

Compara registros de dos tablas SQL usando coincidencia difusa con rapidfuzz.

Uso

Instala las dependencias:

pip install rapidfuzz pyodbc

Configura tus datos y columnas en `params_dict`.

Ejecuta el script:
python
resultados = execute_dynamic_matching(params_dict, score_cutoff=80)
print(resultados)

¿Qué hace?

Se conecta a SQL Server, compara columnas que definas entre dos tablas, busca coincidencias aproximadas.

server: Nombre o dirección del servidor SQL Server.

database: Nombre de la base de datos.

username: Usuario para conectarse al servidor.

password: Contraseña del usuario.

sourceSchema: Esquema de la tabla origen (por ejemplo, "dbo").

sourceTable: Nombre de la tabla origen.

destSchema: Esquema de la tabla destino.

destTable: Nombre de la tabla destino.

src_dest_mappings: Diccionario que indica qué columna de la tabla origen se compara con cuál de la tabla destino. Ejemplo: { "nombre": "first_name", "Ciudad": "City" }

score_cutoff: Valor mínimo de similitud (de 0 a 100) para considerar una coincidencia.




documentacion rapidfuzz
se elimino rebundancia de variable en params dict("username2": "root",
    "password2": "",) para solo utilizar username y password

se elimino la siguiente linea sin utilizar 
    return mysql.connector.connect(connection_string)

se agrego alias a import mysql.connector as mc
y se modifico respectivamente las lineas de acuerdo al nuevo nombre