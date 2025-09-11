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


viernes 5 de septiembre del 2025 - victor jireh
se creo store_procedure con el script sql a ejecutar en mysqlworkbench
se actualizo el insert para ser compatible y eficientes con el store procedure
tambien se actualizo un poco la funcion execute_dynamic_matching

Jueves 10 de eseptiembre Entregable #14

-¿Cómo funciona el Fuzzy Match y qué prefiere al comparar?-

Cuando trabajamos con bases de datos reales, es común encontrar 
**variaciones en nombres, apellidos o correos electrónicos** 
debido a errores de captura, diferencias de acentuación o duplicación de caracteres.  
En estos casos, una comparación exacta (`==`) no es suficiente.  
Para eso usamos **fuzzy matching**, que mide **similitud aproximada** en lugar de igualdad estricta.

---
 
-Principios del fuzzy matching-

El algoritmo se basa en la **distancia de edición** (ej. *Levenshtein*), 
que mide cuántas operaciones se necesitan para transformar una cadena en otra:
- **Sustitución**: cambiar una letra → `Perez` → `Pérez`  
- **Inserción**: agregar una letra → `Lopez` → `Lopezz`  
- **Eliminación**: quitar una letra → `Gonzales` → `Gonzalez`  

Cuantas **menos operaciones**, mayor es el **puntaje de similitud** (cercano a 100).

---

-Factores que favorecen una coincidencia alta-

1. **Pequeñas diferencias ortográficas**  
   - El algoritmo tolera duplicaciones o letras cambiadas.  
   - Ejemplo:  
     | Cadena A   | Cadena B  | Puntaje |
     |------------|-----------|---------|
     | Perezz     | Pérez     | 97.27   |
     | Lopezz     | Lopez     | 97.27   |

---

2. **Normalización de acentos y mayúsculas**  
   - “é” y “e” se consideran equivalentes.  
   - Minúsculas y mayúsculas no afectan el resultado.  
   - Ejemplo:  
     | Cadena A | Cadena B | Puntaje |
     |----------|----------|---------|
     | Perez    | Pérez    | 100.00  |
     | Maria    | María    | 100.00  |

---

3. **Longitud similar de las cadenas**  
   - Dos cadenas con longitudes cercanas reciben mejor puntaje.  
   - Ejemplo:  
     | Cadena A   | Cadena B     | Puntaje |
     |------------|--------------|---------|
     | Maria      | Mariaa       | 96.00+  |
     | Maria      | Mariangelica | 82.00+  |

---

4. **Tokens compartidos aunque cambien de orden**  
   - Dependiendo de la métrica usada (`token_sort_ratio`, `WRatio`, etc.), el orden puede no importar.  
   - Ejemplo:  
     | Cadena A   | Cadena B   | Puntaje |
     |------------|------------|---------|
     | Juan Pérez | Pérez Juan | 100.00  |

---

5. **Campos exactos pesan más (ej. correo electrónico)**  
   - Si un campo clave como **Email** coincide al 100%, el puntaje global será alto,
    aunque el nombre/apellido tenga variaciones.  
   - Ejemplo:  
     | Nombre   | Email                           | Mejor Coincidencia                      | Puntaje |
     |----------|---------------------------------|-----------------------------------------|---------|
     | Juan Perezz | juan.perez@gmail.com         | Juan Pérez, juan.perez@gmail.com        | 97.27   |
     | Juan Perez  | juan.perez@gmail.com         | Juan Pérez, juan.perez@gmail.com        | 100.00  |

---

-Interpretación de puntajes-

- **100** → Coincidencia exacta o diferencias insignificantes (acentos, mayúsculas).  
- **90 - 99** → Probable coincidencia con errores menores (letra extra, acento faltante).  
- **80 - 89** → Coincidencia posible, pero revisar manualmente (nombre muy parecido).  
- **< 80** → Probablemente registros distintos.  

---

-Ejemplo de análisis real-

Analizando nuevo registro: {'Nombre': 'Maria', 'Apellido': 'Gonzales', 'Email': 'maria.gonzalez@yahoo.com'}
Mejor coincidencia: {'Nombre': 'Maria', 'Apellido': 'Gonzalez', 'Email': 'maria.gonzalez@yahoo.com'} con puntaje 96.25

Analizando nuevo registro: {'Nombre': 'María', 'Apellido': 'Gonzalez', 'Email': 'maria.gonzalez@yahoo.com'}
Mejor coincidencia: {'Nombre': 'Maria', 'Apellido': 'Gonzalez', 'Email': 'maria.gonzalez@yahoo.com'} con puntaje 100.00





