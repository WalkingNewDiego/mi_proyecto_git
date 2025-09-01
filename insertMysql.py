import csv
import mysql.connector


def insert_clientes_from_csv():
    # Configuración de conexión a MySQL
    conn = mysql.connector.connect(
        host="localhost",      # Cambia si tu servidor no es lh
        user="root",           # Usuario de MySQL
        password="",           # Contraseña de MySQL
        database="crm"         # Base de datos
    )
    cursor = conn.cursor()

    # 🔹 Vaciar tabla antes de insertar
    cursor.execute("TRUNCATE TABLE Clientes") #123

    # Ruta de tu archivo CSV
    csv_file = "./UAL-ADAPP/clientes.csv"

    # Abrir y leer el archivo CSV
    with open(csv_file, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            sql = """
            INSERT INTO Clientes (cliente_id, nombre, apellido, email)
            VALUES (%s, %s, %s, %s)
            """
            values = (
                row["cliente_id"],
                row["nombre"],
                row["apellido"],
                row["email"],
            )

            try:
                cursor.execute(sql, values)
            except mysql.connector.Error as err:
                print(f"Error insertando {row['nombre']}: {err}")

    # Confirmar cambios
    conn.commit()
    cursor.close()
    conn.close()
    print("Datos de Clientes insertados correctamente desde CSV.")


def insert_usuarios_from_csv():
    # Configuración de conexión a MySQL
    conn = mysql.connector.connect(
        host="localhost",      
        user="root",           
        password="",           
        database="dbo"         
    )
    cursor = conn.cursor()

    # 🔹 Vaciar tabla antes de insertar
    cursor.execute("TRUNCATE TABLE Usuarios")

    # Ruta de tu archivo CSV
    csv_file = "./UAL-ADAPP/usuarios.csv"

    # Abrir y leer el archivo CSV
    with open(csv_file, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            sql = """
            INSERT INTO Usuarios (username, first_name, last_name, email, password_hash, rol)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (
                row["username"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["password_hash"],
                row.get("rol", "user")
            )

            try:
                cursor.execute(sql, values)
            except mysql.connector.Error as err:
                print(f"Error insertando {row['username']}: {err}")

    # Confirmar cambios
    conn.commit()
    cursor.close()
    conn.close()
    print("Datos de Usuarios insertados correctamente desde CSV.")


# Ejecutar funciones
insert_clientes_from_csv()
insert_usuarios_from_csv()