import mysql.connector
import csv
from datetime import datetime
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

config_clientes = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'port': 3306,
    'database': 'crm'
}

config_usuarios = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'port': 3306,
    'database': 'dbo'
}

def insertar_clientes(cursor, clientes_csv):
    with open(clientes_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  
        for row in reader:
            cliente_id, nombre, apellido, email, fecha_str = row
            fecha_mysql = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
            cursor.callproc("sp_insert_cliente", (cliente_id, nombre, apellido, email, fecha_mysql))

# def insertar_usuarios(cursor, archivo_csv):
#     with open(archivo_csv, newline='', encoding='utf-8') as csvfile:
#         reader = csv.reader(csvfile)
#         next(reader)
#         for row in reader:
#             userId, username, first_name, last_name, email, password_hash, rol, fecha_str = row
#             fecha_mysql = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
#             cursor.callproc("sp_insert_usuario", (userId, username, first_name, last_name, email, password_hash, rol, fecha_mysql))

def main():
    conn_c = cursor_c = None
    try:
        conn_c = mysql.connector.connect(**config_clientes)
        cursor_c = conn_c.cursor()
        insertar_clientes(cursor_c, "clientes.csv")
        conn_c.commit()
        print("Clientes insertados correctamente.")
    except mysql.connector.Error as err:
        print(f"Error clientes: {err}")
        if conn_c:
            conn_c.rollback()
    finally:
        if cursor_c:
            cursor_c.close()
        if conn_c:
            conn_c.close()

    #conn_u = cursor_u = None
    # try:
    #     conn_u = mysql.connector.connect(**config_usuarios)
    #     cursor_u = conn_u.cursor()
    #     insertar_usuarios(cursor_u, "usuarios.csv")
    #     conn_u.commit()
    #     print("Usuarios insertados correctamente.")
    # except mysql.connector.Error as err:
    #     print(f"Error usuarios: {err}")
    #     if conn_u:
    #         conn_u.rollback()
    # finally:
    #     if cursor_u:
    #         cursor_u.close()
    #     if conn_u:
    #         conn_u.close()

if __name__ == "__main__":
    main()