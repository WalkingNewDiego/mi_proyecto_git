# archivo: probar_rapidfuzz.py

from rapidfuzz import fuzz, process

# Lista de registros de ejemplo
registros = [
    {"Nombre": "Juan", "Apellido": "Pérez", "Email": "juan.perez@gmail.com"},
    {"Nombre": "Maria", "Apellido": "Gonzalez", "Email": "maria.gonzalez@yahoo.com"},
    {"Nombre": "Carlos", "Apellido": "Lopez", "Email": "carlos.lopez@hotmail.com"},
    {"Nombre": "Ana", "Apellido": "Martínez", "Email": "ana.martinez@gmail.com"},
]

# Nuevos registros para probar
nuevos_registros = [
    {"Nombre": "Juan", "Apellido": "Perezz", "Email": "juan.perez@gmail.com"},
    {"Nombre": "Juan", "Apellido": "Perez", "Email": "juan.perez@gmail.com"},  # typo en Apellido
    {"Nombre": "Maria", "Apellido": "Gonzales", "Email": "maria.gonzalez@yahoo.com"},
    {"Nombre": "María", "Apellido": "Gonzalez", "Email": "maria.gonzalez@yahoo.com"},  # typo en Apellido
    {"Nombre": "Carlos", "Apellido": "Lopez", "Email": "carlos.lopez123@hotmail.com"},
    {"Nombre": "Carlos", "Apellido": "Lopezz", "Email": "carlos.lopez@hotmail.com"},  # email diferente
    {"Nombre": "Ana", "Apellido": "Martinez", "Email": "ana.martinez@gmail.com"},  # sin acento
    {"Nombre": "Ana", "Apellido": "Martínez", "Email": "ana.martinezz@gmail.com"}
]

# Función para normalizar strings: eliminar acentos y pasar a minúsculas
import unicodedata

def normalizar(texto):
    texto = unicodedata.normalize('NFD', texto)
    texto = texto.encode('ascii', 'ignore').decode('utf-8')
    return texto.lower()

# Comparar registros usando RapidFuzz
for nuevo in nuevos_registros:
    print(f"\nAnalizando nuevo registro: {nuevo}")
    mejores_coincidencias = []

    for reg in registros:
        # Calcular similitud por campo
        nombre_score = fuzz.ratio(normalizar(nuevo['Nombre']), normalizar(reg['Nombre']))
        apellido_score = fuzz.ratio(normalizar(nuevo['Apellido']), normalizar(reg['Apellido']))
        email_score = fuzz.ratio(normalizar(nuevo['Email']), normalizar(reg['Email']))

        # Puntaje ponderado: Email 50%, Apellido 30%, Nombre 20%
        puntaje_total = 0.2*nombre_score + 0.3*apellido_score + 0.5*email_score
        mejores_coincidencias.append((reg, puntaje_total))

    # Ordenar por puntaje descendente
    mejores_coincidencias.sort(key=lambda x: x[1], reverse=True)

    # Mostrar la mejor coincidencia
    mejor_registro, mejor_puntaje = mejores_coincidencias[0]
    print(f"Mejor coincidencia: {mejor_registro} con puntaje {mejor_puntaje:.2f}")