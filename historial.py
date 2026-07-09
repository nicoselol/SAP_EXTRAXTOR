
"""
===============================================================================
Archivo: historial.py

Descripción:
Este archivo se encarga de registrar en la base de datos el historial de cada
extracción realizada desde SAP. Su objetivo es mantener un registro de las
operaciones ejecutadas, indicando la información utilizada y el resultado de
cada proceso.

Funcionalidades principales:
- Establece la conexión con la base de datos MySQL.
- Registra el usuario de SAP que realizó la extracción.
- Almacena la tabla consultada y el año de la extracción.
- Guarda el modo de extracción (año, mes o rango de meses).
- Registra el período consultado cuando aplica.
- Almacena la cantidad de registros insertados.
- Registra el estado final de la extracción (OK o ERROR).
- Confirma la inserción y cierra la conexión con la base de datos.

Flujo general:
1. Se establece la conexión con MySQL.
2. Se preparan los datos de la extracción.
3. Se inserta un nuevo registro en la tabla
   'historial_extracciones'.
4. Se confirma la transacción.
5. Se cierran el cursor y la conexión.

===============================================================================
"""
import mysql.connector

def guardar_historial(usuario, tabla, anio, modo, mes_inicio, mes_fin, registros, estado):
    print("Guardando historial...")

    conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="aitv_com"
    )

    cursor = conexion.cursor()

    query = """
    INSERT INTO historial_extracciones
    (usuario_sap, tabla, anio, modo, mes_inicio, mes_fin, registros_insertados, estado)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    valores = (
        usuario,
        tabla,
        anio,
        modo,
        mes_inicio,
        mes_fin,
        registros,
        estado
    )

    cursor.execute(query, valores)

    conexion.commit()

    cursor.close()
    conexion.close()
