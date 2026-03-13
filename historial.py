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