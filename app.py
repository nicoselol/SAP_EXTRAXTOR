"""
===============================================================================
Archivo: main.py

Descripción:
Este archivo es el punto de entrada de la aplicación desarrollada con FastAPI.
Su función principal es exponer los diferentes endpoints que permiten realizar
extracciones de información desde SAP, almacenar los datos obtenidos en la base
de datos y registrar el historial de cada extracción realizada.

Funcionalidades principales:
- Inicializa la aplicación FastAPI.
- Sirve la interfaz web (index.html) y los archivos estáticos.
- Permite extraer información de SAP por:
    * Año.
    * Mes.
    * Rango de meses.
- Envía el progreso de la extracción en tiempo real mediante
  Server-Sent Events (StreamingResponse).
- Guarda la información extraída en la base de datos.
- Registra en el historial el resultado de cada extracción
  (exitosa o con error).
- Expone un endpoint para consultar las últimas extracciones realizadas.

Flujo general:
1. El usuario realiza una solicitud desde la interfaz web.
2. Se consulta la información en SAP.
3. Los datos obtenidos son procesados y consolidados.
4. La información se almacena en la base de datos.
5. Se registra la operación en el historial.
6. Se devuelve la respuesta al usuario.

===============================================================================
"""

from fastapi import FastAPI
from historial import guardar_historial
from sap_reader import leer_tabla_por_anio, leer_tabla_por_mes
from db import guardar_datos
from sap_reader import leer_tabla_por_rango
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
import json
import time
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()
    
@app.get("/sap/progreso")
def extraer_con_progreso(tabla: str, anio: int, usuario: str, password: str):

    def generar():

        datos_finales = None

        for bloque in leer_tabla_por_anio(tabla, anio, usuario, password):

            yield f"data: {json.dumps({'progreso': bloque['progreso']})}\n\n"

            if bloque["datos"]:
                datos_finales = bloque["datos"]
        registros = 0

        if datos_finales:

            datos = []

            for i in range(len(datos_finales[0])):

                fila = ""

                for grupo in datos_finales:
                    fila += grupo[i] + ";"

                datos.append(fila)

            guardar_datos(anio, datos)
            registros = len(datos)
        guardar_historial(usuario, tabla, anio, "anio", None, None, registros, "OK")
        yield f"data: {json.dumps({'fin': True, 'registros': registros})}\n\n"

    return StreamingResponse(generar(), media_type="text/event-stream")
@app.get("/sap/historico")
def historico(tabla: str, anio: int, usuario: str, password: str):
    try:

        print(f"Extrayendo datos de SAP -> Tabla: {tabla} Año: {anio}")

        bloques = list(leer_tabla_por_anio(tabla, anio, usuario, password))

        datos_grupos = bloques[-1]["datos"]

        datos = []

        for i in range(len(datos_grupos[0])):
            fila = ""
            for grupo in datos_grupos:
                fila += grupo[i] + ";"
            datos.append(fila)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)
        guardar_historial(usuario, tabla, anio, "anio", None, None, len(datos), "OK")

        return {
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio
        }

    except Exception as e:

        guardar_historial(usuario, tabla, anio, "anio", None, None, 0, "ERROR")

        return {
            "error": str(e)
        }


# NUEVO ENDPOINT
@app.get("/sap/historico-mes")
def historico_mes(tabla: str, anio: int, mes: str, usuario: str, password: str):
    try:

        print(f"Extrayendo datos SAP -> Tabla: {tabla} Año: {anio} Mes: {mes}")

        bloques = list(leer_tabla_por_mes(tabla, anio, mes, usuario, password))

        datos_grupos = bloques[-1]["datos"]

        datos = []

        for i in range(len(datos_grupos[0])):
            fila = ""
            for grupo in datos_grupos:
                fila += grupo[i] + ";"
            datos.append(fila)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)
        guardar_historial(usuario, tabla, anio, "mes", mes, mes, len(datos), "OK")

        return {
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio,
            "mes": mes
        }

    except Exception as e:

        guardar_historial(usuario, tabla, anio, "mes", mes, mes, 0, "ERROR")

        return {
            "error": str(e)
        }
@app.get("/sap/historico-rango")
def historico_rango(tabla: str, anio: int, mes_inicio: str, mes_fin: str, usuario: str, password: str):
    try:

        print(f"Extrayendo SAP -> Tabla: {tabla} Año: {anio} Mes inicio: {mes_inicio} Mes fin: {mes_fin}")

        bloques = list(leer_tabla_por_rango(tabla, anio, mes_inicio, mes_fin, usuario, password))

        datos_grupos = bloques[-1]["datos"]

        datos = []

        for i in range(len(datos_grupos[0])):
            fila = ""
            for grupo in datos_grupos:
                fila += grupo[i] + ";"
            datos.append(fila)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)
        guardar_historial(usuario, tabla, anio, "rango", mes_inicio, mes_fin, len(datos), "OK")

        return {
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio,
            "mes_inicio": mes_inicio,
            "mes_fin": mes_fin
        }

    except Exception as e:

        guardar_historial(usuario, tabla, anio, "rango", mes_inicio, mes_fin, 0, "ERROR")

        return {
            "error": str(e)
        }
@app.get("/historial")
def ver_historial():

    import mysql.connector
    
    conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="aitv_com"
    )

    cursor = conexion.cursor(dictionary=True)

    query = """
    SELECT fecha, usuario_sap, tabla, anio, modo, mes_inicio, mes_fin, registros_insertados, estado
    FROM historial_extracciones
    ORDER BY fecha DESC
    LIMIT 20
    """

    cursor.execute(query)

    historial = cursor.fetchall()

    cursor.close()
    conexion.close()

    return historial
