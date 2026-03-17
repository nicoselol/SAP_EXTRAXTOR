
from fastapi import FastAPI
from sap_reader import leer_tabla_por_anio, leer_tabla_por_mes, leer_tabla_por_rango
from db import guardar_datos, guardar_historial
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def traducir_error_sap(error):

    error_str = str(error)

    if "RFC_LOGON_FAILURE" in error_str:
        return "Usuario o contraseña SAP incorrectos"

    elif "TABLE_NOT_AVAILABLE" in error_str:
        return "La tabla SAP no existe"

    elif "FIELD_NOT_FOUND" in error_str:
        return "Un campo de la tabla SAP no existe"

    elif "RFC_COMMUNICATION_FAILURE" in error_str:
        return "No se pudo conectar con el servidor SAP"

    elif "RFC_ABAP_RUNTIME_FAILURE" in error_str:
        return "Error interno en SAP al ejecutar la consulta"

    elif "TIME_OUT" in error_str:
        return "La consulta a SAP tardó demasiado tiempo"

    else:
        return error_str


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# HISTORICO POR AÑO
@app.get("/sap/historico")
def historico(tabla: str, anio: int, usuario: str, password: str, entorno: str):

    try:

        print(f"Extrayendo datos de SAP -> Tabla: {tabla} Año: {anio}")

        datos = leer_tabla_por_anio(tabla, anio, usuario, password, entorno)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)

        # GUARDAR HISTORIAL
        guardar_historial(
            usuario,
            tabla,
            anio,
            "anio",
            None,
            None,
            len(datos),
            "OK"
        )

        return {
            "success": True,
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio
        }

    except Exception as e:

        mensaje = traducir_error_sap(e)

        guardar_historial(
            usuario,
            tabla,
            anio,
            "anio",
            None,
            None,
            0,
            "ERROR"
        )

        return {
            "success": False,
            "error": mensaje
        }


# HISTORICO POR MES
@app.get("/sap/historico-mes")
def historico_mes(tabla: str, anio: int, mes: str, usuario: str, password: str, entorno: str):

    try:

        print(f"Extrayendo datos SAP -> Tabla: {tabla} Año: {anio} Mes: {mes}")

        datos = leer_tabla_por_mes(tabla, anio, mes, usuario, password, entorno)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)

        # GUARDAR HISTORIAL
        guardar_historial(
            usuario,
            tabla,
            anio,
            "mes",
            mes,
            mes,
            len(datos),
            "OK"
        )

        return {
            "success": True,
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio,
            "mes": mes
        }

    except Exception as e:

        mensaje = traducir_error_sap(e)

        guardar_historial(
            usuario,
            tabla,
            anio,
            "mes",
            mes,
            mes,
            0,
            "ERROR"
        )

        return {
            "success": False,
            "error": mensaje
        }


# HISTORICO POR RANGO
@app.get("/sap/historico-rango")
def historico_rango(tabla: str, anio: int, mes_inicio: str, mes_fin: str, usuario: str, password: str, entorno: str):

    try:

        print(f"Extrayendo SAP -> Tabla: {tabla} Año: {anio} Mes inicio: {mes_inicio} Mes fin: {mes_fin}")

        datos = leer_tabla_por_rango(tabla, anio, mes_inicio, mes_fin, usuario, password, entorno)

        print(f"Filas obtenidas de SAP: {len(datos)}")

        guardar_datos(anio, datos)

        # GUARDAR HISTORIAL
        guardar_historial(
            usuario,
            tabla,
            anio,
            "rango",
            mes_inicio,
            mes_fin,
            len(datos),
            "OK"
        )

        return {
            "success": True,
            "mensaje": "Datos guardados correctamente",
            "registros_insertados": len(datos),
            "anio": anio,
            "mes_inicio": mes_inicio,
            "mes_fin": mes_fin
        }

    except Exception as e:

        mensaje = traducir_error_sap(e)

        guardar_historial(
            usuario,
            tabla,
            anio,
            "rango",
            mes_inicio,
            mes_fin,
            0,
            "ERROR"
        )

        return {
            "success": False,
            "error": mensaje
        }
        
        
@app.get("/historial", response_class=HTMLResponse)
def ver_historial(request: Request):

    import mysql.connector

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="jVilN(lH)KrC7=qR5TXC8F",
        database="aitv_com"  # tu base real
    )

    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT *
        FROM historial_extracciones
        ORDER BY fecha DESC
    """)

    datos = cursor.fetchall()

    conn.close()

    return templates.TemplateResponse(
        "historial.html",
        {"request": request, "datos": datos}
    )