from fastapi import FastAPI
from sap_reader import leer_tabla_por_anio, leer_tabla_por_mes
from db import guardar_datos
from sap_reader import leer_tabla_por_rango
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
@app.get("/sap/historico")
def historico(tabla: str, anio: int, usuario: str, password: str):

    print(f"Extrayendo datos de SAP -> Tabla: {tabla} Año: {anio}")

    datos = leer_tabla_por_anio(tabla, anio, usuario, password)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio
    }


# NUEVO ENDPOINT
@app.get("/sap/historico-mes")
def historico_mes(tabla: str, anio: int, mes: str, usuario: str, password: str):

    print(f"Extrayendo datos SAP -> Tabla: {tabla} Año: {anio} Mes: {mes}")

    datos = leer_tabla_por_mes(tabla, anio, mes, usuario, password)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio,
        "mes": mes
    }
    
@app.get("/sap/historico-rango")
def historico_rango(tabla: str, anio: int, mes_inicio: str, mes_fin: str, usuario: str, password: str):

    print(f"Extrayendo SAP -> Tabla: {tabla} Año: {anio} Mes inicio: {mes_inicio} Mes fin: {mes_fin}")

    datos = leer_tabla_por_rango(tabla, anio, mes_inicio, mes_fin, usuario, password)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio,
        "mes_inicio": mes_inicio,
        "mes_fin": mes_fin
    }