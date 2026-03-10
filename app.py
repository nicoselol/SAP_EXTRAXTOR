from fastapi import FastAPI
from sap_reader import leer_tabla_por_anio, leer_tabla_por_mes
from db import guardar_datos
from sap_reader import leer_tabla_por_rango
app = FastAPI()


@app.get("/sap/historico")
def historico(tabla: str, anio: int):

    print(f"Extrayendo datos de SAP -> Tabla: {tabla} Año: {anio}")

    datos = leer_tabla_por_anio(tabla, anio)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio
    }


# NUEVO ENDPOINT
@app.get("/sap/historico-mes")
def historico_mes(tabla: str, anio: int, mes: str):

    print(f"Extrayendo datos SAP -> Tabla: {tabla} Año: {anio} Mes: {mes}")

    datos = leer_tabla_por_mes(tabla, anio, mes)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio,
        "mes": mes
    }
    
@app.get("/sap/historico-rango")
def historico_rango(tabla: str, anio: int, mes_inicio: str, mes_fin: str):

    print(f"Extrayendo SAP -> Tabla: {tabla} Año: {anio} Mes inicio: {mes_inicio} Mes fin: {mes_fin}")

    datos = leer_tabla_por_rango(tabla, anio, mes_inicio, mes_fin)

    print(f"Filas obtenidas de SAP: {len(datos)}")

    guardar_datos(anio, datos)

    return {
        "mensaje": "Datos guardados correctamente",
        "registros_insertados": len(datos),
        "anio": anio,
        "mes_inicio": mes_inicio,
        "mes_fin": mes_fin
    }