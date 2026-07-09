"""
===============================================================================
Archivo: prueba_conexion.py

Descripción:
Este archivo se utiliza para verificar la conexión entre la aplicación y el
sistema SAP. Además de validar la autenticación, realiza una consulta de prueba
mediante la función RFC_READ_TABLE para comprobar que es posible acceder a la
información de una tabla específica.

Funcionalidades principales:
- Carga las credenciales de conexión desde el archivo .env.
- Establece una conexión con el servidor SAP utilizando la librería PyRFC.
- Ejecuta una consulta de prueba sobre una tabla de SAP.
- Aplica un filtro para consultar únicamente los registros del año indicado.
- Limita la cantidad de registros devueltos para validar la conexión.
- Muestra en consola los datos obtenidos.

Objetivo:
Comprobar que la configuración, las credenciales y la comunicación con SAP
funcionan correctamente antes de ejecutar el proceso completo de extracción.

===============================================================================
"""
import os
os.add_dll_directory(r"C:\nwrfcsdk\lib")

from dotenv import load_dotenv
from pyrfc import Connection

load_dotenv()

conn = Connection(
    user=os.getenv("SAP_USER"),
    passwd=os.getenv("SAP_PASS"),
    ashost=os.getenv("SAP_ASHOST"),
    sysnr=os.getenv("SAP_SYSNR"),
    client=os.getenv("SAP_CLIENT"),
    lang=os.getenv("SAP_LANG"),
)

print("🔥 Leyendo SAP...")

result = conn.call(
    "RFC_READ_TABLE",
    QUERY_TABLE="ZSDVTACOM1",
    DELIMITER=";",
    
    OPTIONS=[
        {"TEXT": "GJAHR = '2017'"}
    ],

    ROWCOUNT=5
)

for row in result["DATA"]:
    print(row["WA"])
