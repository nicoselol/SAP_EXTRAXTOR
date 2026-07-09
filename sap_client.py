"""
===============================================================================
Archivo: sap_client.py

Descripción:
Este archivo se encarga de establecer la conexión con el sistema SAP mediante
la librería PyRFC. Utiliza los parámetros de configuración almacenados en el
archivo .env y las credenciales proporcionadas por el usuario para crear una
sesión de conexión con el servidor SAP.

Funcionalidades principales:
- Carga las variables de entorno necesarias para la conexión.
- Configura el acceso a las librerías del SAP NetWeaver RFC SDK.
- Crea una conexión con el servidor SAP utilizando PyRFC.
- Recibe el usuario y la contraseña como parámetros para autenticarse.
- Devuelve un objeto de conexión que será utilizado por otros módulos de la
  aplicación para realizar consultas a SAP.

Objetivo:
Centralizar la lógica de conexión a SAP, permitiendo que el resto de la
aplicación reutilice una única función para autenticarse y acceder a la
información del sistema.

===============================================================================
"""
import os
from dotenv import load_dotenv
os.add_dll_directory(r"C:\nwrfcsdk\lib")
from pyrfc import Connection

load_dotenv()

def conectar_sap(usuario, password):

    conn = Connection(

        user=usuario,
        passwd=password,

        ashost=os.getenv("SAP_ASHOST"),
        sysnr=os.getenv("SAP_SYSNR"),
        client=os.getenv("SAP_CLIENT"),
        lang=os.getenv("SAP_LANG")

    )

    return conn
