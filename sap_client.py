import os
os.add_dll_directory(r"C:\nwrfcsdk\lib")
from dotenv import load_dotenv

from pyrfc import Connection

load_dotenv()


def conectar_sap(usuario, password, entorno):

    if entorno == "prod":
        host = os.getenv("SAP_PRD_HOST")

    elif entorno == "qa":
        host = os.getenv("SAP_QAS_HOST")

    else:
        raise ValueError("Entorno SAP no válido")

    conn = Connection(

        user=usuario,
        passwd=password,

        ashost=host,
        sysnr=os.getenv("SAP_SYSNR"),
        client=os.getenv("SAP_CLIENT"),
        lang=os.getenv("SAP_LANG")

    )

    return conn