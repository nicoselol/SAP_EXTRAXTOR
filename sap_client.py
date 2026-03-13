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