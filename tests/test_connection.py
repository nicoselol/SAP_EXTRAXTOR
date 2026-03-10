import os

# ⚡ SIEMPRE PRIMERO
os.add_dll_directory(r"C:\nwrfcsdk\lib")

from dotenv import load_dotenv
load_dotenv()

# ⚡ IMPORT DESPUÉS
from pyrfc import Connection


conn = Connection(
    user=os.getenv("SAP_USER"),
    passwd=os.getenv("SAP_PASS"),
    ashost=os.getenv("SAP_ASHOST"),
    sysnr=os.getenv("SAP_SYSNR"),
    client=os.getenv("SAP_CLIENT"),
    lang=os.getenv("SAP_LANG"),
)

print("🔥 CONECTADO A SAP 🔥")

result = conn.call(
    "STFC_CONNECTION",
    REQUTEXT="Hola desde Python"
)

print(result)