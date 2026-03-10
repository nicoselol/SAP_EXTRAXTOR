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