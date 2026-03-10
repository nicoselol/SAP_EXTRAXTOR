import os
from dotenv import load_dotenv
os.add_dll_directory(r"C:\nwrfcsdk\lib")
from pyrfc import Connection

load_dotenv()

def get_connection():

    conn = Connection(
        user=os.getenv("SAP_USER"),
        passwd=os.getenv("SAP_PASS"),
        ashost=os.getenv("SAP_ASHOST"),
        sysnr=os.getenv("SAP_SYSNR"),
        client=os.getenv("SAP_CLIENT"),
        lang=os.getenv("SAP_LANG"),
    )

    return conn