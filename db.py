import mysql.connector
from datetime import datetime

def get_mysql_connection():

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="aitv_com",
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci"
    )

    return conn


def guardar_datos(anio, datos):

    conn = get_mysql_connection()
    cursor = conn.cursor()

    print(f"Borrando datos existentes del año {anio}")

    cursor.execute(
        "DELETE FROM zsdvtacom1 WHERE GJAHR = %s",
        (anio,)
    )

    columnas_mysql = """
    MANDT,ID,GJAHR,MES,STATUS,ORIGEN,FKDAT,BUDAT,VBELN,POSNR,VBTYP,VKBUR,
    DOVTA,KUNAG,NAME1,AUBEL,AUPOS,AUART,SPART,AUFNR,WERKS,NETWR,KWAKRB,KWBRUM,
    WAVWR,VVDDP,PORCENT_COMISION,WAERK,KURRF,PERNR_E,VKBUR_E,PERNR_I,VKBUR_I,CREAL,PORENT,MATNR,ARKTX,
    MVGR1,BEZEI,NTGEW,GEWEI,PRCTR,LTEXT,KDGRP,KVGR1,KVGR2,PS_PSP_PNR,LIQUIDACION,RACCT,
    DEVOL,EMNAM,KZWI1,KZWI2,KZWI3,KZWI4,KZWI6,RFWRT,CRTOTAL,VBEL2,FKART,TCAMBIO,BELNR,
    ZTERM,BWTAR,DIM1,DIM2,DIM3,CUOBJ,MATNR2,ARKTX2,POSKI,RECO,UEPOS,NTGEW2,
    KWBRUM2,KZWI22,KZWI33,COSSER,COSMAT,RACCT2,RACCT3,PERNR_E2,EMNAM2,MACROS,SEGMEN,MICROS,
    CODMASE,CODSEGM,CODMISE,PORCDESC,COSTNOTI,PERID1,PERID2,COSTSUB,KNUMV,BZIRK,AIU,SEGMENT,
    NAME,KONDA,KONDATX,PERBL,ABSMG001,ERLOS001,VRPRS001,INGRESOUEN,COSTOUEN,UTILIDADUEN,TPNEGO,TPNEGONAME,UTILIDADOP,
    OTINGRESOP,CTOSNOOPER,INGRESOTOT,COSTOTOTAL,UTILIDADTO,OBSERV,INGPLANR,INGREALR,VKGRP,DESC_GP_VTAS,KVGR4,DESC_GP_CTE4,
    DESTINATARIO,NOMBRE_DES,ZONAVTAS_DEST,ESP_DESTINA,NOMBRE_ESP,COD_SOLI,NOMBRE_SOL,OF_SOLICITANTE,NOM_OFICINA,HIS_CTE,KVGR5,FLETE,
    ING_FLETE,COD_PROCESO,PROCESO,UTILIDAD_PLAN,RENTABILIDAD_PLAN,DESC_PAGO,BOLSA,MTPOS,BNAME,F_REGISTRO,NOMBRE_USUARIO
    """

    columnas_lista = columnas_mysql.replace("\n", "").replace(" ", "").split(",")

    placeholders = ",".join(["%s"] * len(columnas_lista))

    query = f"""
    INSERT INTO zsdvtacom1 ({columnas_mysql})
    VALUES ({placeholders})
    """

    log_conversiones = set()
    filas_insertadas = 0

    for fila in datos:

        valores = fila.split(";")
        valores = valores[:len(columnas_lista)]

        valores_convertidos = []

        for i, valor in enumerate(valores):

            columna = columnas_lista[i]

            # -------- CONVERSION FECHAS SAP --------
            if columna in ["FKDAT", "BUDAT", "F_REGISTRO"] and valor:

                try:
                    fecha = f"{valor[0:4]}-{valor[4:6]}-{valor[6:8]}"
                    valores_convertidos.append(fecha)

                    log_conversiones.add(
                        f"{columna} -> conversión fecha SAP YYYYMMDD → YYYY-MM-DD"
                    )

                except:
                    valores_convertidos.append(valor)

            else:
                valores_convertidos.append(valor)

        cursor.execute(query, valores_convertidos)
        filas_insertadas += 1

    conn.commit()

    print(f"\nFilas insertadas: {filas_insertadas}")

    if log_conversiones:
        print("\nConversiones realizadas:")
        for c in log_conversiones:
            print(c)

    cursor.close()
    conn.close()
    
def guardar_historial(usuario, tabla, anio, modo, mes_inicio, mes_fin, registros, estado):

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="aitv_com"
    )

    cursor = conn.cursor()

    sql = """
    INSERT INTO historial_extracciones
    (fecha, usuario_sap, tabla, anio, modo, mes_inicio, mes_fin, registros_insertados, estado)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    valores = (
        datetime.now(),
        usuario,
        tabla,
        anio,
        modo,
        mes_inicio,
        mes_fin,
        registros,
        estado
    )

    cursor.execute(sql, valores)

    conn.commit()
    conn.close()