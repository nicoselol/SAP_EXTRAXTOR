from sap_client import conectar_sap


# -------------------------------
# CAMPOS DIVIDIDOS EN GRUPOS
# -------------------------------

campos_1 = [
{"FIELDNAME": "MANDT"},
{"FIELDNAME": "ID"},
{"FIELDNAME": "GJAHR"},
{"FIELDNAME": "MES"},
{"FIELDNAME": "STATUS"},
{"FIELDNAME": "ORIGEN"},
{"FIELDNAME": "FKDAT"},
{"FIELDNAME": "BUDAT"},
{"FIELDNAME": "VBELN"},
{"FIELDNAME": "POSNR"},
{"FIELDNAME": "VBTYP"},
{"FIELDNAME": "VKBUR"},
]


campos_2 = [
{"FIELDNAME": "DOVTA"},
{"FIELDNAME": "KUNAG"},
{"FIELDNAME": "NAME1"},
{"FIELDNAME": "AUBEL"},
{"FIELDNAME": "AUPOS"},
{"FIELDNAME": "AUART"},
{"FIELDNAME": "SPART"},
{"FIELDNAME": "AUFNR"},
{"FIELDNAME": "WERKS"},
{"FIELDNAME": "NETWR"},
{"FIELDNAME": "KWAKRB"},
{"FIELDNAME": "KWBRUM"},
]


campos_3 = [
{"FIELDNAME": "WAVWR"},
{"FIELDNAME": "VVDDP"},
{"FIELDNAME": "PORCENT_COMISION"},
{"FIELDNAME": "WAERK"},
{"FIELDNAME": "KURRF"},
{"FIELDNAME": "PERNR_E"},
{"FIELDNAME": "VKBUR_E"},
{"FIELDNAME": "PERNR_I"},
{"FIELDNAME": "VKBUR_I"},
{"FIELDNAME": "CREAL"},
{"FIELDNAME": "PORENT"},
{"FIELDNAME": "MATNR"},
{"FIELDNAME": "ARKTX"},
]


campos_4 = [
{"FIELDNAME": "MVGR1"},
{"FIELDNAME": "BEZEI"},
{"FIELDNAME": "NTGEW"},
{"FIELDNAME": "GEWEI"},
{"FIELDNAME": "PRCTR"},
{"FIELDNAME": "LTEXT"},
{"FIELDNAME": "KDGRP"},
{"FIELDNAME": "KVGR1"},
{"FIELDNAME": "KVGR2"},
{"FIELDNAME": "PS_PSP_PNR"},
{"FIELDNAME": "LIQUIDACION"},
{"FIELDNAME": "RACCT"},
]


campos_5 = [
{"FIELDNAME": "DEVOL"},
{"FIELDNAME": "EMNAM"},
{"FIELDNAME": "KZWI1"},
{"FIELDNAME": "KZWI2"},
{"FIELDNAME": "KZWI3"},
{"FIELDNAME": "KZWI4"},
{"FIELDNAME": "KZWI6"},
{"FIELDNAME": "RFWRT"},
{"FIELDNAME": "CRTOTAL"},
{"FIELDNAME": "VBEL2"},
{"FIELDNAME": "FKART"},
{"FIELDNAME": "TCAMBIO"},
{"FIELDNAME": "BELNR"},
]

campos_6 = [
{"FIELDNAME": "ZTERM"},
{"FIELDNAME": "BWTAR"},
{"FIELDNAME": "DIM1"},
{"FIELDNAME": "DIM2"},
{"FIELDNAME": "DIM3"},
{"FIELDNAME": "CUOBJ"},
{"FIELDNAME": "MATNR2"},
{"FIELDNAME": "ARKTX2"},
{"FIELDNAME": "POSKI"},
{"FIELDNAME": "RECO"},
{"FIELDNAME": "UEPOS"},
{"FIELDNAME": "NTGEW2"},
]

campos_7 = [
{"FIELDNAME": "KWBRUM2"},
{"FIELDNAME": "KZWI22"},
{"FIELDNAME": "KZWI33"},
{"FIELDNAME": "COSSER"},
{"FIELDNAME": "COSMAT"},
{"FIELDNAME": "RACCT2"},
{"FIELDNAME": "RACCT3"},
{"FIELDNAME": "PERNR_E2"},
{"FIELDNAME": "EMNAM2"},
{"FIELDNAME": "MACROS"},
{"FIELDNAME": "SEGMEN"},
{"FIELDNAME": "MICROS"},
]

campos_8 = [
{"FIELDNAME": "CODMASE"},
{"FIELDNAME": "CODSEGM"},
{"FIELDNAME": "CODMISE"},
{"FIELDNAME": "PORCDESC"},
{"FIELDNAME": "COSTNOTI"},
{"FIELDNAME": "PERID1"},
{"FIELDNAME": "PERID2"},
{"FIELDNAME": "COSTSUB"},
{"FIELDNAME": "KNUMV"},
{"FIELDNAME": "BZIRK"},
{"FIELDNAME": "AIU"},
{"FIELDNAME": "SEGMENT"},
]

campos_9 = [
{"FIELDNAME": "NAME"},
{"FIELDNAME": "KONDA"},
{"FIELDNAME": "KONDATX"},
{"FIELDNAME": "PERBL"},
{"FIELDNAME": "ABSMG001"},
{"FIELDNAME": "ERLOS001"},
{"FIELDNAME": "VRPRS001"},
{"FIELDNAME": "INGRESOUEN"},
{"FIELDNAME": "COSTOUEN"},
{"FIELDNAME": "UTILIDADUEN"},
{"FIELDNAME": "TPNEGO"},
{"FIELDNAME": "TPNEGONAME"},
{"FIELDNAME": "UTILIDADOP"},
]

campos_10 = [
{"FIELDNAME": "OTINGRESOP"},
{"FIELDNAME": "CTOSNOOPER"},
{"FIELDNAME": "INGRESOTOT"},
{"FIELDNAME": "COSTOTOTAL"},
{"FIELDNAME": "UTILIDADTO"},
{"FIELDNAME": "OBSERV"},
{"FIELDNAME": "INGPLANR"},
{"FIELDNAME": "INGREALR"},
{"FIELDNAME": "VKGRP"},
{"FIELDNAME": "DESC_GP_VTAS"},
{"FIELDNAME": "KVGR4"},
{"FIELDNAME": "DESC_GP_CTE4"},
]

campos_11 = [
{"FIELDNAME": "DESTINATARIO"},
{"FIELDNAME": "NOMBRE_DES"},
{"FIELDNAME": "ZONAVTAS_DEST"},
{"FIELDNAME": "ESP_DESTINA"},
{"FIELDNAME": "NOMBRE_ESP"},
{"FIELDNAME": "COD_SOLI"},
{"FIELDNAME": "NOMBRE_SOL"},
{"FIELDNAME": "OF_SOLICITANTE"},
{"FIELDNAME": "NOM_OFICINA"},
{"FIELDNAME": "HIS_CTE"},
{"FIELDNAME": "KVGR5"},
{"FIELDNAME": "FLETE"},
]

campos_12 = [
{"FIELDNAME": "ING_FLETE"},
{"FIELDNAME": "COD_PROCESO"},
{"FIELDNAME": "PROCESO"},
{"FIELDNAME": "UTILIDAD_PLAN"},
{"FIELDNAME": "RENTABILIDAD_PLAN"},
{"FIELDNAME": "DESC_PAGO"},
{"FIELDNAME": "BOLSA"},
{"FIELDNAME": "MTPOS"},
{"FIELDNAME": "BNAME"},
{"FIELDNAME": "F_REGISTRO"},
{"FIELDNAME": "NOMBRE_USUARIO"},
]

# -------------------------------
# FUNCION QUE LEE UN GRUPO
# -------------------------------

def leer_grupo(conn, tabla, anio, campos, batch=500):

    rowskips = 0
    datos = []

    while True:

        result = conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE=tabla,
            DELIMITER=";",
            OPTIONS=[{"TEXT": f"GJAHR = '{anio}'"}],
            FIELDS=campos,
            ROWCOUNT=batch,
            ROWSKIPS=rowskips
        )

        filas = result["DATA"]

        if not filas:
            break

        for row in filas:
            datos.append(row["WA"])

        rowskips += batch

    return datos


# -------------------------------
# FUNCION PRINCIPAL
# -------------------------------

def leer_tabla_por_anio(tabla, anio, usuario, password):

    conn = conectar_sap(usuario, password)

    grupos = [
        campos_1, campos_2, campos_3, campos_4,
        campos_5, campos_6, campos_7, campos_8,
        campos_9, campos_10, campos_11, campos_12
    ]

    datos_grupos = []
    total_grupos = len(grupos)

    for i, grupo in enumerate(grupos):

        print(f"Leyendo grupo {i+1}...")

        datos = leer_grupo(conn, tabla, anio, grupo)

        datos_grupos.append(datos)

        progreso = int(((i + 1) / total_grupos) * 100)

        yield {
            "progreso": progreso,
            "grupo": i + 1,
            "total_grupos": total_grupos,
            "datos": datos_grupos if i == total_grupos - 1 else None
        }

def leer_grupo_mes(conn, tabla, anio, mes, campos, batch=500):

    rowskips = 0
    datos = []

    while True:

        result = conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE=tabla,
            DELIMITER=";",
            OPTIONS=[{"TEXT": f"GJAHR = '{anio}' AND MES = '{mes}'"}],
            FIELDS=campos,
            ROWCOUNT=batch,
            ROWSKIPS=rowskips
        )

        filas = result["DATA"]

        if not filas:
            break

        for row in filas:
            datos.append(row["WA"])

        rowskips += batch

    return datos

def leer_tabla_por_mes(tabla, anio, mes, usuario, password):
    conn = conectar_sap(usuario, password)

    print("Leyendo grupo 1...")
    datos1 = leer_grupo_mes(conn, tabla, anio, mes, campos_1)

    print("Leyendo grupo 2...")
    datos2 = leer_grupo_mes(conn, tabla, anio, mes, campos_2)

    print("Leyendo grupo 3...")
    datos3 = leer_grupo_mes(conn, tabla, anio, mes, campos_3)

    print("Leyendo grupo 4...")
    datos4 = leer_grupo_mes(conn, tabla, anio, mes, campos_4)

    print("Leyendo grupo 5...")
    datos5 = leer_grupo_mes(conn, tabla, anio, mes, campos_5)

    print("Leyendo grupo 6...")
    datos6 = leer_grupo_mes(conn, tabla, anio, mes, campos_6)

    print("Leyendo grupo 7...")
    datos7 = leer_grupo_mes(conn, tabla, anio, mes, campos_7)

    print("Leyendo grupo 8...")
    datos8 = leer_grupo_mes(conn, tabla, anio, mes, campos_8)

    print("Leyendo grupo 9...")
    datos9 = leer_grupo_mes(conn, tabla, anio, mes, campos_9)

    print("Leyendo grupo 10...")
    datos10 = leer_grupo_mes(conn, tabla, anio, mes, campos_10)

    print("Leyendo grupo 11...")
    datos11 = leer_grupo_mes(conn, tabla, anio, mes, campos_11)

    print("Leyendo grupo 12...")
    datos12 = leer_grupo_mes(conn, tabla, anio, mes, campos_12)

    datos_finales = []

    for i in range(len(datos1)):

        fila = (
            datos1[i] + ";" +
            datos2[i] + ";" +
            datos3[i] + ";" +
            datos4[i] + ";" +
            datos5[i] + ";" +
            datos6[i] + ";" +
            datos7[i] + ";" +
            datos8[i] + ";" +
            datos9[i] + ";" +
            datos10[i] + ";" +
            datos11[i] + ";" +
            datos12[i] + ";"
        )

        datos_finales.append(fila)

    return datos_finales

def leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos, batch=500):

    rowskips = 0
    datos = []

    while True:

        result = conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE=tabla,
            DELIMITER=";",
            OPTIONS=[{
                "TEXT": f"GJAHR = '{anio}' AND MES >= '{mes_inicio}' AND MES <= '{mes_fin}'"
            }],
            FIELDS=campos,
            ROWCOUNT=batch,
            ROWSKIPS=rowskips
        )

        filas = result["DATA"]

        if not filas:
            break

        for row in filas:
            datos.append(row["WA"])

        rowskips += batch

    return datos

def leer_tabla_por_rango(tabla, anio, mes_inicio, mes_fin, usuario, password):

    conn = conectar_sap(usuario, password)

    print("Leyendo grupo 1...")
    datos1 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_1)

    print("Leyendo grupo 2...")
    datos2 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_2)

    print("Leyendo grupo 3...")
    datos3 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_3)

    print("Leyendo grupo 4...")
    datos4 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_4)

    print("Leyendo grupo 5...")
    datos5 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_5)

    print("Leyendo grupo 6...")
    datos6 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_6)

    print("Leyendo grupo 7...")
    datos7 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_7)

    print("Leyendo grupo 8...")
    datos8 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_8)

    print("Leyendo grupo 9...")
    datos9 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_9)

    print("Leyendo grupo 10...")
    datos10 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_10)

    print("Leyendo grupo 11...")
    datos11 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_11)

    print("Leyendo grupo 12...")
    datos12 = leer_grupo_rango(conn, tabla, anio, mes_inicio, mes_fin, campos_12)

    datos_finales = []

    for i in range(len(datos1)):

        fila = (
            datos1[i] + ";" +
            datos2[i] + ";" +
            datos3[i] + ";" +
            datos4[i] + ";" +
            datos5[i] + ";" +
            datos6[i] + ";" +
            datos7[i] + ";" +
            datos8[i] + ";" +
            datos9[i] + ";" +
            datos10[i] + ";" +
            datos11[i] + ";" +
            datos12[i] + ";"
        )

        datos_finales.append(fila)

    return datos_finales