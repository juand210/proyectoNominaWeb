import fdb
from typing import Optional
from config import DATABASE_DSN, DATABASE_USER, DATABASE_PASSWORD
from datetime import date


def get_firebird_connection():
    return fdb.connect(
        dsn=DATABASE_DSN,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        charset="UTF8",
    )


def authenticate_user(cod_personal: int, passweb: str) -> Optional[dict]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        sql = """
            SELECT
                COD_PERSONAL,
                NOMBRE,
                CORREO_ELECTRONICO
            FROM NR_PERSONAL
            WHERE COD_PERSONAL = ? AND PASSWEB = ?
        """
        cur.execute(sql, (cod_personal, passweb))
        row = cur.fetchone()

        if not row:
            return None

        return {
            "cod_personal": row[0],
            "nombre": row[1],
            "email": row[2],
        }

    except Exception as e:
        print(f"Error conexión Firebird: {e}")
        return None
    finally:
        if con:
            con.close()


def get_tipos_planilla(tipo: str = None) -> list[tuple[str, str]]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        if tipo:
            sql = """
                SELECT DISTINCT CLASE, DESCRIPCION 
                FROM TIPDOC 
                WHERE TIPO = ? AND CLASE IS NOT NULL AND DESCRIPCION IS NOT NULL
                ORDER BY CLASE
            """
            cur.execute(sql, (tipo,))

        rows = cur.fetchall()
        return [(row[0], row[1]) for row in rows if row[0] and row[1]]

    except Exception as e:
        print(f"Error tipos planilla: {e}")
        return [("PL", "Planilla Ordinaria"), ("OT", "Otro")]
    finally:
        if con:
            con.close()


def get_anios() -> list[str]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        sql = """
            SELECT DISTINCT EXTRACT(YEAR FROM FECHA) AS ANIO
            FROM nr_rets_personal_mst
            WHERE FECHA IS NOT NULL
            ORDER BY ANIO DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            return [str(row[0]) for row in rows]
        else:
            return [str(date.today().year)]
    except Exception as e:
        print(f"Error tipos planilla: {e}")
        return [str(date.today().year)]
    finally:
        if con:
            con.close()


def query_nomina(cod_personal: int, desde: date, hasta: date, tipo_planilla: str, numero_planilla: str = None) -> list[dict]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        # SQL base
        sql = """
            SELECT DISTINCT
                P.COD_TIPO, P.NUMERO, P.COD_EMPRESA, P.COD_SUCURSAL,
                P.FECHA_INICIAL, P.FECHA_FINAL, P.COD_TIPO_NOMINA, P.COD_PERIODO PERIODO,
                P.COD_CONCEPTO, P.BASE, P.COD_PERSONAL, P.COD_VINCULOLB,
                P.DEVENGO, P.DEDUCCION, TN.DESCRIPCION AS NOMBRETN, 
                CO.DESCRIPCION AS NOMBRECO, MST.SYNC_APLIC_NOMINA, E.NOMBRE,
                'PLANILLA' AS LIQUIDADO
            FROM LIQUIDACIONES P
            INNER JOIN NR_CLASE_LIQUIDACION TN ON (P.COD_TIPO_NOMINA = TN.COD_TIPO_NOMINA)
            INNER JOIN NR_DEDUCCION CO ON (P.COD_CONCEPTO = CO.COD_CONCEPTO)
            INNER JOIN NR_PERSONAL E ON (P.COD_PERSONAL = E.COD_PERSONAL)
            INNER JOIN NR_PERSONAL_NOMINA_MST MST ON (P.NUMERO = MST.NUMERO)
            WHERE P.COD_PERSONAL = ? 
            AND P.FECHA_INICIAL >= ? 
            AND P.FECHA_INICIAL <= ?
            AND P.COD_TIPO = ?
        """
        params = [cod_personal, desde, hasta, tipo_planilla]

        if numero_planilla and numero_planilla.strip():
            sql += " AND P.NUMERO = ?"
            params.append(numero_planilla.strip())

        sql += " ORDER BY P.FECHA_INICIAL DESC, E.NOMBRE"

        cur.execute(sql, params)
        rows = cur.fetchall()

        datos_nomina = []
        for row in rows:
            datos_nomina.append({
                "tipo": row[0],
                "numero": row[1],
                "periodo": row[7],
                "fecha_inicial": row[4].strftime("%d-%m-%Y"),
                "fecha_final": row[5].strftime("%d-%m-%Y"),
                "contrato": row[11],
                "empleado": row[18],
                "cedula": row[10],
                "cod_tipo_nom": row[6], "nombre_tipo_nom": row[14],
                "concepto": row[8], "nombre_concepto": row[15],
                "base": f"${row[9]:,.0f}" if row[9] else "$0",
                "devengo": f"${row[12]:,.0f}" if row[12] else "$0",
                "deduccion": f"${row[13]:,.0f}" if row[13] else "$0",
                "neto": f"${(row[12] or 0) - (row[13] or 0):,.0f}"
            })

        return datos_nomina

    except Exception as e:
        print(f"Error query nomina: {e}")
        return []
    finally:
        if con:
            con.close()


def query_certificado_laboral(cod_personal: int, estado: str = None) -> list[dict]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        sql = """
            SELECT 
                V.COD_TIPO, V.NUMERO, V.COD_EMPRESA, V.COD_SUCURSAL,
                V.COD_PERSONAL, V.COD_VINCULOLB, V.FECHA,
                V.FECHA_INGRESO, V.FECHA_RETIRO, V.SUELDO_BASICO, TRIM(V.ESTADO) AS ESTADO
            FROM NR_VINCULO V
            WHERE V.COD_PERSONAL = ?
        """
        params = [cod_personal]

        if not estado == "TODOS":
            sql += " AND UPPER(V.ESTADO) = UPPER(?)"
            params.append(estado.strip())

        sql += " ORDER BY V.FECHA DESC, V.NUMERO"

        cur.execute(sql, params)
        rows = cur.fetchall()

        datos_certificado = []
        for row in rows:
            datos_certificado.append({
                "tipo": row[0] or "",
                "numero": row[1] or "",
                "empresa": row[2],
                "sucursal": row[3],
                "cod_personal": row[4],
                "cod_vinculo": row[5],
                "fecha": row[6].strftime("%d/%m/%Y") if row[6] else "",
                "fecha_ingreso": row[7].strftime("%d/%m/%Y") if row[7] else "",
                "fecha_retiro": row[8].strftime("%d/%m/%Y") if row[8] else "",
                "sueldo": f"${row[9]:,.0f}" if row[9] else "$0",
                "estado": row[10] or ""
            })

        return datos_certificado

    except Exception as e:
        print(f"Error query certificado: {e}")
        return []
    finally:
        if con:
            con.close()


def query_certificado_ingresos(cod_personal: int, periodo_ingresos: int) -> list[dict]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        sql = """
            SELECT N.COD_PERSONAL, N.COD_VINCULOLB, N.SALARIO_INGRESO, N.CESANTIAS_INTERESES,
            N.GASTO_REPRESENTACION, N.PENSION_JVI, N.OTROS_INGRESOS, N.TOTAL_INGRESOS_BRUTO,
            N.APORTES_OBLIGATORIOS, N.APORTES_VOLUNTARIOS, N.APORTES_OBLIGATORIOS_PENSION, E.SYNC_APLIC_NOMINA
            FROM NR_RETS_PERSONAL_DTL N
            INNER JOIN NR_RETS_PERSONAL_MST E ON (N.COD_EMPRESA=E.COD_EMPRESA AND N.COD_SUCURSAL=E.COD_SUCURSAL AND 
            N.COD_TIPO=E.COD_TIPO AND N.NUMERO=E.NUMERO)
            WHERE N.COD_PERSONAL= ? AND E.ANNO= ?
            AND N.COD_TIPO IN (SELECT CLASE FROM TIPDOC WHERE TIPO = 'IR')
        """
        params = [cod_personal, periodo_ingresos]

        cur.execute(sql, params)
        rows = cur.fetchall()

        datos_certificado = []

        def fmt_val(v):
            return "$0" if v in (None, "") else v

        for row in rows:
            datos_certificado.append({
                "cod_personal": fmt_val(row[0]),
                "cod_vinculo": fmt_val(row[1]),
                "salario": fmt_val(row[2]),
                "cesantias": fmt_val(row[3]),
                "gasto": fmt_val(row[4]),
                "pension": fmt_val(row[5]),
                "otros": fmt_val(row[6]),
                "total_ingresos": fmt_val(row[7]),
                "aportes": fmt_val(row[8]),
                "aportes_voluntarios": fmt_val(row[9]),
                "aportes_pension": fmt_val(row[10]),
            })

        return datos_certificado

    except Exception as e:
        print(f"Error query certificado: {e}")
        return []
    finally:
        if con:
            con.close()


def query_vacaciones(cod_personal: int, tipo_liq: str) -> list[dict]:
    con = None
    try:
        con = get_firebird_connection()
        cur = con.cursor()

        if tipo_liq == "liquida_vacacion":
            sql = """
                SELECT L.COD_EMPRESA,L.COD_SUCURSAL,L.COD_TIPO,MAX(L.NUMERO)NUMERO,MAX(LD.PERIODO_CAUSACION_INI) AS DESDE,
                MAX(LD.PERIODO_CAUSACION_FIN) AS HASTA,TN.DESCRIPCION AS NOMBRETN,L.COD_TIPO_NOMINA,
                LD.COD_PERSONAL,LD.COD_VINCULOLB,E.NOMBRE,MAX(LD.PERIODO_CAUSACION_FIN+1) FECHA_INICIAL,
                0 AS DISFRUTADAS,(0.0) as TOTAL,LD.COD_PERSONAL,LD.COD_VINCULOLB,E.NOMBRE,
                MAX(L.VACACIONES_HASTA+1) FECHA_INICIAL,0 AS DISFRUTADAS,(0.0) as TOTAL
                from NR_PERSONAL_VAC_MST L
                INNER JOIN NR_PERSONAL_VAC_DTL LD ON (L.COD_EMPRESA = LD.COD_EMPRESA AND
                L.COD_SUCURSAL=LD.COD_SUCURSAL AND L.COD_TIPO=LD.COD_TIPO AND L.NUMERO=LD.NUMERO)
                INNER JOIN NR_PERSONAL E ON (LD.COD_PERSONAL=E.COD_PERSONAL)
                INNER JOIN NR_CLASE_LIQUIDACION TN ON (L.COD_TIPO_NOMINA = TN.COD_TIPO_NOMINA)
                INNER JOIN NR_VINCULO CO ON ((LD.COD_EMPRESA=CO.COD_EMPRESA) AND
                (LD.COD_SUCURSAL=CO.COD_SUCURSAL) AND (LD.COD_VINCULOLB=CO.COD_VINCULOLB) AND
                (LD.POS_VINCULOLB=CO.POS_VINCULOLB)
                AND (LD.COD_PERSONAL=CO.COD_PERSONAL) AND
                CO.ESTADO NOT IN ('RETIRADO','LIQUIDADO'))
                WHERE (LD.COD_PERSONAL) = ?
                GROUP BY L.COD_EMPRESA,L.COD_SUCURSAL,L.COD_TIPO,TN.DESCRIPCION,
                L.COD_TIPO_NOMINA,LD.COD_PERSONAL,LD.COD_VINCULOLB,E.NOMBRE
            """
            params = (f"{cod_personal}%",)

            cur.execute(sql, params)
            rows = cur.fetchall()

            # DATOS PARA LIQUIDADOS
            datos_vacaciones = []
            for row in rows:
                datos_vacaciones.append({
                    "dias_compensar": row[0] if row[0] is not None else 0,
                    "cod_empresa": row[1] or "",
                    "cod_sucursal": row[2] or "",
                    "cod_tipo": row[3] or "",
                    "numero": row[4] or "",
                    "desde": row[5].strftime("%d/%m/%Y") if row[5] else "",
                    "hasta": row[6].strftime("%d/%m/%Y") if row[6] else "",
                    "nombre_tipo_nomina": row[7] or "",
                    "cod_tipo_nomina": row[8] or "",
                    "cod_personal": row[9] or "",
                    "cod_vinculo": row[10] or "",
                    "nombre": row[11] or "",
                    "fecha_inicial": row[12].strftime("%d/%m/%Y") if row[12] else "",
                    "disfrutadas": row[13] if row[13] is not None else 0,
                    "total_vacaciones": row[14] if row[14] is not None else 0.0,
                })
            return datos_vacaciones

        else:
            sql = """
                SELECT 0.0 DIAS_COMPENSAR,L.COD_EMPRESA,L.COD_SUCURSAL,L.COD_TIPO,L.NUMERO,
                L.FECHA_ULT_VACACION AS FECHA_INICIAL, L.FECHA_PER_VAC_INI DESDE ,L.FECHA_PER_VAC_FIN  HASTA,
                'SIN' AS ESTADO_VACACION,L.COD_PERSONAL, L.COD_VINCULOLB,E.NOMBRE,TN.DESCRIPCION AS NOMBRETN,
                L.COD_TIPO_NOMINA from NR_VINCULO L
                INNER JOIN NR_PERSONAL E ON (L.COD_PERSONAL=E.COD_PERSONAL) 
                INNER JOIN NR_CLASE_LIQUIDACION TN ON (L.COD_TIPO_NOMINA = TN.COD_TIPO_NOMINA)
                WHERE (L.COD_PERSONAL) = ? AND L.ESTADO NOT IN ('RETIRADO','LIQUIDADO')
                AND ((L.COD_PERSONAL||L.COD_VINCULOLB) NOT IN 
                (SELECT DISTINCT (COD_PERSONAL||COD_VINCULOLB) FROM NR_PERSONAL_VAC_DTL ))
            """
            params = (cod_personal,)

            cur.execute(sql, params)
            rows = cur.fetchall()

            # DATOS PARA NO LIQUIDADOS
            datos_vacaciones = []
            for row in rows:
                datos_vacaciones.append({
                    "dias_compensar": row[0] if row[0] is not None else 0,
                    "cod_empresa": row[1] or "",
                    "cod_sucursal": row[2] or "",
                    "cod_tipo": row[3] or "",
                    "numero": row[4] or "",
                    "fecha_inicial": row[5].strftime("%d/%m/%Y") if row[5] else "",
                    "desde": row[6].strftime("%d/%m/%Y") if row[6] else "",
                    "hasta": row[7].strftime("%d/%m/%Y") if row[7] else "",
                    "estado_vacacion": row[8] or "",
                    "cod_personal": row[9] or "",
                    "cod_vinculo": row[10] or "",
                    "nombre": row[11] or "",
                    "nombre_tipo_nomina": row[12] or "",
                    "cod_tipo_nomina": row[13] or ""
                })
            return datos_vacaciones

    except Exception as e:
        print(f"Error conexión Firebird: {e}")
        return []
    finally:
        if con:
            con.close()



