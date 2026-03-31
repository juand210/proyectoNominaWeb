from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import Annotated, Optional
from db.firebird import (authenticate_user, get_tipos_planilla, query_nomina, query_certificado_laboral,
                         query_certificado_ingresos, get_anios, query_vacaciones)
from datetime import date, timedelta

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")  # ← Línea clave
jinja2_template = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return jinja2_template.TemplateResponse(
        request=request,
        name="login.html",
        context={"request": request}
    )


@app.get("/home", response_class=HTMLResponse)
def home(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    cod_personal = request.cookies.get("cod_personal", "")
    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")

    return jinja2_template.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "request": request,
            "cod_personal": cod_personal,
            "nombre": nombre,
            "email": email
        }
    )


@app.post("/login")
async def login(request: Request, cod_personal: Annotated[int, Form()],  # ← str NO int
                password: Annotated[str, Form()]):
    user = authenticate_user(cod_personal, password)

    if user:
        response = RedirectResponse("/home", status_code=303)
        response.set_cookie(key="session_token", value=f"user_{user['cod_personal']}", httponly=True)
        response.set_cookie(key="cod_personal", value=str(user['cod_personal']), httponly=True)
        response.set_cookie(key="nombre", value=user['nombre'], httponly=False)
        response.set_cookie(key="email", value=user['email'], httponly=False)
        response.set_cookie(key="theme_reset", value="0", httponly=False)

        return response


@app.get("/logout")
async def logout():
    response = RedirectResponse("/", status_code=303)
    response.delete_cookie("session_token")
    response.delete_cookie("cod_personal")
    response.delete_cookie("nombre")
    response.delete_cookie("email")

    response.set_cookie(key="theme_reset", value="1", httponly=False)  # ←

    return response


@app.get("/nomina", response_class=HTMLResponse)
def nomina(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    hoy = date.today()
    mes_pasado = hoy - timedelta(days=30)

    return jinja2_template.TemplateResponse(
        request=request,
        name="nomina.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "fecha_inicio": mes_pasado.isoformat(),
            "fecha_fin": hoy.isoformat(),
            "tipos_planilla": get_tipos_planilla("PL"),
            "numero_planilla": "",
            "datos_nomina": [],
            "error": None
        }
    )


@app.post("/nomina")
def consultar_nomina(
        request: Request,
        fecha_inicio: Annotated[str, Form()],
        fecha_fin: Annotated[str, Form()],
        tipo_planilla: Annotated[str, Form()],
        numero_planilla: Annotated[str | None, Form()] = None
):
    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    cod_personal = int(request.cookies.get("cod_personal", 0))
    try:
        inicio = date.fromisoformat(fecha_inicio)
        fin = date.fromisoformat(fecha_fin)
        if inicio > fin:
            return jinja2_template.TemplateResponse(
                request=request,
                name="nomina.html",
                context={
                    "request": request, "nombre": nombre, "email": email,
                    "fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin,
                    "tipo_planilla": tipo_planilla, "numero_planilla": numero_planilla or "",
                    "tipos_planilla": get_tipos_planilla("PL"),
                    "datos_nomina": [],
                    "error": "Fecha inicio no puede ser mayor a fecha fin"
                }
            )
    except ValueError:
        return jinja2_template.TemplateResponse(
            request=request,
            name="nomina.html",
            context={
                "request": request, "nombre": nombre, "email": email,
                "fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin,
                "tipo_planilla": tipo_planilla, "numero_planilla": numero_planilla or "",
                "tipos_planilla": get_tipos_planilla("PL"),
                "datos_nomina": [],
                "error": "Formato de fecha inválido"
            }
        )

    datos_nomina = query_nomina(
        cod_personal=cod_personal,
        desde=inicio,
        hasta=fin,
        tipo_planilla=tipo_planilla,
        numero_planilla=numero_planilla
    )

    return jinja2_template.TemplateResponse(
        request=request,
        name="nomina.html",
        context={
            "request": request, "nombre": nombre, "email": email,
            "fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin,
            "tipo_planilla": tipo_planilla, "numero_planilla": numero_planilla or "",
            "tipos_planilla": get_tipos_planilla("PL"),
            "datos_nomina": datos_nomina,
            "error": None
        }
    )


@app.get("/certificados", response_class=HTMLResponse)
def certificado(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    estado = "Todos"

    return jinja2_template.TemplateResponse(
        request=request,
        name="Certificado_laboral.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "estado": estado,
            "anios": get_anios(),
            "datos_certificado": [],
            "error": None
        }
    )


@app.post("/certificados")
def consultar_certificados(
        request: Request,
        tipo_certificado: Annotated[str, Form()],
        estado: Annotated[str | None, Form()] = None,
        periodo_ingresos: Annotated[int | None, Form()] = None
):
    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    cod_personal = int(request.cookies.get("cod_personal", 0))

    if tipo_certificado == "laboral":
        datos_certificado = query_certificado_laboral(
            cod_personal=cod_personal,
            estado=estado
        )
    else:
        datos_certificado = query_certificado_ingresos(
            cod_personal=cod_personal,
            periodo_ingresos=periodo_ingresos
        )

    return jinja2_template.TemplateResponse(
        request=request,
        name="Certificado_laboral.html",
        context={
            "request": request, "nombre": nombre, "email": email,
            "estado": estado, "periodo_ingresos": periodo_ingresos,
            "tipo_certificado": tipo_certificado,
            "datos_certificado": datos_certificado,
            "anios": get_anios(),
            "error": None
        }
    )


@app.get("/vacaciones", response_class=HTMLResponse)
def vacaciones(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")

    return jinja2_template.TemplateResponse(
        request=request,
        name="vacaciones.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "datos_vacaciones": [],
            "error": None
        }
    )


@app.post("/vacaciones")
def consultar_certificados(
        request: Request,
        tipo_vacaciones: Annotated[str, Form()],
):
    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    cod_personal = int(request.cookies.get("cod_personal", 0))

    datos_vacaciones = query_vacaciones(cod_personal, tipo_vacaciones)

    return jinja2_template.TemplateResponse(
        request=request,
        name="vacaciones.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "tipo_vacaciones": tipo_vacaciones,
            "datos_vacaciones": datos_vacaciones,
            "error": None
        }
    )

