from datetime import date, timedelta
from typing import Annotated

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from db.firebird import (authenticate_user, get_tipos_planilla, query_nomina_detalle, query_certificado_laboral,
                         query_certificado_ingresos, get_anios, query_vacaciones, query_nomina_encabezado)

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

    titulo = "Principal"
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
            "titulo": titulo,
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
    titulo = "Planillas de nomina"

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
            "titulo": titulo,
            "error": None
        }
    )

@app.post("/nomina/detalle")
def ver_detalle_nomina(
    request: Request,
    fecha_inicio: Annotated[str, Form()],
    fecha_fin: Annotated[str, Form()],
    tipo: Annotated[str, Form()],
    numero: Annotated[int, Form()]
):
    try:
        cod_personal = int(request.cookies.get("cod_personal", 0))
        inicio = date.fromisoformat(fecha_inicio)
        fin = date.fromisoformat(fecha_fin)

        detalle = query_nomina_detalle(
            cod_personal=cod_personal,
            desde=inicio,
            hasta=fin,
            tipo=tipo,
            numero=numero
        )

        return jinja2_template.TemplateResponse(
            request=request,
            name="partials/detalle_nomina.html",
            context={
                "request": request,
                "detalle": detalle,
                "tipo": tipo,
                "numero": numero
            }
        )

    except ValueError:
        return jinja2_template.TemplateResponse(
            request=request,
            name="partials/detalle_nomina.html",
            context={
                "request": request,
                "detalle": [],
                "tipo": tipo,
                "numero": numero,
                "error": "Fechas inválidas"
            }
        )


@app.post("/nomina")
def consultar_nominas(
        request: Request,
        fecha_inicio: Annotated[str, Form()],
        fecha_fin: Annotated[str, Form()],
        tipo_planilla: Annotated[str, Form()],
        numero_planilla: Annotated[str | None, Form()] = None
):
    try:
        cod_personal = int(request.cookies.get("cod_personal", 0))
        inicio = date.fromisoformat(fecha_inicio)
        fin = date.fromisoformat(fecha_fin)

        if inicio > fin:
            return jinja2_template.TemplateResponse(
                request=request,
                name="partials/tabla_nomina.html",
                context={
                    "request": request,
                    "datos_nomina": []
                }
            )

    except ValueError:
        return jinja2_template.TemplateResponse(
            request=request,
            name="partials/tabla_nomina.html",
            context={
                "request": request,
                "datos_nomina": []
            }
        )

    datos_nomina = query_nomina_encabezado(
        cod_personal=cod_personal,
        desde=inicio,
        hasta=fin,
        tipo_planilla=tipo_planilla,
        numero_planilla=numero_planilla
    )

    return jinja2_template.TemplateResponse(
        request=request,
        name="partials/tabla_nomina.html",
        context={
            "request": request,
            "datos_nomina": datos_nomina
        }
    )


@app.get("/certificados", response_class=HTMLResponse)
def certificado(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    cod_personal = int(request.cookies.get("cod_personal", ""))
    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    estado = "Todos"
    titulo = "Certificados"

    return jinja2_template.TemplateResponse(
        request=request,
        name="Certificado_laboral.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "estado": estado,
            "anios": get_anios(cod_personal),
            "datos_certificado": [],
            "titulo": titulo,
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
        name="partials/tabla_certificado.html",
        context={
            "request": request,
            "datos_certificado": datos_certificado,
            "tipo_certificado": tipo_certificado
        }
    )

@app.get("/vacaciones", response_class=HTMLResponse)
def vacaciones(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    titulo = "Vacaciones"

    return jinja2_template.TemplateResponse(
        request=request,
        name="vacaciones.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "datos_vacaciones": [],
            "titulo": titulo,
            "error": None
        }
    )


@app.post("/vacaciones")
def consultar_vacaciones(
    request: Request,
    tipo_vacaciones: Annotated[str, Form()],
):
    cod_personal = int(request.cookies.get("cod_personal", 0))

    datos_vacaciones = query_vacaciones(cod_personal, tipo_vacaciones)

    return jinja2_template.TemplateResponse(
        request=request,
        name="partials/tabla_vacaciones.html",
        context={
            "request": request,
            "datos_vacaciones": datos_vacaciones,
            "tipo_vacaciones": tipo_vacaciones
        }
    )


@app.get("/ausentismo", response_class=HTMLResponse)
def ausentismo(request: Request):
    session_token = request.cookies.get("session_token")
    if not session_token:
        return RedirectResponse("/", status_code=303)

    nombre = request.cookies.get("nombre", "Usuario")
    email = request.cookies.get("email", "")
    titulo = "Ausentismo"

    return jinja2_template.TemplateResponse(
        request=request,
        name="ausentismo.html",
        context={
            "request": request,
            "nombre": nombre,
            "email": email,
            "titulo": titulo,
            "error": None
        }
    )
