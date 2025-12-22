import json
import uuid
from datetime import datetime
from flask import g, request

from db.db_connector import DBConnector

PAYMENT_PATHS = {
    "/company/add-card",
    "/company/edit-card",
    "/company/pay",
    "/company/schedule-pay",
    "/payments/add-account",
}

def _safe_json(obj, max_len=4000):
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        s = s[:max_len] + "...(truncated)"
    return s

def _sanitize_body(path: str, body: dict):
    if not isinstance(body, dict):
        return body

    sanitized = dict(body)

    # token nunca deve ser guardado em logs
    if "token" in sanitized:
        sanitized["token"] = "***REDACTED***"

    # dados sensíveis
    if path in ("/company/add-card", "/company/edit-card"):
        cn = sanitized.get("card_number")
        if cn and isinstance(cn, str):
            sanitized["card_number"] = f"**** **** **** {cn[-4:]}"
        if "cvc" in sanitized:
            sanitized["cvc"] = "***REDACTED***"

    if path == "/payments/add-account":
        iban = sanitized.get("bank_account_number")
        if iban and isinstance(iban, str) and len(iban) >= 6:
            sanitized["bank_account_number"] = f"{iban[:4]}...{iban[-4:]}"

    # se vier payload encriptado, não logar o conteúdo bruto
    if "encrypted_payload" in sanitized:
        sanitized["encrypted_payload"] = "***ENCRYPTED_BLOB_REDACTED***"

    return sanitized

def register_payment_logging(app):
    @app.before_request
    def _before():
        if request.path not in PAYMENT_PATHS:
            return

        g.request_id = str(uuid.uuid4())
        g.req_ts = datetime.utcnow().isoformat() + "Z"

    @app.after_request
    def _after(response):
        if request.path not in PAYMENT_PATHS:
            return response

        try:
            dbc = DBConnector()

            # headers (redact)
            hdrs = dict(request.headers)
            for k in list(hdrs.keys()):
                if k.lower() in ("authorization", "cookie"):
                    hdrs[k] = "***REDACTED***"

            # body
            body_obj = None
            try:
                body_obj = request.get_json(silent=True)
            except Exception:
                body_obj = None

            body_obj = _sanitize_body(request.path, body_obj if body_obj else {})

            # response body (limitado)
            resp_text = response.get_data(as_text=True)
            if len(resp_text) > 4000:
                resp_text = resp_text[:4000] + "...(truncated)"

            # tenta extrair user do body (token) sem logar token
            user_id = None
            username = None
            try:
                if isinstance(body_obj, dict):
                    # não temos token aqui porque já redatámos; isto é só “best effort”
                    pass
            except Exception:
                pass

            dbc.execute_query("insert_payment_api_log", args={
                "request_id": getattr(g, "request_id", str(uuid.uuid4())),
                "ip": request.remote_addr or "",
                "timestamp": getattr(g, "req_ts", datetime.utcnow().isoformat() + "Z"),
                "endpoint": request.path,
                "method": request.method,
                "headers": _safe_json(hdrs),
                "body": _safe_json(body_obj),
                "user_id": user_id,
                "username": username,
                "response_status": int(response.status_code),
                "response_body": resp_text,
            })
        except Exception:
            # logging nunca pode partir o request
            pass

        # útil para correlacionar com o report
        response.headers["X-Request-ID"] = getattr(g, "request_id", "")
        return response
