import json
from flask import Blueprint, request, jsonify
from api.auth.jwt_utils import validate_token
from db.db_connector import DBConnector
from services.payment_crypto import get_public_key_pem, decrypt_hybrid_payload

payments = Blueprint('payments', __name__)

@payments.route('/crypto/payment-public-key', methods=['GET'])
def payment_public_key():
    return jsonify({"status": "Ok", "public_key_pem": get_public_key_pem()}), 200

@payments.route('/payments/add-account', methods=['POST'])
def add_account():
    dbc = DBConnector()
    data = request.get_json() or {}

    is_valid, payload = validate_token(data.get("token"))
    if not is_valid:
        return jsonify({"status": "Unauthorised"}), 403

    if "encrypted_payload" in data:
        try:
            clear = decrypt_hybrid_payload(data["encrypted_payload"]).decode("utf-8")
            clear_data = json.loads(clear)
        except Exception:
            return jsonify({"status": "Bad request", "message": "Invalid encrypted payload"}), 400
    else:
        clear_data = data

    bank_account_number = clear_data.get("bank_account_number")
    bic = clear_data.get("bank_identifier_code")

    if not bank_account_number or not bic:
        return jsonify({"status": "Bad request"}), 400

    if len(bank_account_number) < 10 or len(bank_account_number) > 34:
        return jsonify({"status": "Bad request", "message": "Invalid bank account number length"}), 400

    ok = dbc.execute_query("upsert_payment_account", args={
        "user_id": payload["user_id"],
        "bank_account_number": bank_account_number,
        "bic": bic,
    })
    if ok:
        return jsonify({"status": "Ok"}), 200
    return jsonify({"status": "Bad request"}), 400
