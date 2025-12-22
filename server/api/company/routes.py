import os
import json
from flask import Blueprint, request, jsonify, abort, send_file
from werkzeug.utils import secure_filename
from db.db_connector import DBConnector
from services.process_file import ProcessFile
from services.process_cash_flow import ProcessCashFlow
from services.process_sales import ProcessSales
from services.fastpay_client import FastPayClient
from services.process_payments import ProcessPayments
from services.payment_crypto import decrypt_hybrid_payload
from api.auth.jwt_utils import validate_token

company = Blueprint('company', __name__)

@company.route('/analytics', methods=['GET', 'POST'])
def list_clients():
    ''' List Sales function'''
    dbc = DBConnector()
    dict_data = request.get_json()
    is_valid, payload = validate_token(dict_data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    results = dbc.execute_query(query='get_company_sales', args=payload['comp_id'])
    pcf = ProcessCashFlow(payload['comp_id'], 'PT', month=7)
    revenue = pcf.revenue

    ps = ProcessSales(results, payload['user_id'])
    ps.get_3_most_recent_sales()

    if isinstance(results, list):
        return jsonify({'status': 'Ok', 'last_3_sales': ps.last_3_sales, 'revenue': revenue, 'sales': results}), 200
    return jsonify({'status': 'Bad request'}), 403


@company.route('/employees', methods=['GET', 'POST'])
def list_employees():
    ''' List employees function'''
    dbc = DBConnector()
    dict_data = request.get_json()
    is_valid, payload = validate_token(dict_data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    results = dbc.execute_query(query='get_employees_list', args=payload['comp_id'])
    if isinstance(results, list):
        return jsonify({'status': 'Ok', 'employees': results}), 200
    return jsonify({'status': 'Bad request'}), 403


@company.route('/products', methods=['GET', 'POST'])
def list_products():
    ''' List products for given company '''
    dbc = DBConnector()
    dict_data = request.get_json()
    is_valid, _payload = validate_token(dict_data.get('token'))
    if not is_valid:
        return jsonify({'status': 'Unauthorised'}), 403

    results = dbc.execute_query(query='get_products_list', args=_payload['comp_id'])
    if isinstance(results, list):
        return jsonify({'status': 'Ok', 'products': results}), 200
    return jsonify({'status': 'Bad request'}), 403


@company.route('/invoice', methods=['GET'])
def invoice():
    """
    FIX: Path Traversal (read) + auth.

    Agora:
      - exige token
      - só admin
      - secure_filename + realpath containment
      - só permite .pdf
    """
    token = request.args.get('token')
    if not token:
        return jsonify({'status': 'Bad request', 'message': 'Missing token'}), 400

    is_valid, payload = validate_token(token)
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    filename = request.args.get('filename', '')
    safe_name = secure_filename(filename)

    if not safe_name.lower().endswith('.pdf'):
        return jsonify({'status': 'Bad request', 'message': 'Only .pdf invoices are allowed'}), 400

    invoices_dir = os.path.join(os.path.dirname(__file__), 'invoices')
    base = os.path.realpath(invoices_dir)
    target = os.path.realpath(os.path.join(invoices_dir, safe_name))

    if not target.startswith(base + os.sep):
        return jsonify({'status': 'Bad request', 'message': 'Invalid filename'}), 400

    if not os.path.exists(target):
        abort(404, description="File not found")

    try:
        return send_file(target, as_attachment=True)
    except Exception as e:
        return str(e), 500


@company.route('/seller/update-commission', methods=['GET', 'POST'])
def update_commission():
    ''' Update seller commission '''
    dict_data = request.get_json()
    token = dict_data['token']
    seller_id = dict_data['seller_id']
    new_commission = dict_data['new_commission']

    is_valid, payload = validate_token(token)
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorized'}), 403

    dbc = DBConnector()
    dbc.execute_query(query='update_seller_commission', args={'seller_id': seller_id, 'new_commission': new_commission})
    return jsonify({'status': 'Ok', 'message': 'Commission updated'}), 200


@company.route('/update_products', methods=['POST'])
def upload_excel():
    ''' Update company products from csv or xlsx '''
    token = request.form.get('token')
    is_valid, payload = validate_token(token)
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorized'}), 403

    comp_id = payload.get('comp_id')
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    pf = ProcessFile(file, comp_id)
    if not pf.is_updated:
        return jsonify({'error': 'File processing failed'}), 400

    return jsonify({'status': 'Ok', 'message': 'File successfully uploaded'}), 200


@company.route('/cash-flow', methods=['POST'])
def cash_flow():
    ''' Calculate compnay's cash flow '''
    dict_data = request.get_json()
    is_valid, payload = validate_token(dict_data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    pcf7 = ProcessCashFlow(country_code=dict_data['country_code'], company_id=payload['comp_id'], month=7)
    pcf8 = ProcessCashFlow(country_code=dict_data['country_code'], company_id=payload['comp_id'], month=8)
    pcf9 = ProcessCashFlow(country_code=dict_data['country_code'], company_id=payload['comp_id'], month=9)

    return jsonify(
        {
            'profit': pcf7.profit + pcf8.profit + pcf9.profit,
            'status': 'Ok',
            'July': {
                'revenue': pcf7.month_revenue,
                'prod_costs': pcf7.month_prod_costs,
                'profit': pcf7.profit,
                'employees': pcf7.employees,
                'vat': pcf7.vat,
                'vat_value': pcf7.vat_value,
                'totalEmployeesPayment': pcf7.total_payment
            },
            'August': {
                'revenue': pcf8.revenue,
                'prod_costs': pcf8.month_prod_costs,
                'profit': pcf8.profit,
                'employees': pcf8.employees,
                'vat': pcf8.vat,
                'vat_value': pcf8.vat_value,
                'totalEmployeesPayment': pcf8.total_payment
            },
            'September': {
                'revenue': pcf9.revenue,
                'prod_costs': pcf9.month_prod_costs,
                'profit': pcf9.profit,
                'employees': pcf9.employees,
                'vat': pcf9.vat,
                'vat_value': pcf9.vat_value,
                'totalEmployeesPayment': pcf9.total_payment
            }
        }
    ), 200


# =========================
#  NEW FEATURE: Payments
# =========================

@company.route('/company/add-card', methods=['POST'])
def add_card():
    dbc = DBConnector()
    data = request.get_json() or {}
    is_valid, payload = validate_token(data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    if "encrypted_payload" in data:
        try:
            clear = decrypt_hybrid_payload(data["encrypted_payload"]).decode("utf-8")
            clear_data = json.loads(clear)
        except Exception:
            return jsonify({"status": "Bad request", "message": "Invalid encrypted payload"}), 400
    else:
        clear_data = data

    required = ["card_number", "card_holder_name", "expiry_date", "cvc", "card_type", "bank_identifier_code"]
    if any(k not in clear_data or not clear_data[k] for k in required):
        return jsonify({'status': 'Bad request'}), 400

    fastpay_customer_id = dbc.execute_query("get_fastpay_customer_id", args=payload["comp_id"])
    if not fastpay_customer_id:
        fastpay_customer_id = payload["comp_id"]
        dbc.execute_query("set_fastpay_customer_id", args={"comp_id": payload["comp_id"], "customer_id": fastpay_customer_id})

    fp = FastPayClient()
    fp_res = fp.associate_card(fastpay_customer_id, {
        "card_number": clear_data["card_number"],
        "card_holder_name": clear_data["card_holder_name"],
        "expiry_date": clear_data["expiry_date"],
        "cvc": clear_data["cvc"],
        "card_type": clear_data["card_type"],
        "bank_identifier_code": clear_data["bank_identifier_code"],
    })

    ok = dbc.execute_query("upsert_company_card", args={
        "comp_id": payload["comp_id"],
        "customer_id": fastpay_customer_id,
        "card_token": fp_res.get("card_token"),
        "last4": fp_res.get("last4"),
        "expiry_date": clear_data["expiry_date"],
        "card_type": clear_data["card_type"],
        "bic": clear_data["bank_identifier_code"],
    })

    if ok:
        return jsonify({"status": "Ok", "last4": fp_res.get("last4")}), 200
    return jsonify({"status": "Bad request"}), 400


@company.route('/company/edit-card', methods=['POST'])
def edit_card():
    return add_card()


@company.route('/company/pay', methods=['POST'])
def pay_now():
    data = request.get_json() or {}
    is_valid, payload = validate_token(data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    bonus = float(data.get("bonus_percentage", 0))
    pp = ProcessPayments(company_id=payload["comp_id"], admin_user_id=payload["user_id"])
    res = pp.pay_now(bonus)
    return jsonify({"status": "Ok", "result": res}), 200


@company.route('/company/schedule-pay', methods=['POST'])
def schedule_pay():
    dbc = DBConnector()
    data = request.get_json() or {}
    is_valid, payload = validate_token(data.get('token'))
    if not is_valid or not payload.get('is_admin'):
        return jsonify({'status': 'Unauthorised'}), 403

    freq = (data.get("frequency_type") or "").lower()
    if freq not in ("weekly", "monthly"):
        return jsonify({"status": "Bad request", "message": "frequency_type must be weekly|monthly"}), 400

    bonus = float(data.get("bonus_percentage", 0))
    ok = dbc.execute_query("upsert_payment_schedule", args={
        "comp_id": payload["comp_id"],
        "frequency_type": freq,
        "bonus_percentage": bonus,
    })
    if ok:
        return jsonify({"status": "Ok"}), 200
    return jsonify({"status": "Bad request"}), 400
