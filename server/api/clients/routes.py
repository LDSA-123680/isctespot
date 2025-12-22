from flask import Blueprint, request, jsonify
from db.db_connector import DBConnector
from api.auth.jwt_utils import validate_token

clients = Blueprint('clients', __name__)

@clients.route('/clients', methods=['GET', 'POST'])
def list_clients():
    """List clients (admin e employees da mesma empresa)."""
    dbc = DBConnector()
    dict_data = request.get_json() or {}
    token = dict_data.get('token')

    is_valid, payload = validate_token(token)
    if not is_valid:
        return jsonify({'status': 'Unauthorised'}), 403

    # Usa comp_id do token (assinado) e fallback para DB se necessário
    comp_id = payload.get('comp_id') or dbc.execute_query(query='get_compnay_id_by_user', args=payload['user_id'])
    results = dbc.execute_query(query='get_clients_list', args=comp_id)

    if isinstance(results, list):
        return jsonify({'status': 'Ok', 'clients': results}), 200
    return jsonify({'status': 'Bad request'}), 400


@clients.route('/clients/new', methods=['POST'])
def new_client():
    """Create a new client (admin e employees)."""
    dbc = DBConnector()
    dict_data = request.get_json() or {}
    token = dict_data.get('token')

    is_valid, payload = validate_token(token)
    if not is_valid:
        return jsonify({'status': 'Unauthorised'}), 403

    comp_id = payload.get('comp_id') or dbc.execute_query(query='get_compnay_id_by_user', args=payload['user_id'])
    if not comp_id:
        return jsonify({'status': 'Bad request', 'message': 'Missing company context'}), 400

    # validação mínima para não rebentar com KeyError
    required = ['first_name', 'last_name', 'email', 'phone_number', 'address', 'city', 'country']
    if any(k not in dict_data for k in required):
        return jsonify({'status': 'Bad request', 'message': 'Missing fields'}), 400

    result = dbc.execute_query('create_client', args={
        'comp_id': comp_id,
        'first_name': dict_data['first_name'],
        'last_name': dict_data['last_name'],
        'email': dict_data['email'],
        'phone_number': dict_data['phone_number'],
        'address': dict_data['address'],
        'city': dict_data['city'],
        'country': dict_data['country'],
    })

    if isinstance(result, int):
        return jsonify({'status': 'Ok', 'client_id': result}), 200
    return jsonify({'status': 'Bad request'}), 400


@clients.route('/clients/delete', methods=['POST'])
def delete_client():
    """Delete client (FIX: só admin)."""
    dbc = DBConnector()
    dict_data = request.get_json() or {}
    token = dict_data.get('token')

    is_valid, payload = validate_token(token)
    if (not is_valid) or (not payload.get('is_admin')):
        return jsonify({'status': 'Unauthorised'}), 403

    client_id = dict_data.get('client_id')
    if not client_id:
        return jsonify({'status': 'Bad request'}), 400

    result = dbc.execute_query(query='delete_client_by_id', args=client_id)
    if result is True or isinstance(result, int):
        return jsonify({'status': 'Ok', 'client_id': client_id}), 200
    return jsonify({'status': 'Bad request'}), 400
