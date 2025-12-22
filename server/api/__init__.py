from flask_cors import CORS
from flask import Flask
from api.auth.routes import auth
from api.company.routes import company
from api.sales.routes import sales
from api.clients.routes import clients
from api.admin.routes import admin
from api.payments.routes import payments
from services.payment_audit_middleware import register_payment_logging

def create_app(config_file='settings.py'):
    app = Flask(__name__)
    app.config.from_pyfile(config_file)

    CORS(auth, origins=["*"])
    CORS(clients, origins=["*"])
    CORS(sales, origins=["*"])
    CORS(company, origins=["*"])
    CORS(payments, origins=["*"])
    CORS(admin, origins=["*"])

    app.register_blueprint(auth)
    app.register_blueprint(company)
    app.register_blueprint(sales)
    app.register_blueprint(clients)
    app.register_blueprint(admin)
    app.register_blueprint(payments)

    # ✅ logging conforme DDT (só para endpoints de pagamentos)
    register_payment_logging(app)

    @app.route('/health', methods=['GET'])
    def health_check():
        return {
            'status': 'healthy',
            'message': 'ISCTE Spot API is running',
        }, 200

    return app
