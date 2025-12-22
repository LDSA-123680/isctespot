import uuid
from db.db_connector import DBConnector
from services.fastpay_client import FastPayClient

class ProcessPayments:
    """Business logic para pagamentos (manual/agendado)."""

    def __init__(self, company_id: int, admin_user_id: int):
        self.company_id = company_id
        self.admin_user_id = admin_user_id
        self.dbc = DBConnector()
        self.fastpay = FastPayClient()

    def compute_targets(self, bonus_percentage: float):
        if bonus_percentage < 0:
            bonus_percentage = 0
        if bonus_percentage > 50:
            bonus_percentage = 50

        targets = self.dbc.execute_query("get_payment_targets", args={
            "comp_id": self.company_id,
            "bonus_pct": bonus_percentage
        })
        return targets or []

    def pay_now(self, bonus_percentage: float) -> dict:
        cust_id = self.dbc.execute_query("get_fastpay_customer_id", args=self.company_id)
        if not cust_id:
            cust_id = self.company_id

        targets = self.compute_targets(bonus_percentage)
        result = self.fastpay.process_multiple_payments(cust_id, targets)

        request_id = str(uuid.uuid4())
        self.dbc.execute_query("insert_payment_audit", args={
            "company_id": self.company_id,
            "admin_user_id": self.admin_user_id,
            "action": "PAY_NOW",
            "request_id": request_id,
            "status": result.get("status", "unknown"),
            "details": f"targets={len(targets)} bonus={bonus_percentage}",
        })
        return {"status": result.get("status", "unknown"), "processed": result.get("processed", 0), "request_id": request_id}
