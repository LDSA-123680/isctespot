import os
import uuid
import requests

class FastPayClient:
    """Client para FastPay (fictício).
    Tem MOCK mode (default) para correr localmente sem depender de API externa.
    """

    def __init__(self):
        self.base_url = os.getenv("FASTPAY_BASE_URL", "").rstrip("/")
        self.api_key = os.getenv("FASTPAY_API_KEY", "dev-fastpay-key")
        self.mock = os.getenv("FASTPAY_MOCK", "true").lower() == "true"

    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def associate_card(self, customer_id: int, card_data: dict) -> dict:
        if self.mock or not self.base_url:
            token = f"card_tok_{uuid.uuid4().hex}"
            card_number = (card_data.get("card_number") or "")
            last4 = card_number[-4:] if len(card_number) >= 4 else "0000"
            return {"status": "ok", "card_token": token, "last4": last4}

        url = f"{self.base_url}/associate/card/{customer_id}"
        resp = requests.post(url, json=card_data, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def process_multiple_payments(self, customer_id: int, list_of_targets: list) -> dict:
        payload = {"list_of_targets": list_of_targets}
        if self.mock or not self.base_url:
            return {"status": "ok", "processed": len(list_of_targets)}

        url = f"{self.base_url}/process/multiple-payments/{customer_id}"
        resp = requests.post(url, json=payload, headers=self._headers(), timeout=15)
        resp.raise_for_status()
        return resp.json()
