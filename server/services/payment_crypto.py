import os
import base64
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_KEYPAIR = None

def _keys_dir() -> str:
    return os.getenv("PAYMENT_KEYS_DIR", os.path.join(os.path.dirname(__file__), "..", "keys"))

def get_keypair():
    global _KEYPAIR
    if _KEYPAIR is not None:
        return _KEYPAIR

    keys_dir = os.path.realpath(_keys_dir())
    os.makedirs(keys_dir, exist_ok=True)

    priv_path = os.path.join(keys_dir, "payment_rsa_private.pem")
    pub_path  = os.path.join(keys_dir, "payment_rsa_public.pem")

    if os.path.exists(priv_path) and os.path.exists(pub_path):
        with open(priv_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        with open(pub_path, "rb") as f:
            public_key = serialization.load_pem_public_key(f.read())
    else:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        with open(priv_path, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ))
        with open(pub_path, "wb") as f:
            f.write(public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            ))

    _KEYPAIR = (private_key, public_key)
    return _KEYPAIR

def get_public_key_pem() -> str:
    _, public_key = get_keypair()
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")

def decrypt_hybrid_payload(envelope: dict) -> bytes:
    private_key, _ = get_keypair()
    enc_key = base64.b64decode(envelope["encrypted_key"])
    nonce = base64.b64decode(envelope["nonce"])
    ciphertext = base64.b64decode(envelope["ciphertext"])

    aes_key = private_key.decrypt(
        enc_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    aesgcm = AESGCM(aes_key)
    return aesgcm.decrypt(nonce, ciphertext, None)
