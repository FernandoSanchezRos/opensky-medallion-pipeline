from datetime import datetime, timedelta
import requests, os

class TokenManager:
    def __init__(self, token_url, client_id, client_secret, timeout=20):
        self.url = token_url # Endpoint OAuth2
        # Credenciales del API
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = str | None # Último token válido cacheado
        self._exp: datetime = datetime.min
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "opensky-poller/1.0"})

    def get(self) -> str | None:
        if not (self.client_id and self.client_secret):
            print("Faltan credenciales (client_id/client_secret); OAuth desactivado.")
            return None
        
        now = datetime.now()
        if self._token and now < self._exp:
            print("Token cache hit")
            return self._token
        
        print("Solicitando nuevo access_token…")
        try:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            r = requests.post(self.url, data=data, timeout=self.timeout)
            r.raise_for_status()
            payload = r.json()
            self._token = payload["access_token"]
            ttl = int(payload.get("expires_in", 1800))
            self._exp = now + timedelta(seconds=max(60, ttl - 60))
            print(f"Token obtenido, expira en {ttl} segundos")
            return self._token
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            print(f"Fallo al pedir el token: {status}")
            raise
        except requests.RequestException:
            print("Error de red al pedir el token")
            raise

    def auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get()}"}