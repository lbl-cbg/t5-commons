from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT


def start_session(client_id, private_key, token_url):
    session = OAuth2Session(
        client_id,
        private_key,
        PrivateKeyJWT(token_url),
        grant_type="client_credentials",
        token_endpoint=token_url
    )
    session.fetch_token()
    return session
