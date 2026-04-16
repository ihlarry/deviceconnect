import os
import logging
from datetime import datetime
from flask import Blueprint, redirect, url_for, session, request
from google_auth_oauthlib.flow import Flow

from .firestore_storage import FirestoreStorage

bp = Blueprint("google_health_auth_bp", __name__)
log = logging.getLogger(__name__)

# Scopes required for Google Health API
GOOGLE_HEALTH_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness",
    "https://www.googleapis.com/auth/googlehealth.sleep",
    "https://www.googleapis.com/auth/googlehealth.body_and_weight",
    "https://www.googleapis.com/auth/googlehealth.nutrition",
    "https://www.googleapis.com/auth/googlehealth.heart_rate",
    "https://www.googleapis.com/auth/googlehealth.oxygen_saturation",
]

firestore_datasetname = os.environ.get("FIRESTORE_DATASET", "tokens")
firestorage = FirestoreStorage(firestore_datasetname)

def get_flow():
    client_id = os.environ.get("GOOGLE_HEALTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_HEALTH_CLIENT_SECRET")
    
    # Fallback to general openid credentials if specific ones aren't set
    if not client_id or not client_secret:
        client_id = os.environ.get("OPENID_AUTH_CLIENT_ID")
        client_secret = os.environ.get("OPENID_AUTH_CLIENT_SECRET")
        
    client_config = {
        "web": {
            "client_id": client_id,
            "project_id": os.environ.get("GOOGLE_CLOUD_PROJECT", "pericardits"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": client_secret
        }
    }
    
    return Flow.from_client_config(
        client_config, 
        scopes=GOOGLE_HEALTH_SCOPES
    )

@bp.route("/services/googlehealth/registration")
def google_health_registration():
    """Initiate Google Health API OAuth flow"""
    user = session.get("user")
    if user is None:
        # Fallback redirect if user not logged in to main app
        return redirect("/login")
        
    flow = get_flow()
    flow.redirect_uri = url_for('google_health_auth_bp.oauth2callback', _external=True)
    
    # Must explicitly request offline access to get a refresh_token
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent',
        include_granted_scopes='true'
    )
    
    session['state'] = state
    return redirect(authorization_url)

@bp.route("/services/googlehealth/callback")
def oauth2callback():
    user = session.get("user")
    if user is None:
        return redirect("/")

    state = session.get('state')
    flow = get_flow()
    flow.redirect_uri = url_for('google_health_auth_bp.oauth2callback', _external=True)

    authorization_response = request.url
    # Ensure protocol match for local dev behind proxies
    if authorization_response.startswith('http://'):
        authorization_response = authorization_response.replace('http://', 'https://')
        
    flow.fetch_token(authorization_response=authorization_response)
    credentials = flow.credentials
    
    # Structure token dictionary to mirror legacy expectations
    token_dict = {
        'access_token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_type': 'Bearer',
        'expires_in': (credentials.expiry - datetime.utcnow()).total_seconds() if credentials.expiry else 3600,
        'expires_at': credentials.expiry.timestamp() if credentials.expiry else None
    }
    
    username = user["email"]
    firestorage.save_google_token(username, token_dict)
    
    return redirect("/")
