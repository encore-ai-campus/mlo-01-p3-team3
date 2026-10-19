"""Public, signed image capability; never a service credential or visitor identifier."""
from django.core import signing

SALT='sketchon.adserver.creative.v1'

def creative_path(decision):
    token=signing.Signer(salt=SALT).sign_object({'decision':str(decision.pk)})
    return f'/creative/{decision.pk}/{token}/'

def verify_creative(decision_id,token):
    return signing.Signer(salt=SALT).unsign_object(token)=={'decision':str(decision_id)}
