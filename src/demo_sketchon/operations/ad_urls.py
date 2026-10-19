from django.urls import path
from . import ad_delivery

urlpatterns = [
    path('display/', ad_delivery.display, name='ad-display'),
    path('<uuid:decision_id>/events/', ad_delivery.event, name='ad-event'),
]
