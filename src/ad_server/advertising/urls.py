from django.urls import path
from django.http import HttpResponse
from . import views

urlpatterns=[
    path('favicon.ico',lambda request:HttpResponse(status=204)),
    path('',views.home,name='ad-home'),
    path('health/',views.health),
    path('api/v1/templates/',views.templates),
    path('assets/<slug:template_id>/',views.asset),
    path('creative/<uuid:decision_id>/<str:token>/',views.decision_asset),
    path('embed/<slug:template_id>/',views.embed),
    path('internal/v1/decisions/',views.decisions_api),
    path('internal/v1/decisions/<uuid:decision_id>/',views.decision_api),
    path('internal/v1/decisions/<uuid:decision_id>/events/',views.events_api),
    path('internal/v1/reports/',views.reports_api),
    path('internal/v1/ui/contract/',views.ui_contract_api),
    path('internal/v1/ui/decisions/',views.ui_decisions_api),
    path('internal/v1/ui/campaigns/validate/',views.ui_campaign_validation_api),
    path('sandbox/',views.sandbox),
    path('sandbox/decisions/',views.sandbox_decision),
    path('sandbox/decisions/<uuid:decision_id>/events/',views.sandbox_event),
    path('sandbox/report/',views.sandbox_report),
    path('test-destination/<slug:slug>/',views.test_destination),
    path('test-ended/',views.test_destination),
]
