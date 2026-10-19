from django.urls import path
from . import views

urlpatterns=[
    path('',views.dashboard),path('people/',views.people),path('projects/',views.projects),
    path('projects/<uuid:project_id>/',views.project_detail),path('events/',views.events),
    path('collection/',views.collection),path('collection/index/',views.index_collection),
    path('collection/<uuid:receipt_id>/review/',views.review_raw),
    path('media/projects/<uuid:project_id>/photo/',views.project_photo),
    path('media/results/<uuid:result_id>/image/',views.result_image),
    path('media/raw/<uuid:receipt_id>/',views.raw_file),
    path('ads/',views.ads),path('ads/new/',views.campaign_edit),path('ads/<uuid:campaign_id>/edit/',views.campaign_edit),
    path('ads/<uuid:campaign_id>/review/',views.campaign_review),path('ads/<uuid:campaign_id>/toggle/',views.campaign_toggle),
    path('ads/<uuid:campaign_id>/image/',views.campaign_image),path('ad-reports/',views.ad_reports),path('audit/',views.audit_log),
]
