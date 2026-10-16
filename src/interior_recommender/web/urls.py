from django.urls import path
from . import views

urlpatterns = [
    path("api/runs/<uuid:run_id>/product-link", views.product_link),
    path("", views.index), path("api/csrf", views.csrf), path("api/templates", views.catalog),
    path("api/templates/register", views.register_template), path("api/runs", views.submit),
    path("api/runs/<uuid:run_id>", views.state), path("api/runs/<uuid:run_id>/selection", views.selection),
    path("api/runs/<uuid:run_id>/actions/<str:name>", views.action),
    path("assets/<uuid:run_id>/<path:name>", views.asset), path("catalog/<uuid:template_id>.png", views.catalog_image),
    path("ui/<str:name>", views.static_file),
]
