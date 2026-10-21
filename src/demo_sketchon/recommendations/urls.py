from django.urls import path
from . import views

app_name = "recommendations"
urlpatterns = [
    path("", views.index, name="index"),
    path("api/csrf", views.csrf, name="csrf"),
    path("ui/product-preview.mjs", views.preview_client, name="preview-client"),
    path("api/runs", views.submit, name="submit"),
    path("api/runs/<uuid:run_id>", views.state, name="state"),
    path("api/runs/<uuid:run_id>/previews", views.submit_preview, name="preview"),
    path("api/runs/<uuid:run_id>/actions/prepare", views.retry, name="retry"),
    path("api/runs/<uuid:run_id>/product-link", views.product_link, name="product-link"),
    path("assets/<uuid:run_id>/<path:name>", views.asset, name="asset"),
]
