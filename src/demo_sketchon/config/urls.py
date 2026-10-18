from django.urls import include, path

urlpatterns = [
    path("ops/", include("operations.urls")),
    path("interior-recommand/", include("recommendations.urls")),
    path("", include("sketch.urls")),
]
