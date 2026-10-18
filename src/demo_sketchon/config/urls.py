from django.urls import include, path

urlpatterns = [
    path("interior-recommand/", include("recommendations.urls")),
    path("", include("sketch.urls")),
]
