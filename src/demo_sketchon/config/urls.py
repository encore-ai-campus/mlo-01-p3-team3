from django.urls import include, path

urlpatterns = [
    path("api/ads/", include("operations.ad_urls")),
    path("ops/", include("operations.urls")),
    path("interior-recommand/", include("recommendations.urls")),
    path("", include("sketch.urls")),
]
