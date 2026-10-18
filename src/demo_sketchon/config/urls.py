from django.urls import include, path

urlpatterns = [path("ops/", include("operations.urls")), path("", include("sketch.urls"))]
