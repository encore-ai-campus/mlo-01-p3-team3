from django.urls import path
from . import views, platform_views, accounts, library

# UI integration seam: the scene-generation route currently wraps the legacy mock.
# Replace its handler with the Application job submission/lookup implementation
# after durable input-revision storage and Bronze acknowledgement are connected.
# Keep provider credentials, Ad transport and FAISS access out of browser code.
urlpatterns = [
    path('api/results/<uuid:result_id>/save/', library.save_result, name='save-result'),
    path('api/saved-versions/<uuid:version_id>/image/', library.saved_version_image, name='saved-version-image'),
    path('api/saved-versions/<uuid:version_id>/edit/', library.edit_saved_version, name='edit-saved-version'),
    path('api/wishlist/', library.wishlist_list_add, name='wishlist-api'),
    path('api/wishlist/<uuid:entry_id>/remove/', library.wishlist_remove, name='wishlist-remove'),
    path('my/projects/', library.saved_projects, name='saved-projects'),
    path('my/projects/<uuid:project_id>/', library.saved_project_detail, name='saved-project-detail'),
    path('my/compare/', library.saved_compare, name='saved-compare'),
    path('my/wishlist/', library.wishlist_page, name='wishlist'),
    path('accounts/login/', accounts.login_page, name='login'),
    path('accounts/signup/', accounts.signup_page, name='signup'),
    path('accounts/logout/', accounts.logout_action, name='logout'),
    path('api/account/status/', accounts.account_status, name='account-status'),
    path('my/profile/', accounts.profile_page, name='profile'),
    path("api/scene-objects/<uuid:object_id>/revisions/", platform_views.revise_object, name="object-revise"),
    path("api/results/<uuid:result_id>/samples/<str:placement_id>/<str:variant>/", views.result_sample, name="result-sample"),
    path("api/projects/<uuid:project_id>/scene-generations/", views.search, name="scene-generate"),
    path("api/scene-generations/<uuid:scene_id>/", platform_views.scene, name="scene-detail"),
    path("api/scene-objects/<uuid:object_id>/recommendations/", platform_views.recommend, name="object-recommend"),
    path("api/jobs/<uuid:job_id>/", platform_views.job, name="job-detail"),
    path("api/recommendation-items/<uuid:item_id>/events/", platform_views.item_event, name="recommendation-event"),
    path("api/recommendation-items/<uuid:item_id>/link/", platform_views.link, name="recommendation-link"),
    path("", views.home, name="home"),
    path("editor/", views.editor, name="editor"),
    path("editor/<uuid:project_id>/", views.editor, name="project-editor"),
    path("results/<uuid:result_id>/", views.result_page, name="result-page"),
    path("api/projects/", views.projects, name="projects"),
    path("api/projects/<uuid:project_id>/", views.project_detail, name="project"),
    path("api/projects/<uuid:project_id>/photo/", views.photo, name="photo"),
    path("api/projects/<uuid:project_id>/events/", views.collect_events, name="events"),
    path("api/projects/<uuid:project_id>/samples/", views.collect_sample, name="samples"),
    path("api/projects/<uuid:project_id>/search/", views.search, name="search"),
    path("api/projects/<uuid:project_id>/crop/", views.crop_photo, name="crop"),
    path("api/results/<uuid:result_id>/image/", views.result_image, name="result-image"),
    path("api/results/<uuid:result_id>/data/", views.result_data, name="result-data"),
    path("api/results/<uuid:result_id>/sprites/<str:placement_id>/", views.result_sprite, name="result-sprite"),
    path("api/results/<uuid:result_id>/placements/", views.update_placements, name="result-placements"),
    path("api/results/<uuid:result_id>/redraw/", views.redraw, name="redraw"),
]
