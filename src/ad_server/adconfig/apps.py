from django.contrib.admin.apps import AdminConfig

class AdAdminConfig(AdminConfig):
    default_site='advertising.admin_site.AdAdminSite'
