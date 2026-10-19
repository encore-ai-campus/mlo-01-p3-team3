from django.contrib.admin import AdminSite
from .admin_ui import MODEL_TITLES


class AdAdminSite(AdminSite):
    site_header='SketchOn 광고 운영'
    site_title='광고 운영 관리'
    index_title='광고 등록과 집행을 순서대로 진행하세요'
    index_template='admin/ad_index.html'

    def get_app_list(self,request,app_label=None):
        apps=super().get_app_list(request,app_label)
        order=list(MODEL_TITLES)
        for app in apps:
            if app['app_label']=='advertising':
                app['name']='광고 운영'
                for model in app['models']:model['name']=MODEL_TITLES.get(model['object_name'],model['name'])
                app['models'].sort(key=lambda model:order.index(model['object_name']) if model['object_name'] in order else 999)
        apps.sort(key=lambda app:app['app_label']!='advertising')
        return apps
