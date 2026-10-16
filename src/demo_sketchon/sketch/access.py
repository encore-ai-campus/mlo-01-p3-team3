from uuid import uuid4
from django.db.models import Q
from django.shortcuts import get_object_or_404
from .models import Project, Composition


def visitor_id(request):
    if 'sketch_owner' not in request.session:
        request.session['sketch_owner'] = uuid4().hex
    return request.session['sketch_owner']


def project_scope(request, prefix=''):
    if request.user.is_authenticated:
        return Q(**{prefix + 'account_id': request.user.pk})
    return Q(**{prefix + 'account__isnull': True, prefix + 'owner': visitor_id(request)})


def owned_project(request, identifier):
    return get_object_or_404(Project.objects.filter(project_scope(request)), pk=identifier)


def owned_result(request, identifier):
    return get_object_or_404(Composition.objects.select_related('project').filter(project_scope(request, 'project__')), pk=identifier)


def claim_guest_projects(request, user):
    return Project.objects.filter(account__isnull=True, owner=visitor_id(request)).update(account=user)


def account_context(request):
    return {'signup_completed':request.user.is_authenticated and request.session.pop('signup_completed', False),
        'account_initial': {'authenticated':request.user.is_authenticated,
        'actor_key':f'user:{request.user.pk}' if request.user.is_authenticated else 'guest:' + visitor_id(request),
        'nickname':(request.user.first_name or request.user.username) if request.user.is_authenticated else ''}}


class PrivateResponseMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        if not request.path.startswith('/static/'):
            response['Cache-Control'] = 'private, no-store'
        return response
