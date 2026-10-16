from django import forms
from django.contrib.auth import get_user_model, login, logout, update_session_auth_hash
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm, PasswordChangeForm
from django.http import JsonResponse
from django.middleware.csrf import get_token
from django.shortcuts import redirect, render
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from .access import visitor_id, claim_guest_projects, project_scope
from .models import Project, SketchEvent


class SignupForm(UserCreationForm):
    nickname = forms.CharField(label='닉네임', max_length=30, strip=True)
    class Meta(UserCreationForm.Meta):
        model = get_user_model()
        fields = ('username', 'nickname')
        labels = {'username': '아이디'}

    def save(self, commit=True):
        user = super().save(commit=False)
        user.first_name = self.cleaned_data['nickname']
        if commit:
            user.save()
        return user


class NicknameForm(forms.Form):
    nickname = forms.CharField(label='닉네임', max_length=30, strip=True)


def safe_next(request):
    target = request.POST.get('next') or request.GET.get('next') or '/'
    return target if url_has_allowed_host_and_scheme(target, {request.get_host()}, require_https=request.is_secure()) else '/'


def auth_event(request, event_type):
    project = Project.objects.filter(project_scope(request)).order_by('-updated_at').first()
    if project:
        SketchEvent.objects.create(project=project, event_type=event_type, payload={})


@require_http_methods(['GET','POST'])
def signup_page(request):
    if request.user.is_authenticated:
        return redirect(safe_next(request))
    form = SignupForm(request.POST or None)
    if request.method == 'POST' and form.is_valid():
        user = form.save()
        visitor_id(request)
        claim_guest_projects(request, user)
        login(request, user)
        auth_event(request, 'auth.login')
        request.session['signup_completed'] = True
        return redirect(safe_next(request))
    return render(request, 'sketch/signup.html', {'form':form, 'next':safe_next(request)})


@require_http_methods(['GET','POST'])
def login_page(request):
    if request.user.is_authenticated:
        return redirect(safe_next(request))
    form = AuthenticationForm(request, data=request.POST or None)
    form.fields['username'].label = '아이디'
    if request.method == 'POST' and form.is_valid():
        user = form.get_user()
        visitor_id(request)
        claim_guest_projects(request, user)
        login(request, user)
        auth_event(request, 'auth.login')
        return redirect(safe_next(request))
    return render(request, 'sketch/login.html', {'form':form, 'next':safe_next(request)})


@require_POST
def logout_action(request):
    previous = visitor_id(request)
    auth_event(request, 'auth.logout')
    logout(request)
    request.session['sketch_owner'] = previous
    return redirect('/')


@require_GET
def account_status(request):
    from .quota import quota_status
    identifier = request.GET.get('project_id')
    access = None
    if identifier:
        try:
            access = Project.objects.filter(project_scope(request), pk=identifier).exists()
        except (ValueError, forms.ValidationError):
            access = False
    return JsonResponse({'authenticated':request.user.is_authenticated,
        'actor_key':f'user:{request.user.pk}' if request.user.is_authenticated else 'guest:' + visitor_id(request),
        'nickname':request.user.first_name or request.user.username if request.user.is_authenticated else '',
        'csrf_token':get_token(request), 'quota':quota_status(request), 'project_access':access})


@login_required
@require_http_methods(['GET','POST'])
def profile_page(request):
    nickname_form = NicknameForm(initial={'nickname':request.user.first_name})
    password_form = PasswordChangeForm(request.user)
    notice = ''
    if request.method == 'POST':
        if request.POST.get('action') == 'nickname':
            nickname_form = NicknameForm(request.POST)
            if nickname_form.is_valid():
                request.user.first_name = nickname_form.cleaned_data['nickname']
                request.user.save(update_fields=['first_name'])
                notice = '닉네임을 변경했어요.'
        elif request.POST.get('action') == 'password':
            password_form = PasswordChangeForm(request.user, request.POST)
            if password_form.is_valid():
                update_session_auth_hash(request, password_form.save())
                notice = '비밀번호를 변경했어요.'
                password_form = PasswordChangeForm(request.user)
    return render(request, 'sketch/profile.html', {'nickname_form':nickname_form, 'password_form':password_form, 'notice':notice, 'my_tab':'profile'})
