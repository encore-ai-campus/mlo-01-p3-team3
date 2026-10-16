"""Member-owned saved versions and wishlists. No provider writes user history."""
import copy
import hashlib
import re
from functools import wraps
from uuid import UUID
from django.contrib.auth.decorators import login_required
from django.core.files.base import ContentFile
from django.db import transaction
from django.db.models import Max
from django.http import FileResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from .access import owned_result
from .models import Project, Composition, SavedProject, SavedProjectVersion, WishlistEntry, SketchEvent
from .platform_views import body, owned_item


def member_api(view):
    @wraps(view)
    def wrapped(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return JsonResponse({'error':'로그인이 필요합니다.'},status=401)
        return view(request,*args,**kwargs)
    return wrapped


def version_dto(version):
    return {'project_id':str(version.saved_project_id), 'version_id':str(version.id),
        'version_number':version.version_number, 'image_url':f'/api/saved-versions/{version.id}/image/',
        'page_url':f'/my/projects/{version.saved_project_id}/'}


@require_POST
@member_api
def save_result(request, result_id):
    owned = owned_result(request, result_id)
    version = None
    try:
        data = body(request)
        identifier = UUID(data.get('request_id'))
        expected = data.get('result_version')
        title = data.get('title','나의 공간')
        if type(expected) is not int or expected < 0 or not isinstance(title,str) or not 1 <= len(title.strip()) <= 100:
            raise ValueError('프로젝트 이름과 결과 버전을 확인해 주세요.')
        with transaction.atomic():
            project = Project.objects.select_for_update().get(pk=owned.project_id)
            result = Composition.objects.select_for_update().get(pk=owned.pk)
            replay = SavedProjectVersion.objects.filter(request_id=identifier).first()
            if replay:
                if replay.composition_id != result.id or replay.source_result_version != expected:
                    return JsonResponse({'error':'저장 요청 내용이 변경됐습니다.'},status=409)
                return JsonResponse(version_dto(replay))
            if result.version != expected:
                return JsonResponse({'error':'가구 배치가 변경됐습니다. 현재 결과를 확인하고 다시 저장해 주세요.'},status=409)
            same = SavedProjectVersion.objects.filter(composition=result, source_result_version=expected).first()
            if same:
                return JsonResponse(version_dto(same))
            library = project.saved_project
            if library is None:
                library = SavedProject.objects.create(user=request.user,title=title.strip())
                project.saved_project = library
                project.save(update_fields=['saved_project'])
            # The lineage is server-owned, never a client-supplied library ID.
            if library.user_id != request.user.pk:
                raise ValueError('프로젝트 소유자를 확인해 주세요.')
            SavedProject.objects.select_for_update().get(pk=library.pk)
            number = (library.versions.aggregate(n=Max('version_number'))['n'] or 0) + 1
            snapshot = {'document':copy.deepcopy(result.input_document), 'request_context':copy.deepcopy(result.generation_context),
                'placements':copy.deepcopy(result.placements), 'initial_placements':copy.deepcopy(result.initial_placements),
                'photo':project.photo.name, 'original_photo':project.original_photo.name,
                'original_checksum':project.original_checksum, 'width':project.width, 'height':project.height,
                'writer_id':project.owner, 'result_version':result.version}
            version = SavedProjectVersion(saved_project=library,version_number=number,composition=result,
                source_result_version=result.version,request_id=identifier,snapshot=snapshot)
            with result.image.open('rb') as source:
                version.image.save(f'{version.id}.png',ContentFile(source.read()),save=False)
            version.save()
            library.updated_at = timezone.now()
            library.save(update_fields=['updated_at'])
            SketchEvent.objects.create(project=project,event_type='project.saved',payload={'saved_project_id':str(library.id),'version_number':number})
        return JsonResponse(version_dto(version),status=201)
    except (ValueError,TypeError,AttributeError) as error:
        if version and version.image: version.image.delete(save=False)
        return JsonResponse({'error':str(error)},status=400)
    except Exception:
        if version and version.image: version.image.delete(save=False)
        raise


def own_version(request, version_id):
    return get_object_or_404(SavedProjectVersion.objects.select_related('saved_project','composition__project'),pk=version_id,saved_project__user=request.user)


@require_GET
@member_api
def saved_version_image(request, version_id):
    version = own_version(request,version_id)
    try:
        return FileResponse(version.image.open('rb'),content_type='image/png')
    except FileNotFoundError:
        return JsonResponse({'error':'저장 이미지를 찾을 수 없습니다.'},status=404)


@require_POST
@member_api
def edit_saved_version(request, version_id):
    from .views import serialize
    version = own_version(request,version_id)
    snapshot = version.snapshot
    with transaction.atomic():
        project = Project.objects.create(owner=version.composition.project.owner, account=request.user, saved_project=version.saved_project,
            title=version.saved_project.title, photo=snapshot['photo'], original_photo=snapshot.get('original_photo',''),
            original_checksum=snapshot.get('original_checksum',''), width=snapshot['width'], height=snapshot['height'],
            document=copy.deepcopy(snapshot['document']), request_context=copy.deepcopy(snapshot['request_context']))
        SketchEvent.objects.create(project=project,event_type='project.restored',payload={'saved_version_id':str(version.id)})
    return JsonResponse(serialize(project),status=201)


@login_required
@require_GET
def saved_projects(request):
    rows = SavedProject.objects.filter(user=request.user).prefetch_related('versions').order_by('-updated_at')
    versions = SavedProjectVersion.objects.filter(saved_project__user=request.user).select_related('saved_project').order_by('-created_at')
    return render(request,'sketch/saved_projects.html',{'projects':rows,'versions':versions,'my_tab':'projects'})


@login_required
@require_GET
def saved_project_detail(request, project_id):
    project=get_object_or_404(SavedProject,pk=project_id,user=request.user)
    return render(request,'sketch/saved_project_detail.html',{'saved_project':project,'versions':project.versions.order_by('-version_number'),'my_tab':'projects'})


@login_required
@require_GET
def saved_compare(request):
    try:
        left = own_version(request,UUID(request.GET.get('left')))
        right = own_version(request,UUID(request.GET.get('right')))
    except (ValueError,TypeError,AttributeError):
        return JsonResponse({'error':'비교할 저장 버전 두 개를 선택해 주세요.'},status=400)
    SketchEvent.objects.create(project=left.composition.project,event_type='project.compared',payload={'left':str(left.pk),'right':str(right.pk)})
    return render(request,'sketch/saved_compare.html',{'left':left,'right':right,'my_tab':'projects'})


def catalog_item_key(item, category_id=''):
    if item.get('product_id'):
        return 'product:' + item['product_id']
    if item.get('catalog_key'):
        return 'sample:' + item['catalog_key']
    # Legacy mock runs did not include a key. Only a verified sample route can
    # produce the category/variant fallback; unrelated items retain unique keys.
    match=re.fullmatch(r'/api/results/[0-9a-f-]+/samples/[A-Za-z0-9-]+/(cream|wood|sage)/\?category=([a-z_]+)',item.get('image_url',''))
    if match:
        return f'sample:{match[2]}:{match[1]}'
    return 'legacy:' + hashlib.sha256((item['id']+item.get('image_url','')).encode()).hexdigest()


def wishlist_dto(entry):
    return {'id':str(entry.id), 'item':entry.item_snapshot, 'stable_item_key':entry.stable_item_key}


@require_http_methods(['GET','POST'])
@member_api
def wishlist_list_add(request):
    if request.method=='GET':
        return JsonResponse({'items':[wishlist_dto(row) for row in WishlistEntry.objects.filter(user=request.user).order_by('-created_at')]})
    try:
        data=body(request)
        run,item=owned_item(request,data,UUID(data.get('item_id')))
        with transaction.atomic():
            entry,created=WishlistEntry.objects.get_or_create(user=request.user,stable_item_key=catalog_item_key(item),
                defaults={'item_snapshot':copy.deepcopy(item),'recommendation_run':run,'item_id':item['id']})
            if created:
                SketchEvent.objects.create(project=run.composition.project,event_type='wishlist.saved',payload={'wishlist_id':str(entry.id),'recommendation_run_id':str(run.id),'item_id':item['id']})
        return JsonResponse(wishlist_dto(entry),status=201 if created else 200)
    except (ValueError,TypeError,AttributeError) as error:
        return JsonResponse({'error':str(error)},status=400)


@require_POST
@member_api
def wishlist_remove(request, entry_id):
    entry=get_object_or_404(WishlistEntry.objects.select_related('recommendation_run__composition__project'),pk=entry_id,user=request.user)
    with transaction.atomic():
        SketchEvent.objects.create(project=entry.recommendation_run.composition.project,event_type='wishlist.removed',payload={'wishlist_id':str(entry.id)})
        entry.delete()
    return JsonResponse({'removed':True})


@login_required
@require_GET
def wishlist_page(request):
    return render(request,'sketch/wishlist.html',{'entries':WishlistEntry.objects.filter(user=request.user).order_by('-created_at'),'my_tab':'wishlist'})
