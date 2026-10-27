import copy
import json
from collections import Counter

import pytest
from pydantic import ValidationError

from interior_recommender.assets import read_json
from interior_recommender.scale import api_scale_bounds, assess_scale
from interior_recommender.schemas import ScalePlan, SceneComparison
from interior_recommender.services import scale_plan_prompt
from interior_recommender.tests.test_reference_preservation import selected_run
from interior_recommender.tests.test_web_workflow import web, post, wait
from interior_recommender.web.workflow import DemoAdapter
from interior_recommender.config import Settings


def fixture(kind):
    return json.loads(DemoAdapter(Settings()).call(kind, "", [])['payload']['text'])


@pytest.mark.parametrize('small,large,expected', [
    (.08, .15, 'TOO_SMALL'), (.2, .35, 'APPROPRIATE'),
    (.5, .7, 'TOO_LARGE'), (.08, .7, 'OUT_OF_RANGE'),
    (.15, .3, 'APPROPRIATE'), (.3, .5, 'APPROPRIATE')])
def test_both_size_limits_override_model_pass(small, large, expected):
    plan = fixture('scale')
    result = fixture('compare')
    result['objects'][0]['box'] = dict(x_min=0, y_min=0, x_max=small, y_max=large)
    bounds = {key: plan[key] for key in ('width_ratio', 'height_ratio')}
    check = assess_scale(result, plan, bounds, 'CHAIR')
    assert check['status'] == expected
    assert result['scale_check']['status'] == 'APPROPRIATE'  # raw response stays immutable


@pytest.mark.parametrize('geometry', ['CHANGED', 'UNCERTAIN'])
def test_background_changes_cannot_validate_scale(geometry):
    plan, result = fixture('scale'), fixture('compare')
    result['scale_check']['room_geometry'] = geometry
    assert assess_scale(result, plan, plan, 'CHAIR')['status'] == 'UNCERTAIN'


def test_weak_evidence_and_ambiguous_target_cannot_pass():
    plan, result = fixture('scale'), fixture('compare')
    for objects in ([], result['objects'] * 2):
        other = {**result, 'objects': objects}
        assert assess_scale(other, plan, plan, 'CHAIR')['status'] == 'UNCERTAIN'
    plan['confidence'] = 'LOW'
    assert assess_scale(result, plan, plan, 'CHAIR')['status'] == 'UNCERTAIN'
    assert SceneComparison.model_validate({'objects': []}).scale_check.status == 'UNCERTAIN'


def test_numeric_bounds_do_not_override_model_perspective_concern():
    plan, result = fixture('scale'), fixture('compare')
    result['scale_check']['status'] = 'TOO_SMALL'
    assert assess_scale(result, plan, plan, 'CHAIR')['status'] == 'TOO_SMALL'


def test_bounds_project_through_portrait_and_landscape_padding():
    plan = fixture('scale')
    original = copy.deepcopy(plan)
    for original_size, scale, expected_width, expected_height in (
        ([1000, 500], [.48, .48], .15, .15),
        ([500, 1000], [.48, .48], .075, .3),
    ):
        bounds = api_scale_bounds(plan, dict(original_size=original_size, scale=scale, size=[480, 480]))
        assert bounds['width_ratio']['minimum'] == pytest.approx(expected_width)
        assert bounds['height_ratio']['minimum'] == pytest.approx(expected_height)
    assert plan == original


@pytest.mark.parametrize('change', [
    {'width_ratio': {'minimum': .5, 'maximum': .2}},
    {'height_ratio': {'minimum': .1, 'maximum': 1.1}},
    {'anchors': []}, {'basis': 'CATEGORY_ONLY'},
    {'basis': 'EXPLICIT', 'explicit_size_intent': None},
    {'width_ratio': {'minimum': float('nan'), 'maximum': .5}},
])
def test_invalid_or_unsupported_ranges_rejected(change):
    with pytest.raises(ValidationError):
        ScalePlan.model_validate({**fixture('scale'), **change})


@pytest.mark.parametrize('verdict', ['TOO_SMALL', 'TOO_LARGE', 'OUT_OF_RANGE', 'UNCERTAIN', None])
def test_scale_review_preserves_result_and_compare_retry_never_generates(web, verdict):
    client, service = web
    calls = Counter()
    value = [verdict]

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            calls[kind] += 1
            response = super().call(kind, *args, **kwargs)
            if kind == 'compare':
                data = json.loads(response['payload']['text'])
                if value[0] is None:
                    data.pop('scale_check')
                else:
                    data['scale_check']['status'] = value[0]
                response['payload']['text'] = json.dumps(data)
            return response

    service.factory = Observed
    child, owner, prepared = selected_run(client, service)
    preview = prepared['preview']
    assert preview['api_scale_bounds']['height_ratio']['minimum'] == pytest.approx(.24)
    assert 'lower AND upper' in preview['prompt']
    assert calls['scale'] == 1
    post(client, f'/api/runs/{child}/actions/generate', {}, json_body=True)
    state = wait(service, child, owner)
    assert state['status'] == 'NEEDS_SCALE_REVIEW'
    assert state['result_image'] == 'generated.png'
    assert not state['comparison']['objects'][0]['eligible_for_selection']
    value[0] = 'APPROPRIATE'
    post(client, f'/api/runs/{child}/actions/compare', {'retry': True}, json_body=True)
    assert wait(service, child, owner)['status'] == 'COMPLETED'
    assert calls['generate'] == calls['scale'] == 1
    assert calls['compare'] == 2
    directory = service.directory(child, owner)
    assert read_json(directory / 'scale.json') == preview['scale_plan']


def test_scale_stage_uses_original_room_and_selected_reference_and_recovers(web):
    client, service = web
    fail = [True]
    calls = Counter()
    captured = []

    class Observed(DemoAdapter):
        def call(self, kind, prompt, images, schema=None):
            calls[kind] += 1
            if kind == 'scale':
                captured.append(images)
                if fail[0]:
                    return {'payload': {'text': '{}'}}
            return super().call(kind, prompt, images, schema)

    service.factory = Observed
    from interior_recommender.tests.test_web_workflow import submit
    run = submit(client).json()['run_id']
    owner = client.session['mvp_owner']
    wait(service, run, owner)
    child = post(client, f'/api/runs/{run}/selection', {'template_id': 'chair-demo'}, json_body=True).json()['run_id']
    for future in service.futures:
        future.result(timeout=5)
    directory = service.directory(child, owner)
    assert service.state(child, owner)['job']['status'] == 'FAILED'
    assert not (directory / 'generation_request.json').exists()
    assert captured[0][0] == (directory / 'inputs/room_image.png').read_bytes()
    fail[0] = False
    post(client, f'/api/runs/{child}/actions/prepare', {'retry': True}, json_body=True)
    assert wait(service, child, owner)['status'] == 'READY_TO_GENERATE'
    assert calls['reference'] == 1 and calls['scale'] == 2 and calls['generate'] == 0


def test_scale_provider_schema_and_explicit_unusual_size_intent():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from interior_recommender.adapters.gemini import GeminiAdapter
    client = Mock()
    client.models.generate_content.return_value = SimpleNamespace(text='{}', usage_metadata=None, response_id=None)
    GeminiAdapter(Settings(), client).call('scale', 'inspect', [], ScalePlan.model_json_schema())
    schema = client.models.generate_content.call_args.kwargs['config'].response_json_schema
    assert '$defs' not in schema
    assert 'typical_size_mm' in schema['properties']['anchors']['items']['properties']
    prompt = scale_plan_prompt({'text': '미니어처 의자'}, {}, {})
    assert 'miniature or oversized' in prompt and '미니어처 의자' in prompt


@pytest.mark.parametrize('mode,expected', [('small', 'TOO_SMALL'), ('large', 'TOO_LARGE'), ('weak', 'UNCERTAIN')])
def test_numeric_or_weak_plan_gate_model_pass_end_to_end(web, mode, expected):
    client, service = web

    class Observed(DemoAdapter):
        def call(self, kind, *args, **kwargs):
            response = super().call(kind, *args, **kwargs)
            if kind == 'scale' and mode == 'weak':
                data = json.loads(response['payload']['text'])
                data.update(basis='CATEGORY_ONLY', confidence='LOW', anchors=[], width_ratio=None, height_ratio=None)
                response['payload']['text'] = json.dumps(data)
            if kind == 'compare' and mode in {'small', 'large'}:
                data = json.loads(response['payload']['text'])
                extent = .05 if mode == 'small' else .8
                data['objects'][0]['box'] = dict(x_min=0, y_min=0, x_max=extent, y_max=extent)
                response['payload']['text'] = json.dumps(data)
            return response

    service.factory = Observed
    child, owner, _ = selected_run(client, service)
    post(client, f'/api/runs/{child}/actions/generate', {}, json_body=True)
    state = wait(service, child, owner)
    assert state['status'] == 'NEEDS_SCALE_REVIEW'
    assert state['comparison']['scale_check']['status'] == expected
    assert not state['comparison']['objects'][0]['eligible_for_selection']


def test_comparison_rejects_modified_frozen_scale_plan(web):
    from interior_recommender.assets import write_json
    client, service = web
    child, owner, _ = selected_run(client, service)
    post(client, f'/api/runs/{child}/actions/generate', {}, json_body=True)
    wait(service, child, owner)
    path = service.directory(child, owner) / 'generation_request.json'
    value = read_json(path)
    value['api_scale_bounds']['height_ratio']['maximum'] = .99
    write_json(path, value)
    post(client, f'/api/runs/{child}/actions/compare', {'retry': True}, json_body=True)
    for future in service.futures:
        future.result(timeout=5)
    state = service.state(child, owner)
    assert state['job']['status'] == 'FAILED'
    assert 'checksum' in state['job']['error']
    assert state['result_image'] == 'generated.png'
