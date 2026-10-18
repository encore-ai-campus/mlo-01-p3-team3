"""NiFi REST deployment plan; never start a processor implicitly.

ExecuteProcess runs one bounded queue tick. Python owns retry state and fences;
NiFi owns the timer. Restricted processor permission must be granted separately.
"""
import json
import re
import ssl
from urllib.request import Request, urlopen
from uuid import UUID

from ..errors import Conflict, PipelineError

TYPE='org.apache.nifi.processors.standard.ExecuteProcess'


def processor_plan(python, config, working_directory, *, period='30 sec'):
    # NiFi splits arguments on ';'. Reject delimiters/EL instead of shell escaping.
    for value in (python,config,working_directory):
        if not isinstance(value,str) or not value.startswith('/') or any(c in value for c in ';\r\n${}'):
            raise PipelineError('NIFI_UNSAFE_DEPLOYMENT_PATH')
    if not re.fullmatch(r'[1-9][0-9]* (sec|min)',period):raise PipelineError('NIFI_PERIOD_INVALID')
    return dict(name='product-pipeline-durable-tick',type=TYPE,
        position=dict(x=0.0,y=0.0),config=dict(schedulingStrategy='TIMER_DRIVEN',schedulingPeriod=period,
        concurrentlySchedulableTaskCount=1,executionNode='PRIMARY',autoTerminatedRelationships=['success'],
        properties={'Command':python,'Command Arguments':'-m;product_pipeline.operations.cli;tick;--config;'+config,
                    'Working Directory':working_directory,'Argument Delimiter':';','Redirect Error Stream':'false'}))


class Client:
    def __init__(self, base, *, ca_file=None, token=None, request=None):
        if not base.startswith('https://') or '@' in base:raise PipelineError('NIFI_TLS_REQUIRED')
        self.base=base.rstrip('/');self.token=token
        self.context=ssl.create_default_context(cafile=ca_file)
        self.request=request or self._request

    def _request(self, method, path, body=None):
        headers={'Accept':'application/json'}
        if self.token:headers['Authorization']='Bearer '+self.token
        data=None if body is None else json.dumps(body).encode()
        if data is not None:headers['Content-Type']='application/json'
        with urlopen(Request(self.base+path,data=data,headers=headers,method=method),
                     context=self.context,timeout=20) as response:
            return json.load(response)

    def prepare(self, group, spec, *, expected_version, apply=False):
        try:UUID(group)
        except ValueError as exc:raise PipelineError('NIFI_GROUP_UUID_REQUIRED') from exc
        about=self.request('GET','/controller/about')['about']
        if not expected_version or about['version']!=expected_version:raise Conflict('NIFI_VERSION_MISMATCH')
        kinds=self.request('GET','/flow/processor-types?type='+TYPE)['processorTypes']
        candidates=[v for v in kinds if v['type']==TYPE and v['bundle']['version']==expected_version]
        if len(candidates)!=1:raise PipelineError('NIFI_PROCESSOR_BUNDLE_NOT_AVAILABLE')
        existing=self.request('GET','/process-groups/'+group+'/processors').get('processors',[])
        matches=[p for p in existing if p['component']['name']==spec['name']]
        component={**spec,'bundle':candidates[0]['bundle']}
        if matches:
            if len(matches)!=1:raise Conflict('NIFI_DUPLICATE_PROCESSOR')
            old=matches[0]['component']
            for key in ('type','bundle'):
                if old.get(key)!=component[key]:raise Conflict('NIFI_EXISTING_FLOW_DIFFERS')
            for key,value in spec['config'].items():
                actual=old['config'].get(key)
                if key=='properties':
                    if any((actual or {}).get(k)!=v for k,v in value.items()):raise Conflict('NIFI_EXISTING_FLOW_DIFFERS')
                elif actual!=value:raise Conflict('NIFI_EXISTING_FLOW_DIFFERS')
            return dict(status='EXISTING_MATCH',processor_id=old['id'],automatic_start=False)
        body=dict(revision=dict(version=0),component=component)
        if not apply:return dict(status='PLAN_ONLY',path='/process-groups/'+group+'/processors',body=body,automatic_start=False)
        created=self.request('POST','/process-groups/'+group+'/processors',body)
        return dict(status='CREATED_STOPPED',processor_id=created['id'],automatic_start=False)

    def set_running(self,processor_id,spec,*,expected_version,running):
        try:UUID(processor_id)
        except ValueError as exc:raise PipelineError('NIFI_PROCESSOR_UUID_REQUIRED') from exc
        if self.request('GET','/controller/about')['about']['version']!=expected_version:raise Conflict('NIFI_VERSION_MISMATCH')
        entity=self.request('GET','/processors/'+processor_id);component=entity['component']
        if component['type']!=spec['type'] or component['name']!=spec['name']:raise Conflict('NIFI_PROCESSOR_IDENTITY_CHANGED')
        for key,value in spec['config'].items():
            old=component['config'].get(key)
            if key=='properties':
                if any((old or {}).get(k)!=v for k,v in value.items()):raise Conflict('NIFI_EXISTING_FLOW_DIFFERS')
            elif old!=value:raise Conflict('NIFI_EXISTING_FLOW_DIFFERS')
        if running and component.get('validationStatus')!='VALID':raise PipelineError('NIFI_FLOW_NOT_VALIDATED')
        self.request('PUT','/processors/'+processor_id+'/run-status',dict(revision=entity['revision'],
                     state='RUNNING' if running else 'STOPPED',disconnectedNodeAcknowledged=False))
        return dict(status='RUNNING_REQUESTED' if running else 'STOP_REQUESTED',processor_id=processor_id)
