import json
import logging
import asyncio
import aiohttp
import secrets
import base64
import traceback
import pymysql
from hailtop.utils import time_msecs, sleep_and_backoff, is_transient_error

from .globals import complete_states, tasks
from .database import check_call_procedure
from .batch_configuration import KUBERNETES_TIMEOUT_IN_SECONDS, \
    KUBERNETES_SERVER_URL
from .utils import cost_from_msec_mcpu

log = logging.getLogger('batch')


async def retry_deadlock(f):
    delay = 0.1
    while True:
        try:
            return await f()
        except pymysql.err.OperationalError as exc:
            if exc.args[0] == 1213:
                log.warn(f'retrying deadlock: {exc}', exc_info=True)
            else:
                raise
        delay = await sleep_and_backoff(delay)


def batch_record_to_dict(record):
    if not record['closed']:
        state = 'open'
    elif record['n_failed'] > 0:
        state = 'failure'
    elif record['n_cancelled'] > 0:
        state = 'cancelled'
    elif record['closed'] and record['n_succeeded'] == record['n_jobs']:
        state = 'success'
    else:
        state = 'running'

    complete = record['closed'] and record['n_completed'] == record['n_jobs']

    d = {
        'id': record['id'],
        'billing_project': record['billing_project'],
        'state': state,
        'complete': complete,
        'closed': record['closed'],
        'n_jobs': record['n_jobs'],
        'n_completed': record['n_completed'],
        'n_succeeded': record['n_succeeded'],
        'n_failed': record['n_failed'],
        'n_cancelled': record['n_cancelled']
    }

    attributes = json.loads(record['attributes'])
    if attributes:
        d['attributes'] = attributes

    msec_mcpu = record['msec_mcpu']
    d['msec_mcpu'] = msec_mcpu

    cost = cost_from_msec_mcpu(msec_mcpu)
    d['cost'] = f'${cost:.4f}'

    return d


async def notify_batch_job_complete(db, batch_id):
    record = await db.select_and_fetchone(
        '''
SELECT *
FROM batches
WHERE id = %s AND NOT deleted AND callback IS NOT NULL AND
   closed AND n_completed = n_jobs;
''',
        (batch_id,))

    if not record:
        return
    callback = record['callback']

    log.info(f'making callback for batch {batch_id}: {callback}')

    try:
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
            await session.post(callback, json=batch_record_to_dict(record))
            log.info(f'callback for batch {batch_id} successful')
    except Exception:
        log.exception(f'callback for batch {batch_id} failed, will not retry.')


async def mark_job_complete(app, batch_id, job_id, attempt_id, new_state, status,
                            start_time, end_time, reason):
    scheduler_state_changed = app['scheduler_state_changed']
    db = app['db']
    inst_pool = app['inst_pool']

    id = (batch_id, job_id)

    log.info(f'marking job {id} complete new_state {new_state}')

    now = time_msecs()

    async def f():
        await check_call_procedure(
            db,
            'CALL mark_job_complete(%s, %s, %s, %s, %s, %s, %s, %s, %s);',
            (batch_id, job_id, attempt_id, new_state,
             json.dumps(status) if status is not None else None,
             start_time, end_time, reason, now))
    rv = retry_deadlock(f)

    log.info(f'mark_job_complete returned {rv} for job {id}')

    old_state = rv['old_state']
    if old_state in complete_states:
        log.info(f'old_state {old_state} complete, doing nothing')
        # already complete, do nothing
        return

    log.info(f'job {id} changed state: {rv["old_state"]} => {new_state}')

    instance_name = rv['instance_name']
    if instance_name:
        instance = inst_pool.name_instance.get(instance_name)
        if instance:
            log.info(f'updating {instance}')

            instance.adjust_free_cores_in_memory(rv['cores_mcpu'])
            scheduler_state_changed.set()
        else:
            log.warning(f'mark_complete for job {id} from unknown {instance}')

    await notify_batch_job_complete(db, batch_id)


async def mark_job_started(app, batch_id, job_id, attempt_id, start_time):
    db = app['db']

    id = (batch_id, job_id)

    log.info(f'mark job {id} started')

    async def f():
        await db.execute_update('''
UPDATE attempts
SET start_time = %s
WHERE batch_id = %s AND job_id = %s AND attempt_id = %s;
''',
                                (start_time, batch_id, job_id, attempt_id))

    await retry_deadlock(f)


def job_record_to_dict(record, running_status=None):
    spec = json.loads(record['spec'])

    attributes = spec.pop('attributes', None)

    result = {
        'batch_id': record['batch_id'],
        'job_id': record['job_id'],
        'state': record['state'],
        'spec': spec
    }

    if attributes:
        result['attributes'] = attributes

    if record['status']:
        status = json.loads(record['status'])
    else:
        status = running_status
    if status:
        result['status'] = status

    msec_mcpu = record['msec_mcpu']
    result['msec_mcpu'] = msec_mcpu

    cost = cost_from_msec_mcpu(msec_mcpu)
    result['cost'] = f'${cost:.4f}'

    return result


async def unschedule_job(app, record):
    scheduler_state_changed = app['scheduler_state_changed']
    db = app['db']
    inst_pool = app['inst_pool']

    batch_id = record['batch_id']
    job_id = record['job_id']
    id = (batch_id, job_id)

    instance_name = record['instance_name']
    assert instance_name is not None

    log.info(f'unscheduling job {id} from instance {instance_name}')

    instance = inst_pool.name_instance.get(instance_name)
    if not instance:
        log.warning(f'unschedule job {id}: unknown instance {instance_name}')
        return

    end_time = time_msecs()
    await check_call_procedure(
        db,
        'CALL unschedule_job(%s, %s, %s, %s, %s);',
        (batch_id, job_id, instance_name, end_time, 'cancelled'))

    log.info(f'unschedule job {id}: updated database')

    instance.adjust_free_cores_in_memory(record['cores_mcpu'])
    scheduler_state_changed.set()

    log.info(f'unschedule job {id}: updated {instance} free cores')

    url = (f'http://{instance.ip_address}:5000'
           f'/api/v1alpha/batches/{batch_id}/jobs/{job_id}/delete')
    delay = 0.1
    while True:
        if instance.state in ('inactive', 'deleted'):
            break
        try:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                await session.delete(url)
                await instance.mark_healthy()
                break
        except Exception as e:
            if (isinstance(e, aiohttp.ClientResponseError) and
                    e.status == 404):  # pylint: disable=no-member
                await instance.mark_healthy()
                break
            else:
                await instance.incr_failed_request_count()
                if is_transient_error(e):
                    pass
                else:
                    raise
        delay = await sleep_and_backoff(delay)

    log.info(f'unschedule job {id}: called delete job')


async def job_config(app, record, attempt_id):
    k8s_client = app['k8s_client']

    job_spec = json.loads(record['spec'])
    job_spec['attempt_id'] = attempt_id

    userdata = json.loads(record['userdata'])

    secrets = job_spec['secrets']
    k8s_secrets = await asyncio.gather(*[
        k8s_client.read_namespaced_secret(
            secret['name'], secret['namespace'],
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        for secret in secrets
    ])

    gsa_key = None
    for secret, k8s_secret in zip(secrets, k8s_secrets):
        if secret['name'] == userdata['gsa_key_secret_name']:
            gsa_key = k8s_secret.data
        secret['data'] = k8s_secret.data

    assert gsa_key

    service_account = job_spec.get('service_account')
    if service_account:
        namespace = service_account['namespace']
        name = service_account['name']

        sa = await k8s_client.read_namespaced_service_account(
            name, namespace,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        assert len(sa.secrets) == 1

        token_secret_name = sa.secrets[0].name

        secret = await k8s_client.read_namespaced_secret(
            token_secret_name, namespace,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

        token = base64.b64decode(secret.data['token']).decode()
        cert = secret.data['ca.crt']

        kube_config = f'''
apiVersion: v1
clusters:
- cluster:
    certificate-authority: /.kube/ca.crt
    server: {KUBERNETES_SERVER_URL}
  name: default-cluster
contexts:
- context:
    cluster: default-cluster
    user: {namespace}-{name}
    namespace: {namespace}
  name: default-context
current-context: default-context
kind: Config
preferences: {{}}
users:
- name: {namespace}-{name}
  user:
    token: {token}
'''

        job_spec['secrets'].append({
            'name': 'kube-config',
            'mount_path': '/.kube',
            'data': {'config': base64.b64encode(kube_config.encode()).decode(),
                     'ca.crt': cert}
        })

        env = job_spec.get('env')
        if not env:
            env = []
            job_spec['env'] = env
        env.append({'name': 'KUBECONFIG',
                    'value': '/.kube/config'})

    return {
        'batch_id': record['batch_id'],
        'user': record['user'],
        'gsa_key': gsa_key,
        'job_spec': job_spec
    }


async def schedule_job(app, record, instance):
    assert instance.state == 'active'

    db = app['db']

    batch_id = record['batch_id']
    job_id = record['job_id']
    attempt_id = ''.join([secrets.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(6)])
    id = (batch_id, job_id)

    try:
        body = await job_config(app, record, attempt_id)
    except Exception:
        log.exception('while making job config')
        status = {
            'worker': None,
            'batch_id': batch_id,
            'job_id': job_id,
            'attempt_id': attempt_id,
            'user': record['user'],
            'state': 'error',
            'error': traceback.format_exc(),
            'container_statuses': {k: {} for k in tasks}
        }
        await mark_job_complete(app, batch_id, job_id, attempt_id, 'Error', status,
                                None, None, 'error')
        return

    log.info(f'schedule job {id} on {instance}: made job config')

    try:
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
            url = (f'http://{instance.ip_address}:5000'
                   f'/api/v1alpha/batches/jobs/create')
            await session.post(url, json=body)
            await instance.mark_healthy()
    except Exception:
        await instance.incr_failed_request_count()
        raise

    log.info(f'schedule job {id} on {instance}: called create job')

    await check_call_procedure(
        db,
        'CALL schedule_job(%s, %s, %s, %s);',
        (batch_id, job_id, attempt_id, instance.name))

    log.info(f'schedule job {id} on {instance}: updated database')

    instance.adjust_free_cores_in_memory(-record['cores_mcpu'])

    log.info(f'schedule job {id} on {instance}: adjusted instance pool')
