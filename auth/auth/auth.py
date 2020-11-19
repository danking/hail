from typing import Optional
import os
import logging
import asyncio
import aiohttp
from aiohttp import web
import aiohttp_session
import uvloop
import google.auth.transport.requests
import google.oauth2.id_token
import google_auth_oauthlib.flow
from hailtop.config import get_deploy_config
from hailtop.tls import get_in_cluster_server_ssl_context
from hailtop.hail_logging import AccessLogger
from hailtop.utils import (secret_alnum_string, HailHTTPUserError,
                           handle_error_for_api)
from gear import (
    setup_aiohttp_session,
    rest_authenticated_developers_only, rest_authenticated_users_only,
    web_authenticated_developers_only, web_maybe_authenticated_user,
    web_authenticated_users_only, create_session,
    check_csrf_token, transaction, Database
)
from web_common import (
    setup_aiohttp_jinja2, setup_common_static_routes, set_message,
    render_template, handle_error_for_web
)

log = logging.getLogger('auth')

uvloop.install()

deploy_config = get_deploy_config()

routes = web.RouteTableDef()

HAIL_DEFAULT_NAMESPACE = os.environ['HAIL_DEFAULT_NAMESPACE']


def get_flow(redirect_uri, state=None):
    scopes = [
        'https://www.googleapis.com/auth/userinfo.profile',
        'https://www.googleapis.com/auth/userinfo.email',
        'openid'
    ]
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        '/auth-oauth2-client-secret/client_secret.json', scopes=scopes, state=state)
    flow.redirect_uri = redirect_uri
    return flow


async def user_from_email(db, email):
    users = [x async for x in db.select_and_fetchall(
        "SELECT * FROM users WHERE email = %s;", email)]
    if len(users) == 1:
        return users[0]
    assert len(users) == 0, users
    return None


async def user_from_username(db, username):
    users = [x async for x in db.select_and_fetchall(
        "SELECT * FROM users WHERE username = %s;", username)]
    if len(users) == 1:
        return users[0]
    assert len(users) == 0, users
    return None


def cleanup_session(session):
    def _delete(key):
        if key in session:
            del session[key]

    _delete('pending')
    _delete('email')
    _delete('state')
    _delete('next')
    _delete('caller')
    _delete('session_id')


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


@routes.get('')
@routes.get('/')
async def get_index(request):  # pylint: disable=unused-argument
    return aiohttp.web.HTTPFound(deploy_config.external_url('auth', '/login'))


@routes.get('/creating')
@web_maybe_authenticated_user
async def creating_account(request, userdata):
    db = request.app['db']
    session = await aiohttp_session.get_session(request)
    if 'pending' in session:
        email = session['email']
        user = await user_from_email(db, email)

        nb_url = deploy_config.external_url('notebook', '')
        next_page = session.pop('next', nb_url)

        cleanup_session(session)

        if user is None:
            set_message(session, f'Account does not exist for email {email}.', 'error')
            return aiohttp.web.HTTPFound(nb_url)

        page_context = {
            'username': user['username'],
            'state': user['state'],
            'email': user['email']
        }

        if user['state'] == 'deleting' or user['state'] == 'deleted':
            return await render_template('auth', request, userdata, 'account-error.html', page_context)

        if user['state'] == 'active':
            session_id = await create_session(db, user['id'])
            session['session_id'] = session_id
            set_message(session, f'Account has been created for {user["username"]}.', 'info')
            return aiohttp.web.HTTPFound(next_page)

        assert user['state'] == 'creating'
        session['pending'] = True
        session['email'] = email
        session['next'] = next_page
        return await render_template('auth', request, userdata, 'account-creating.html', page_context)

    return aiohttp.web.HTTPUnauthorized()


@routes.get('/creating/wait')
async def creating_account_wait(request):
    session = await aiohttp_session.get_session(request)
    if 'pending' not in session:
        raise web.HTTPUnauthorized()
    return await _wait_websocket(request, session['email'])


async def _wait_websocket(request, email):
    app = request.app
    db = app['db']

    user = await user_from_email(db, email)
    if not user:
        return web.HTTPNotFound()

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        count = 0
        while count < 10:
            try:
                user = await user_from_email(db, email)
                assert user
                if user['state'] != 'creating':
                    log.info(f"user {user['username']} is no longer creating")
                    break
            except Exception:  # pylint: disable=broad-except
                log.exception(f"/creating/wait: error while updating status for user {user['username']}")
            await asyncio.sleep(1)
            count += 1

        if count >= 10:
            log.info(f"user {user['username']} is still in state creating")

        ready = user['state'] == 'active'

        await ws.send_str(str(int(ready)))
        return ws
    finally:
        await ws.close()


@routes.get('/signup')
async def signup(request):
    next_page = request.query.get('next', deploy_config.external_url('notebook', ''))

    flow = get_flow(deploy_config.external_url('auth', '/oauth2callback'))

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true')

    session = await aiohttp_session.new_session(request)
    cleanup_session(session)
    session['state'] = state
    session['next'] = next_page
    session['caller'] = 'signup'

    return aiohttp.web.HTTPFound(authorization_url)


@routes.get('/login')
async def login(request):
    next_page = request.query.get('next', deploy_config.external_url('notebook', ''))

    flow = get_flow(deploy_config.external_url('auth', '/oauth2callback'))

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true')

    session = await aiohttp_session.new_session(request)

    cleanup_session(session)
    session['state'] = state
    session['next'] = next_page
    session['caller'] = 'login'

    return aiohttp.web.HTTPFound(authorization_url)


@routes.get('/oauth2callback')
async def callback(request):
    session = await aiohttp_session.get_session(request)
    if 'state' not in session:
        raise web.HTTPUnauthorized()

    nb_url = deploy_config.external_url('notebook', '')
    creating_url = deploy_config.external_url('auth', '/creating')

    state = session['state']
    caller = session['caller']
    next_page = session.pop('next', nb_url)
    cleanup_session(session)

    flow = get_flow(deploy_config.external_url('auth', '/oauth2callback'), state=state)

    try:
        flow.fetch_token(code=request.query['code'])
        token = google.oauth2.id_token.verify_oauth2_token(
            flow.credentials.id_token, google.auth.transport.requests.Request())
        email = token['email']
    except Exception as e:
        log.exception('oauth2 callback: could not fetch and verify token')
        raise web.HTTPUnauthorized() from e

    db = request.app['db']

    user = await user_from_email(db, email)

    if user is None:
        if caller == 'login':
            set_message(session, f'Account does not exist for email {email}', 'error')
            return aiohttp.web.HTTPFound(nb_url)

        assert caller == 'signup'

        username, domain = email.split('@')

        if domain != 'broadinstitute.org':
            raise web.HTTPUnauthorized()

        await db.execute_insertone(
            '''
        INSERT INTO users (state, username, email, is_developer)
        VALUES (%s, %s, %s, %s);
        ''',
            ('creating', username, email, False))

        session['pending'] = True
        session['email'] = email

        return web.HTTPFound(creating_url)

    if user['state'] in ('deleting', 'deleted'):
        page_context = {
            'username': user['username'],
            'state': user['state'],
            'email': user['email']
        }
        return await render_template('auth', request, user, 'account-error.html', page_context)

    if user['state'] == 'creating':
        if caller == 'signup':
            set_message(session, f'Account is already creating for email {email}', 'error')
        if caller == 'login':
            set_message(session, f'Account for email {email} is still being created.', 'error')
        session['pending'] = True
        session['email'] = user['email']
        return web.HTTPFound(creating_url)

    assert user['state'] == 'active'
    if caller == 'signup':
        set_message(session, f'Account has already been created for {user["username"]}.', 'info')
    session_id = await create_session(db, user['id'])
    session['session_id'] = session_id
    return aiohttp.web.HTTPFound(next_page)


@routes.get('/user')
@web_authenticated_users_only()
async def user_page(request, userdata):
    return await render_template('auth', request, userdata, 'user.html', {})


async def create_copy_paste_token(db, session_id, max_age_secs=300):
    copy_paste_token = secret_alnum_string()
    await db.just_execute(
        "INSERT INTO copy_paste_tokens (id, session_id, max_age_secs) VALUES(%s, %s, %s);",
        (copy_paste_token, session_id, max_age_secs))
    return copy_paste_token


@routes.post('/copy-paste-token')
@check_csrf_token
@web_authenticated_users_only()
async def get_copy_paste_token(request, userdata):
    session = await aiohttp_session.get_session(request)
    session_id = session['session_id']
    db = request.app['db']
    copy_paste_token = await create_copy_paste_token(db, session_id)
    page_context = {
        'copy_paste_token': copy_paste_token
    }
    return await render_template('auth', request, userdata, 'copy-paste-token.html', page_context)


@routes.post('/api/v1alpha/copy-paste-token')
@rest_authenticated_users_only
async def get_copy_paste_token_api(request, userdata):
    session_id = userdata['session_id']
    db = request.app['db']
    copy_paste_token = await create_copy_paste_token(db, session_id)
    return web.Response(body=copy_paste_token)


@routes.post('/logout')
@check_csrf_token
@web_maybe_authenticated_user
async def logout(request, userdata):
    if not userdata:
        return web.HTTPFound(deploy_config.external_url('notebook', ''))

    db = request.app['db']
    session_id = userdata['session_id']
    await db.just_execute('DELETE FROM sessions WHERE session_id = %s;', session_id)

    session = await aiohttp_session.get_session(request)
    cleanup_session(session)

    return web.HTTPFound(deploy_config.external_url('notebook', ''))


@routes.get('/api/v1alpha/login')
async def rest_login(request):
    callback_port = request.query['callback_port']

    flow = get_flow(f'http://127.0.0.1:{callback_port}/oauth2callback')
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true')

    return web.json_response({
        'authorization_url': authorization_url,
        'state': state
    })


@routes.get('/roles')
@web_authenticated_developers_only()
async def get_roles(request, userdata):
    db = request.app['db']
    roles = [x async for x in
             db.select_and_fetchall('SELECT * FROM roles;')]
    page_context = {
        'roles': roles
    }
    return await render_template('auth', request, userdata, 'roles.html', page_context)


@routes.post('/roles')
@check_csrf_token
@web_authenticated_developers_only()
async def post_create_role(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    name = post['name']

    role_id = await db.execute_insertone(
        '''
INSERT INTO `roles` (`name`)
VALUES (%s);
''',
        (name))

    set_message(session, f'Created role {role_id} {name}.', 'info')

    return web.HTTPFound(deploy_config.external_url('auth', '/roles'))


@routes.get('/users')
@web_authenticated_developers_only()
async def get_users(request, userdata):
    db = request.app['db']
    users = [x async for x in
             db.select_and_fetchall('SELECT * FROM users;')]
    page_context = {
        'users': users
    }
    return await render_template('auth', request, userdata, 'users.html', page_context)


@routes.post('/users')
@check_csrf_token
@web_authenticated_developers_only()
async def post_create_user(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    username = post['username']
    email = post.get('email')
    is_developer = post.get('is_developer') == '1'
    is_service_account = post.get('is_service_account') == '1'
    errored, user_id = handle_error_for_web(create_user, db, username, email, is_developer, is_service_account)
    if not errored:
        set_message(session, f'Created user (or it already existed) {user_id} {username} {email}.', 'info')
    return web.HTTPFound(deploy_config.external_url('auth', '/users'))


@routes.post('/users/delete')
@check_csrf_token
@web_authenticated_developers_only()
async def web_delete_user(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    id = post['id']
    username = post['username']

    n_rows = await delete_user(db, username=username, id=id)

    if n_rows != 1:
        assert n_rows == 0
        set_message(session, f'Delete failed, no such user {id} {username}.', 'error')
    else:
        set_message(session, f'Deleted user {id} {username}.', 'info')

    return web.HTTPFound(deploy_config.external_url('auth', '/users'))


@routes.get('/api/v1alpha/oauth2callback')
async def rest_callback(request):
    state = request.query['state']
    code = request.query['code']
    callback_port = request.query['callback_port']

    try:
        flow = get_flow(f'http://127.0.0.1:{callback_port}/oauth2callback', state=state)
        flow.fetch_token(code=code)
        token = google.oauth2.id_token.verify_oauth2_token(
            flow.credentials.id_token, google.auth.transport.requests.Request())
        email = token['email']
    except Exception as e:
        log.exception('fetching and decoding token')
        raise web.HTTPUnauthorized() from e

    db = request.app['db']
    users = [x async for x in
             db.select_and_fetchall("SELECT * FROM users WHERE email = %s AND state = 'active';", email)]

    if len(users) != 1:
        raise web.HTTPUnauthorized()
    user = users[0]

    session_id = await create_session(db, user['id'], max_age_secs=None)

    return web.json_response({
        'token': session_id,
        'username': user['username']
    })


@routes.post('/api/v1alpha/copy-paste-login')
async def rest_copy_paste_login(request):
    copy_paste_token = request.query['copy_paste_token']
    db = request.app['db']

    @transaction(db)
    async def maybe_pop_token(tx):
        session = await tx.execute_and_fetchone("""
SELECT sessions.session_id AS session_id, users.username AS username FROM copy_paste_tokens
INNER JOIN sessions ON sessions.session_id = copy_paste_tokens.session_id
INNER JOIN users ON users.id = sessions.user_id
WHERE copy_paste_tokens.id = %s
  AND NOW() < TIMESTAMPADD(SECOND, copy_paste_tokens.max_age_secs, copy_paste_tokens.created)
  AND users.state = 'active';""", copy_paste_token)
        if session is None:
            raise web.HTTPUnauthorized()
        await tx.just_execute("DELETE FROM copy_paste_tokens WHERE id = %s;", copy_paste_token)
        return session

    session = await maybe_pop_token()  # pylint: disable=no-value-for-parameter
    return web.json_response({
        'token': session['session_id'],
        'username': session['username']
    })


@routes.post('/api/v1alpha/logout')
@rest_authenticated_users_only
async def rest_logout(request, userdata):
    session_id = userdata['session_id']
    db = request.app['db']
    await db.just_execute('DELETE FROM sessions WHERE session_id = %s;', session_id)

    return web.Response(status=200)


@routes.get('/api/v1alpha/userinfo')
async def rest_userinfo(request):
    if 'Authorization' not in request.headers:
        log.info('Authorization not in request.headers')
        raise web.HTTPUnauthorized()

    auth_header = request.headers['Authorization']
    if not auth_header.startswith('Bearer '):
        log.info('Bearer not in Authorization header')
        raise web.HTTPUnauthorized()
    session_id = auth_header[7:]

    # b64 encoding of 32-byte session ID is 44 bytes
    if len(session_id) != 44:
        log.info('Session id != 44 bytes')
        raise web.HTTPUnauthorized()

    db = request.app['db']
    users = [x async for x in
             db.select_and_fetchall('''
SELECT users.*, sessions.session_id FROM users
INNER JOIN sessions ON users.id = sessions.user_id
WHERE users.state = 'active' AND (sessions.session_id = %s) AND (ISNULL(sessions.max_age_secs) OR (NOW() < TIMESTAMPADD(SECOND, sessions.max_age_secs, sessions.created)));
''', session_id)]

    if len(users) != 1:
        log.info(f'Unknown session id: {session_id}')
        raise web.HTTPUnauthorized()
    user = users[0]

    return web.json_response(user)


@routes.post('/api/v1alpha/users')
@rest_authenticated_developers_only
async def rest_create_user(request, userdata):  # pylint: disable=unused-argument
    db = request.app['db']
    data = await request.json()
    username = data['username']
    email = data.get('email')
    is_developer = data.get('is_developer') == '1'
    is_service_account = data.get('is_service_account') == '1'
    await handle_error_for_api(create_user, db, username, email, is_developer, is_service_account)
    return web.json_response({})


async def create_user(db,
                      username: str,
                      email: str,
                      is_developer: bool,
                      is_service_account: bool):
    if is_developer and is_service_account:
        raise HailHTTPUserError('User cannot be both a developer and a service account.', 'error')

    if not is_service_account and email is None:
        raise HailHTTPUserError('Email is required for users that are not service accounts.', 'error')

    @transaction(db)
    async def maybe_insert(tx):
        user = await tx.execute_and_fetchone("""
SELECT username, email, is_developer, is_service_account
FROM users
WHERE username = %s OR email = %s""")
        if user is not None:
            if user['email'] != email:
                raise HailHTTPUserError(
                    'a user with the same username already exists', 'error')
            if user['username'] != username:
                raise HailHTTPUserError(
                    'a user with the same email already exists', 'error')
            if user['is_developer'] != is_developer:
                raise HailHTTPUserError(
                    'user already exists with different is_developer', 'error')
            if user['is_service_account'] != is_service_account:
                raise HailHTTPUserError(
                    'user already exists with different is_service_account', 'error')
            return user['user_id']

        assert user is None
        return await db.execute_insertone(
            '''
INSERT INTO users (state, username, email, is_developer, is_service_account)
VALUES (%s, %s, %s, %s, %s);
''',
            ('creating', username, email, is_developer, is_service_account))

    return await maybe_insert()  # pylint: disable=no-value-for-parameter


@routes.post('/api/v1alpha/users/delete')
@rest_authenticated_developers_only
async def rest_delete_user(request, userdata):  # pylint: disable=unused-argument
    db = request.app['db']
    data = await request.post()
    n_affected = await delete_user(db, username=data['username'])
    assert n_affected <= 1
    return web.json_response({})


async def delete_user(db, *, username: str, id: Optional[int] = None):
    if id is not None:
        return await db.execute_update('''UPDATE users
SET state = 'deleting'
WHERE id = %s AND username = %s;
''', (id, username))

    return await db.execute_update('''UPDATE users
SET state = 'deleting'
WHERE id = %s AND username = %s;
''', (username,))


@routes.post('/api/v1alpha/create_session')
@rest_authenticated_developers_only
async def rest_create_session(request, userdata):  # pylint: disable=unused-argument
    db = request.app['db']
    data = await request.json()
    user = await user_from_username(db, data['username'])
    one_hour = 60 * 60
    thirty_days = 30 * 24 * 60 * 60
    max_age_secs = min(data.get('max_age_secs', one_hour),
                       thirty_days)

    if user is None:
        raise web.HTTPForbidden()
    if user['is_service_account'] != 1:
        raise web.HTTPForbidden()

    session_id = await create_session(db, user['id'], max_age_secs=max_age_secs)

    return web.json_response({HAIL_DEFAULT_NAMESPACE: session_id})


@routes.post('/api/v1alpha/delete_session')
@rest_authenticated_developers_only
async def rest_delete_session(request, userdata):  # pylint: disable=unused-argument
    db = request.app['db']
    data = await request.json()
    session_id = data['session_id']

    await db.just_execute('DELETE FROM sessions WHERE session_id = %s;', session_id)

    return web.json_response({})


async def on_startup(app):
    db = Database()
    await db.async_init(maxsize=50)
    app['db'] = db


async def on_cleanup(app):
    await app['db'].async_close()


def run():
    app = web.Application()

    setup_aiohttp_jinja2(app, 'auth')
    setup_aiohttp_session(app)

    setup_common_static_routes(routes)
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    web.run_app(deploy_config.prefix_application(app, 'auth'),
                host='0.0.0.0',
                port=5000,
                access_log_class=AccessLogger,
                ssl_context=get_in_cluster_server_ssl_context())
