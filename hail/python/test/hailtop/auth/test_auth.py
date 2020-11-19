from hailtop.auth import (create_user, create_session, get_userinfo,
                          delete_user, delete_session)


def test_create_user_and_session():
    try:
        username = 'test-user-123'
        email = 'test-user-123@broadinstitute.org'

        response = create_user(username, email, '--service-account')
        assert response == {}
        response = create_user(username, email, '--service-account')  # test idempotence
        assert response == {}
        tokens = create_session(username, max_age_secs=5 * 60)
        assert len(tokens) == 1
        session_id = tokens.values()[0]
        userinfo = get_userinfo(session_id=session_id)
        assert userinfo['username'] == username
        assert userinfo['email'] == email
        assert userinfo['is_service_account'] == 1
        assert userinfo['session_id'] == session_id
        delete_session(session_id)
    finally:
        delete_user(username)
