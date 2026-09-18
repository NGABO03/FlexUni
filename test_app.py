import hmac, hashlib
import pytest
import app as m

@pytest.fixture(autouse=True)
def fresh_db():
    with m.app.app_context():
        m.db.drop_all()
        m.db.create_all()
        m.seed()
    yield


def csrf(client):
    client.get('/register')
    with client.session_transaction() as sess:
        return sess['csrf']


def test_register_requires_csrf():
    client = m.app.test_client()
    response = client.post('/register', data={
        'name': 'Test Student', 'email': 'test@example.com', 'password': 'password123',
        'course': 'Computer Science', 'university_id': '1'
    })
    assert response.status_code == 400


def test_register_creates_membership():
    client = m.app.test_client()
    response = client.post('/register', data={
        'csrf_token': csrf(client), 'name': 'Test Student', 'email': 'test@example.com', 'password': 'password123',
        'course': 'Computer Science', 'university_id': '1'
    })
    assert response.status_code == 302
    user = m.User.query.filter_by(email='test@example.com').first()
    assert user is not None
    assert m.UniversityMembership.query.filter_by(user_id=user.id, university_id=1, status='active').first() is not None


def test_signed_payment_webhook_does_not_require_csrf():
    client = m.app.test_client()
    raw = b'{"id":"evt_test","type":"checkout.completed"}'
    signature = hmac.new(b'webhook-secret', raw, hashlib.sha256).hexdigest()
    response = client.post('/billing/webhook', data=raw, headers={
        'X-Event-Id': 'evt_test', 'X-Event-Type': 'checkout.completed',
        'X-Webhook-Signature': signature,
    })
    assert response.status_code == 202


def test_security_headers_are_present():
    response = m.app.test_client().get('/healthz')
    assert response.status_code == 200
    assert response.headers['X-Content-Type-Options'] == 'nosniff'
    assert 'Content-Security-Policy' in response.headers
    assert 'Permissions-Policy' in response.headers
