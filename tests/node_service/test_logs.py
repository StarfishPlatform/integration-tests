from .testutils import gen_batch, complete_batch, gen_log, complete_log, logs_eq
from .testutils import get, post


def test_api_can_be_reached():
    r = get('/')
    assert r.status_code == 200


def test_api_post_fails_400_on_invalid_schema(test_name):
    r = post(['log', test_name, 'some-run-id'], json={}, fail=False)
    assert r.status_code == 400


def test_api_post_succeeds_201(test_name):
    r = post(['log', test_name, 'some-run-id'], json=gen_log())
    assert r.status_code == 201


def test_post_invalid_batches_fails_400(test_name):
    batch = gen_batch(count=10)
    batch[-1] = {'something': 'invalid'}
    r = post(['logs', test_name, 'some-run-id'], json=batch, fail=False)
    assert r.status_code == 400


def test_post_batch_logs_succeeds_201(test_name):
    r = post(['logs', test_name, 'some-run-id'], json=gen_batch(count=10))
    assert r.status_code == 201


def test_get_retrieve_logs_is_empty(test_name):
    r = get(['logs', test_name, 'some-run-id'])
    assert r.json() == dict(payload=[], nextPage=None)


def test_get_after_post_single_works(test_name):
    log = gen_log()
    service_id, run_id = test_name, 'some-run-id'
    expected_payload = [complete_log(log, service_id, run_id)]
    post(['log', service_id, run_id], json=log)

    r = get(['logs', service_id, run_id])

    json = r.json()
    assert json['nextPage'] is None
    assert json['payload'] == expected_payload


def test_get_after_post_batch_works(test_name):
    batch = gen_batch(10)
    service_id, run_id = test_name, 'some-run-id'
    expected_payload = complete_batch(batch, service_id, run_id)
    post(['logs', service_id, run_id], json=batch)

    r = get(['logs', service_id, run_id])

    json = r.json()
    assert json['nextPage'] is None
    assert logs_eq(json['payload'], expected_payload)


def test_get_service_is_empty_by_default(test_name):
    r = get(['logs', test_name])

    assert r.json()['nextPage'] is None
    assert r.json()['payload'] == []


def test_get_service_after_batches_returns_their_union(test_name):
    batch1, batch2 = gen_batch(2), gen_batch(4)

    post(['logs', test_name, 'run1'], json=batch1)
    post(['logs', test_name, 'run2'], json=batch2)

    r = get(['logs', test_name])

    batches = (complete_batch(batch1, test_name, 'run1')
               + complete_batch(batch2, test_name, 'run2'))

    assert r.json()['nextPage'] is None
    assert logs_eq(r.json()['payload'], batches)
