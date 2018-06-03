import random
import time

import pytest
import requests

API_URL = 'http://localhost:3000'
RUN_ID = str(int(time.time())).rjust(11, '0')


def get(path, fail=True):
    if not isinstance(path, type('string')):
        path = '/' + '/'.join(path)

    url = API_URL + path
    print("getting url=", url)
    r = requests.get(url)

    if fail:
        r.raise_for_status()
    return r


def post(path, json=None, fail=True):
    if not isinstance(path, type('string')):
        path = '/' + '/'.join(path)

    url = API_URL + path
    print("Posting to url=", url)
    r = requests.post(url, json=json)

    if fail:
        r.raise_for_status()
    return r


def name(prefix):
    return f'{prefix}-{RUN_ID}'


def unique_name(prefix):
    unique = str(random.randint(0, 9999999)).ljust(7, '0')
    return f'{name(prefix)}-{unique}'


def gen_log():
    return dict(
        userID=name('some-user'),
        timestamp=int(time.time() * 1000),
        direction=random.choice(['in', 'out']),
        storage=name('some-storage')
    )


def complete_log(log, service_id, run_id):
    return dict(
        serviceID=service_id,
        runID=run_id,
        **log
    )


def complete_batch(batch, service_id, run_id):
    return [dict(
        serviceID=service_id,
        runID=run_id,
        timestamp=batch['timestamp'],
        storage=batch['storage'],
        direction=batch['direction'],
        userID=x)
        for x in batch['userIDs']]


def gen_batch(count=100):
    return dict(
        userIDs=[name(f'some-user-{i}') for i in range(count)],
        timestamp=int(time.time() * 1000),
        direction=random.choice(['in', 'out']),
        storage=name('some-storage')
    )


def logs_eq(p1, p2):
    assert isinstance(p1, type([]))
    assert isinstance(p2, type([]))

    p1 = sorted(p1, key=lambda x: [x[k] for k in sorted(x.keys())])
    p2 = sorted(p2, key=lambda x: [x[k] for k in sorted(x.keys())])

    assert p1 == p2
    return True


@pytest.fixture
def test_name(request):
    return name(request.node.name)


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
