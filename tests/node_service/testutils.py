import random

import requests
import time

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
