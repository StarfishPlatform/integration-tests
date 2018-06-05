from .testutils import gen_log
from .testutils import get, post


def test_graph_can_be_queried():
    r = get('/graph')
    assert r.status_code == 200


def test_graph_will_contain_my_services(test_name):
    post(['log', test_name, 'some-run-id'], json=gen_log())
    r = get('/graph')

    assert r.json() == ''
