import pytest

from .testutils import name


@pytest.fixture
def test_name(request):
    return name(request.node.name)
