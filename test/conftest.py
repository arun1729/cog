import pytest
from cog import torque

_original_init = torque.Graph.__init__


def _patched_init(self, *args, **kwargs):
    kwargs.setdefault("use_memory_view", _patched_init._use_memory_view)
    _original_init(self, *args, **kwargs)


def pytest_addoption(parser):
    parser.addoption(
        "--use-memory-view",
        action="store",
        default="both",
        choices=["true", "false", "both"],
        help="Run graph tests with memory view on, off, or both (default: both)",
    )


def pytest_generate_tests(metafunc):
    opt = metafunc.config.getoption("--use-memory-view")
    if "use_memory_view" in metafunc.fixturenames:
        if opt == "both":
            metafunc.parametrize("use_memory_view", [True, False], ids=["memview", "disk"], indirect=True)
        elif opt == "true":
            metafunc.parametrize("use_memory_view", [True], ids=["memview"], indirect=True)
        else:
            metafunc.parametrize("use_memory_view", [False], ids=["disk"], indirect=True)


@pytest.fixture
def use_memory_view(request):
    _patched_init._use_memory_view = request.param
    torque.Graph.__init__ = _patched_init
    yield request.param
    torque.Graph.__init__ = _original_init


@pytest.fixture(autouse=True)
def _auto_memory_view_mode(request):
    """Automatically apply memory view mode for unittest-style test classes."""
    opt = request.config.getoption("--use-memory-view")
    if opt == "both":
        yield
        return
    mode = opt == "true"
    _patched_init._use_memory_view = mode
    torque.Graph.__init__ = _patched_init
    yield
    torque.Graph.__init__ = _original_init
