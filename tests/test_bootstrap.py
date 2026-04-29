import caracaldb
from caracaldb._version import __version__


def test_package_exports_version() -> None:
    assert caracaldb.__version__ == __version__
