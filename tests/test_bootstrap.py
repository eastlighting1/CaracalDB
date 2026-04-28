import caracaldb


def test_package_exports_version() -> None:
    assert caracaldb.__version__ == "0.1.0"
