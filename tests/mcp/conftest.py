import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    # tests on the db fixture need the real warehouse, so they run with the integration suite
    for item in items:
        if "db" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.integration)
