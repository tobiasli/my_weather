"""Configuration of pytest for weather."""

def pytest_addoption(parser):
    parser.addoption("--password", action="store", default="")
    parser.addoption("--salt", action="store", default="")