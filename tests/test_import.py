def test_import_package():
    import importlib
    mod = importlib.import_module("lakehouse_aqi")
    assert hasattr(mod, "spark_session") or hasattr(mod, "spark_session")
