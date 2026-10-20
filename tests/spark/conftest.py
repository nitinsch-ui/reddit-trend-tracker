import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
