import dagster as dg
from dagster_polars import PolarsParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    manager = PolarsParquetIOManager(base_dir="s3://uploads")
    return dg.Definitions(resources={"io_manager": manager})
