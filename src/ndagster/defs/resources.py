import dagster as dg
from dagster_polars import PolarsParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "io_manager": PolarsParquetIOManager(base_dir="s3://uploads"),
        },
    )
