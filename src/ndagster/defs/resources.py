import dagster as dg
from dagster_polars import PolarsParquetIOManager
from upath import UPath


class RawFileIOManager(dg.UPathIOManager):
    extension = ".zip"
    content_type = "application/zip"

    def dump_to_path(self, context: dg.OutputContext, obj: bytes, path: UPath) -> None:
        path.fs.pipe_file(str(path), obj, ContentType=self.content_type)
        context.add_output_metadata({"content_type": self.content_type})

    def load_from_path(self, context: dg.InputContext, path: UPath) -> bytes:
        return path.read_bytes()


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "io_manager_parquet": PolarsParquetIOManager(base_dir="s3://uploads"),
            "io_manager_raw": RawFileIOManager(base_path=UPath("s3://uploads/raw")),
        }
    )
