import polars as pl
from polars import col as c
import polars.selectors as cs
from config import BASE_PARQUET_PATH, DATABASE_URL, FACILITY_DIRECTORY, MEDISPAN_PATH, PRA_REPORT_QUERIES
import duckdb
from console import console

def generate_facility_directory():
    """Load the facility directory from the database."""
    with duckdb.connect(database=DATABASE_URL) as con:
        con.sql('SELECT * FROM facility_directory').pl().write_parquet(FACILITY_DIRECTORY)

class DataLoaders:
    @staticmethod
    def load_hospital_price_table() -> pl.LazyFrame:
        return pl.scan_parquet(BASE_PARQUET_PATH / "hospital_price_table.parquet")

    @staticmethod
    def load_hcpcs_desc_table() -> pl.LazyFrame:
        return pl.scan_parquet(BASE_PARQUET_PATH / "hcpcs_desc_table.parquet")

    @staticmethod
    def load_ndc_name_table() -> pl.LazyFrame:
        return pl.scan_parquet(BASE_PARQUET_PATH / "ndc_name_table.parquet")

    @staticmethod
    def load_hospital_table() -> pl.LazyFrame:
        return pl.scan_parquet(BASE_PARQUET_PATH / "hospital_table.parquet")
    
    @staticmethod
    def load_facility_directory() -> pl.LazyFrame:
        return pl.scan_parquet(FACILITY_DIRECTORY)
    
    @staticmethod
    def load_benchmark_table() -> pl.LazyFrame:
        return pl.scan_parquet(BASE_PARQUET_PATH / "benchmark_table.parquet")

    @staticmethod
    def load_medispan_table() -> pl.LazyFrame:
        return pl.scan_parquet(MEDISPAN_PATH)


if __name__ == "__main__":
    search_terms = ['ANTINEOPLASTICS AND ADJUNCTIVE THERAPIES','PSYCHOTHERAPEUTIC AND NEUROLOGICAL AGENTS - MISC.']

    def drug_predicate_gpi_2(gpi_2_groups: list[str]) -> pl.Series:
        return (
        DataLoaders().load_medispan_table()
        .filter(c.gpi_2_group.is_in(gpi_2_groups))
        .select(c.ndc)
        .collect(engine="streaming")
        .to_series()
        .implode()
        )

    def get_ndcs_hcpcs(gpi_2_groups: list[str]) -> list[list[str]]:
        data = (
            DataLoaders().load_benchmark_table()
            .filter(c.ndc.is_in(drug_predicate_gpi_2(gpi_2_groups)))
        )
        ndcs = data.select(c.ndc).filter(c.ndc.is_not_null()).collect(engine="streaming").to_series().unique().to_list()

        hcpcs = data.select(c.hcpcs).filter(c.hcpcs.is_not_null()).collect(engine="streaming").to_series().unique().to_list()

        return [ndcs, hcpcs]

    def gpi_2_filter(gpi_2_groups: list[str]) -> pl.Expr:
        return c.ndc.is_in(get_ndcs_hcpcs(gpi_2_groups)[0]) 


    (
    DataLoaders()
    .load_medispan_table()
    .filter(c.product.str.contains('(?i)lisinopril'))
    .collect(engine="streaming")
    .glimpse()
    )