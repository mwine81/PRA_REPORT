import polars as pl
from polars import col as c
import polars.selectors as cs
from data_loaders import DataLoaders as dl

def hcpcs_code() -> pl.Expr:
    return cs.matches('(?i)1$').alias('hcpcs')

def ndc() -> pl.Expr:
    return cs.matches('(?i)column_4').str.replace_all('[-]','').str.zfill(11).alias('ndc')

def load_asp_ndc_data() -> pl.LazyFrame:
    return pl.scan_csv(r'C:\Users\mwine\Projects\SEPT_2025\PRA_REPORT\data\asp*.csv', skip_lines=1, infer_schema_length=0, encoding='utf8-lossy', has_header=False)

def drug_name() -> pl.Expr:
    return cs.matches('(?i)drug.*name').alias('drug_name')

def load_asp_ndc():
    return (
    load_asp_ndc_data()
    .select(ndc(),hcpcs_code())
    .filter(c.hcpcs.str.contains('(?i)^J'))
    .join(dl.load_medispan_table().select(c.ndc), on='ndc')
    .unique('hcpcs')
    )

def load_drug_name_table() -> pl.LazyFrame:
    return (
    dl.load_medispan_table().select(c.ndc,c.gpi, c.gpi_10_generic_name.alias('drug_name'))
    .join(load_asp_ndc().select(c.ndc,c.hcpcs), on='ndc', how='left')
    )

def add_standard_drug_name(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf
        .join(load_drug_name_table().select(c.hcpcs, c.drug_name.alias('drug_name_1')).filter(c.hcpcs.is_not_null()), on='hcpcs', how='left')
        .join(load_drug_name_table().select(c.ndc, c.drug_name.alias('drug_name_2')), on='ndc', how='left')
        .with_columns(pl.coalesce([c.drug_name_1, c.drug_name_2]).alias('drug_name'))
        .drop(['drug_name_1','drug_name_2'])
        
    )
    




