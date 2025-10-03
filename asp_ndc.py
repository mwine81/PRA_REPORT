import polars as pl
from polars import col as c
import polars.selectors as cs
from config import MEDISPAN_PATH

def _hcpcs_code() -> pl.Expr:
    return cs.matches('(?i)1$').alias('hcpcs')

def _ndc() -> pl.Expr:
    return cs.matches('(?i)column_4').str.replace_all('[-]','').str.zfill(11).alias('ndc')

def _load_asp_ndc_data() -> pl.LazyFrame:
    return pl.scan_csv(r'C:\Users\mwine\Projects\SEPT_2025\PRA_REPORT\data\asp*.csv', skip_lines=1, infer_schema_length=0, encoding='utf8-lossy', has_header=False)

def _load_medispan() -> pl.LazyFrame:
    return pl.scan_parquet(MEDISPAN_PATH)

def _load_asp_ndc():
    return (
    _load_asp_ndc_data()
    .select(_ndc(),_hcpcs_code())
    .filter(c.hcpcs.str.contains('(?i)^J'))
    .join(_load_medispan().select(c.ndc), on='ndc')
    .unique('hcpcs')
    )

def _load_drug_name_table() -> pl.LazyFrame:
    return (
    _load_medispan().select(c.ndc,c.gpi, c.gpi_10_generic_name.alias('drug_name'))
    .join(_load_asp_ndc().select(c.ndc,c.hcpcs), on='ndc', how='left')
    )

def add_standard_drug_name(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf
        .join(_load_drug_name_table().select(c.hcpcs, c.drug_name.alias('drug_name_1')).filter(c.hcpcs.is_not_null()), on='hcpcs', how='left')
        .join(_load_drug_name_table().select(c.ndc, c.drug_name.alias('drug_name_2')), on='ndc', how='left')
        .with_columns(pl.coalesce([c.drug_name_1, c.drug_name_2]).alias('drug_name'))
        .drop(['drug_name_1','drug_name_2'])
        
    )



    




