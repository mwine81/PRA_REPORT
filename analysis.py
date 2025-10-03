import console
from data_loaders import DataLoaders as dl
import polars as pl
from polars import col as c
import polars.selectors as cs
import importlib
from console import console

def cancer_predicate():
    """
    Predicate to filter for cancer drugs based on gpi_4_class.
    """
    return c.gpi_4_class.str.contains('(?i)antineoplastic')

def ms_predicate():
    """
    Predicate to filter for multiple sclerosis drugs based on gpi_4_class.
    """
    return c.gpi_4_class.str.contains('(?i)multiple.*sclerosis')

def cancer_or_ms_predicate():
    """
    Predicate to filter for either cancer or multiple sclerosis drugs.
    """
    return (cancer_predicate() | ms_predicate())

def cancer_and_ms_ndcs() -> pl.LazyFrame:
    """
    Get NDCs for drugs used in cancer and multiple sclerosis treatment.
    """
    return (
    # load medispan data and filter for cancer or MS drugs
    dl
    .load_medispan_table()
    .filter(cancer_or_ms_predicate())
    .group_by(c.product, c.gpi_4_class)
    .agg(c.ndc.unique())
    .unique()
    )

def load_base_data() -> pl.LazyFrame:
    """
    Load hospital price table data for cancer and multiple sclerosis drugs.
    """
    # load a table of ndcs for cancer and ms drugs. Explode the list of ndcs to join on
    ndcs = cancer_and_ms_ndcs().explode('ndc')
    return (
        # load hospital price table
        dl
        .load_hospital_price_table()
        # join data on cancer and ms ndcs
        .join(ndcs, on='ndc')
    )

def show(lazy_frame: pl.LazyFrame) -> None:
    return lazy_frame.collect(engine="streaming").glimpse()

def is_inpatient_predicate() -> pl.Expr:
    """
    Predicate to identify outpatient services based on the 'place_of_service' column.
    """
    return cs.matches('(?i)setting').str.contains('(?i)both|in').or_(cs.matches('(?i)setting').is_null()).alias('is_inpatient')

if __name__ == "__main__":
    product_selection = 'drug_name'
    console.print(
        dl
        .load_hospital_price_table_with_drug_names()
        .filter(c.setting.str.contains('(?i)in|out'))
        .group_by(c.hospital_id, c.setting, product_selection)
        .agg(
            c.standard_charge_negotiated_dollar.mean().round(2),
        )
        .collect(engine='streaming')
        .pivot(
            on='setting',
            index=['hospital_id', product_selection],
        )
        .filter(c.outpatient.is_not_null() & c.inpatient.is_not_null())
        .with_columns(c.outpatient.sub(c.inpatient).round(2).alias('outpatient_diff'))
        .select(
            pl.len().alias('row_ct'),
            c.hospital_id.n_unique().alias('unique_hospitals'),
            c.outpatient.mean().round(2).alias('avg_outpatient_price'),
            c.inpatient.mean().round(2).alias('avg_inpatient_price'),
            c.outpatient_diff.mean().round(2).alias('avg_outpatient_diff'),
        )
    )
        
    
    
    



