from data_loaders import DataLoaders as dl
import polars as pl
from polars import col as c
import polars.selectors as cs
import importlib

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

if __name__ == "__main__":
    (
    dl
    .load_hospital_price_table_with_drug_names()
    .group_by('hospital_id')
    # get unique counts of drug_name, hcpcs, ndc at hospital level
    .agg(
        c.drug_name.n_unique().alias('drugs_unique'),
        c.hcpcs.n_unique().alias('hcpcs_unique'),
        c.ndc.n_unique().alias('ndcs_unique')
    )
    .select(
        # get average, min, max, stddev of unique drug_name, hcpcs, ndc counts across hospitals
        cs.numeric().mean().cast(pl.Int32).name.prefix('avg_'),
        cs.numeric().min().cast(pl.Int32).name.prefix('min_'),
        cs.numeric().max().cast(pl.Int32).name.prefix('max_'),
        cs.numeric().std().cast(pl.Int32).name.prefix('std_'),
    )
    .collect(engine="streaming")
    .glimpse()
    )



