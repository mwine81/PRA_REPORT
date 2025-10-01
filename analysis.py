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

(
load_base_data()
.group_by(c.product)
.agg(
    cs.matches('^standard.*gross|cash|negotiated_dollar').n_unique().name.suffix('_unique_count'),
)
.select(
    cs.numeric().min().round(2).name.suffix('_min'),
    cs.numeric().max().round(2).name.suffix('_max'),
    cs.numeric().mean().round(2).name.suffix('_avg'),
)
.select(~cs.matches('(?i)calculated'))
.collect(engine="streaming")
)

