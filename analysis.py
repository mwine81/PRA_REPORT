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

def payment_cols() -> pl.Expr:
    return cs.matches('(?i)gross|cash|dollar$')

def standardize_single_unit() -> pl.Expr:
    """
    Standardize the drug unit of measurement to '1 UNIT'.
    """
    return payment_cols().truediv(cs.matches('(?i)unit')).round(4)



if __name__ == "__main__":
    product_selection = 'hcpcs'
    console.log(
        dl
        .load_hospital_price_table_with_drug_names()
        #.filter(c.hospital_id == 'bffa8e3d-dcb6-45f0-a2c6-cf546cca6e8f')
        .filter(pl.col(product_selection).is_not_null())
        .with_columns(standardize_single_unit())
        .filter(c.standard_charge_negotiated_dollar > .01)
        .with_columns(c.setting.fill_null('Unknown').alias('setting'))
        .with_columns(c.standard_charge_negotiated_dollar.mean().over([ product_selection, 'setting', 'drug_type_of_measurement']).round(2).alias('avg_negotiated_dollar'))
        .with_columns(c.standard_charge_negotiated_dollar.std().over([product_selection, 'setting', 'drug_type_of_measurement']).round(2).alias('std_negotiated_dollar'))
        .with_columns(c.standard_charge_negotiated_dollar.sub(c.avg_negotiated_dollar).truediv(c.std_negotiated_dollar).round(4).alias('z_score_negotiated_dollar'))
        .filter(c.z_score_negotiated_dollar.is_infinite().or_(c.z_score_negotiated_dollar.is_null().or_(c.z_score_negotiated_dollar.is_nan())).not_())
        .group_by('hospital_id',product_selection)
        .agg(c.z_score_negotiated_dollar.mean().round(4))
        .group_by('hospital_id')
        .agg(c.z_score_negotiated_dollar.mean().round(4).alias('avg_z_score_negotiated_dollar'))
        .sort('avg_z_score_negotiated_dollar', descending=True)
        .collect(engine="streaming")
    )
        
    
    
    



