import polars as pl
from polars import col as c
import polars.selectors as cs
from IPython.display import Image, display

class CustomExpressions:

    @staticmethod
    def unique_hospital_ct() -> pl.Expr:
        return c.hospital_id.n_unique().alias('unique_hospital_ct')

    @staticmethod
    def hcpcs_name() -> pl.Expr:
        return c.hcpcs_desc.str.split(' - ').list.get(1).str.split('[').list.get(0).alias('hcpcs_name')

    @staticmethod
    def unique_type_of_measurements() -> pl.Expr:
        return cs.matches('(?i)type.*meas').str.to_lowercase().str.strip_chars().unique().alias('unique_type_of_measurements')

    @staticmethod
    def calculate_price_pct() -> pl.Expr:
        return (cs.matches('(?i)calculated').sum().truediv(pl.len()).round(4)).alias('calculate_price_pct')

    @staticmethod
    def price_stats() -> list[pl.Expr]:
        return (
            [
                c.standard_charge_negotiated_dollar.min().round(2).alias('min_price'),
                c.standard_charge_negotiated_dollar.max().round(2).alias('max_price'),
                c.standard_charge_negotiated_dollar.mean().round(2).alias('mean_price'),
                c.standard_charge_negotiated_dollar.std().round(2).alias('std_price'),
            ]
        )

    @staticmethod
    def j_code_predicate() -> pl.Expr:
        return c.hcpcs.str.starts_with('J').alias('is_j_code')

    @staticmethod
    def pct_negotiated_gt_cash() -> pl.Expr:
        # sum boolean true values and divide by total count to get percentage
        return (c.standard_charge_negotiated_dollar.gt(c.standard_charge_discounted_cash).sum().truediv(pl.len()).round(4).alias('pct_price_gt_cash_price'))
    

def render_plotly_figure(fig, name):
    """
    Render a Plotly figure as both a static PNG and an inline image in a Jupyter notebook.
    Parameters:
    - fig: Plotly figure object to render.
    - name: Base name for saving the PNG file (without extension).
    """
    # Save a static PNG (will be embedded in the notebook output and therefore visible on GitHub)
    png_path = f"fig/{name}.png"

    # write static PNG (requires kaleido)
    fig.write_image(png_path, width=800, height=600, scale=2)

    # display the PNG inline (this output will be stored in the notebook and rendered by GitHub)
    display(Image(png_path))