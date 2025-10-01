import marimo

__generated_with = "0.15.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from data_loaders import DataLoaders
    from utilities import CustomExpressions as ce
    import polars as pl
    from polars import col as c
    import polars.selectors as cs
    import altair as alt
    from scratch import hospital_summary, hospital_summary_pct
    return hospital_summary, hospital_summary_pct


@app.cell
def _(hospital_summary):
    hospital_summary().collect(engine='streaming')
    return


@app.cell
def _(hospital_summary_pct):
    hospital_summary_pct().collect(engine='streaming')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
