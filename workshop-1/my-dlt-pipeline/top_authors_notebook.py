"""
Marimo notebook: Top 10 authors by book count.
Uses dlt pipeline dataset + ibis for data access, then Plotly for visualization.
Run: marimo edit top_authors_notebook.py
Ref: https://dlthub.com/docs/general-usage/dataset-access/marimo
"""

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import dlt

    return (dlt,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Top 10 authors by book count

    This notebook uses **dlt** pipeline data with **ibis** for querying, then visualizes
    the top 10 authors by number of books (from the Open Library pipeline).
    """)
    return


@app.cell
def _(dlt):
    # Attach to the existing open_library_pipeline (run from workshop-1/my-dlt-pipeline)
    pipeline = dlt.attach(pipeline_name="open_library_pipeline")
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name  # "open_library_data"
    return dataset, dataset_name


@app.cell
def _(dataset):
    # Native ibis connection from the dlt dataset
    ibis_conn = dataset.ibis()
    return (ibis_conn,)


@app.cell
def _(dataset_name, ibis_conn):
    import ibis

    # Table of book–author links (one row per book–author pair)
    authors = ibis_conn.table("books__author_name", database=dataset_name)

    # Top 10 authors by book count
    top_authors_expr = (
        authors.group_by(authors.value)
        .aggregate(book_count=authors._dlt_parent_id.count())
        .order_by(ibis.desc("book_count"))
        .limit(10)
    )
    return (top_authors_expr,)


@app.cell
def _(top_authors_expr):
    # Execute the ibis expression to get a DataFrame
    top_10_df = top_authors_expr.execute()
    return (top_10_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    **Top 10 authors by book count (data):**
    """)
    return


@app.cell
def _(mo, top_10_df):
    mo.ui.table(top_10_df)
    return


@app.cell
def _(top_10_df):
    import plotly.express as px

    # Bar chart: author name (value) vs book_count
    fig = px.bar(
        top_10_df,
        x="value",
        y="book_count",
        labels={"value": "Author", "book_count": "Number of books"},
        title="Top 10 authors by book count (Open Library)",
        text_auto=True,
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        showlegend=False,
    )
    return (fig,)


@app.cell
def _(mo):
    mo.md("""
    **Chart:**
    """)
    return


@app.cell
def _(fig, mo):
    # Use mo.ui.plotly for interactive bar chart
    mo.ui.plotly(fig)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
