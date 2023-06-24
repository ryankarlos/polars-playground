import polars as pl


def pipeline() -> pl.DataFrame.pipe:
    """Example pipeline.

    Returns:
        pl.DataFrame.pipe:
    """
    df = pl.DataFrame({"b": [1, 2], "a": [3, 4]})
    return df.pipe(lambda tdf: tdf.select(sorted(tdf.columns)))
