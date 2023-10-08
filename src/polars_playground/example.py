
import requests



r = requests.get("https://datasets-server.huggingface.co/parquet?dataset=malicious-website-features-2.4M")
j = r.json()
urls = [f['url'] for f in j['parquet_files'] if f['split'] == 'train']
urls
['https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0000.parquet',
 'https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0001.parquet']

import polars as pl

df = (
    pl.read_parquet("https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0000.parquet")
    .groupby("horoscope")
    .agg(
        [
            pl.count(),
            pl.col("text").str.n_chars().mean().alias("avg_blog_length")
        ]
    )
    .sort("avg_blog_length", descending=True)
    .limit(5)
)
print(df)
shape: (5, 3)
