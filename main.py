import os

import s3fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sas7bdat import SAS7BDAT
import lzma
import pathlib
import shutil
import json
import pydash
import sh
import click

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_EXCEPTION

files = [
"vicks-hft-datasets/vicks-hft-datasets/OPTM_EU/option_price_view.sas7bdat.lz"
"vicks-hft-datasets/vicks-hft-datasets/OPTM_EU/tick_option_price_view.sas7bdat.lz"
"vicks-hft-datasets/vicks-hft-datasets/OPTM_EU/tick_volatility_surface_view.sas7bdat.lz"
"vicks-hft-datasets/vicks-hft-datasets/OPTM_EU/volatility_surface_view.sas7bdat.lz"
]

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
BB_KEY = os.environ["BB_KEY"]
BB_SECRET = os.environ["BB_SECRET"]

BB_EP = "https://s3.us-west-004.backblazeb2.com"
BB_BUCKET = "vicks-hft-datasets"
prefix = f"{BB_BUCKET}/vicks-hft-datasets/OPTM_EU/"

src_fs: s3fs.S3FileSystem = s3fs.S3FileSystem(key=BB_KEY, secret=BB_SECRET, client_kwargs={"endpoint_url": BB_EP})
dst_fs: s3fs.S3FileSystem = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)

DST_BUCKET = "philippe-trino/OPTM_EU"


def _run(f):
    dst_fs: s3fs.S3FileSystem = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)
    src_fs: s3fs.S3FileSystem = s3fs.S3FileSystem(key=BB_KEY, secret=BB_SECRET, client_kwargs={"endpoint_url": BB_EP})

    # download file
    outfilename = f.removeprefix(prefix)
    src_fs.download(f, outfilename)
    # decompress
    sh.lzip("-fd", outfilename)
    outfilename = outfilename.removesuffix(".lz")

    # convert to parquet
    for idx, df in enumerate(pd.read_sas(outfilename, chunksize=20000000)):
        with dst_fs.open(f"{DST_BUCKET}/{outfilename.removesuffix('.sas7bdat')}/{idx}.parquet", "wb") as dstfile:
            df.convert_dtypes().to_parquet(dstfile)

    sh.rm("-f", outfilename)


_run(files[int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])])
