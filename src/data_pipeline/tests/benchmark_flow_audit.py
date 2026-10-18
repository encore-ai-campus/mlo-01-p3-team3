"""Synthetic streaming correspondence benchmark, NOT full ETL capacity evidence."""
import argparse
from pathlib import Path
import time
import tracemalloc

from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json
from product_pipeline.operations.flow_audit import compare_sorted


def main():
    p=argparse.ArgumentParser();p.add_argument('--observations',type=int,default=65498);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if not 1<=a.observations<=1000000:p.error('use 1..1000000 synthetic rows')
    def source():
        for i in range(a.observations):yield dict(id=f'{i:012d}',document_hash=f'{i:064x}')
    tracemalloc.start();started=time.perf_counter()
    count=compare_sorted(source(),source(),'id',['document_hash'])
    seconds=time.perf_counter()-started;_,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    report=dict(passed=count==a.observations,scope='SYNTHETIC_SORTED_STREAM_COMPARE_ONLY',observations=count,
        duration_seconds=seconds,python_traced_peak_bytes=peak,external_services=0,
        measures_raw_json_parsing=False,measures_sorting=False,measures_spark_or_mysql=False,
        production_capacity_verified=False)
    atomic_json(a.output,report);print(canonical(report));return 0


if __name__=='__main__':raise SystemExit(main())
