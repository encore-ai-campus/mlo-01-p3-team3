"""Command arguments and JSON presentation for the gold service."""
import argparse
from pathlib import Path

from ..console import emit
from .service import Request, run


def main(argv=None):
    parser=argparse.ArgumentParser(description='고정 Silver 버전의 상품 Gold 생성')
    for name in ('config','versions','inputs','registration','state-dir'):
        parser.add_argument('--'+name,required=True,type=Path)
    parser.add_argument('--as-of',required=True,help='UTC 업무 평가 기준 시각. 재개 때 동일 값 사용')
    parser.add_argument('--max-products',type=int,default=0)
    parser.add_argument('--rehearse',action='store_true')
    parser.add_argument('--batch-writes',action='store_true',help='최대 32MiB·100상품의 테이블별 일괄 저장')
    parser.add_argument('--selection',type=Path,help='고정 원천 배치에 속한 상품만 평가')
    args=parser.parse_args(argv)
    outcome = run(Request(**vars(args)), progress=emit)
    emit(outcome.report)
    return outcome.code
