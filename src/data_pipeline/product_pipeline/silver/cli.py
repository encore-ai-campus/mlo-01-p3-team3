"""Command arguments and JSON presentation for the silver service."""
import argparse
from pathlib import Path

from ..console import emit
from .service import Request, run


def main(argv=None):
    parser=argparse.ArgumentParser(description='상품 Silver 정규화·품질·Delta 평가')
    parser.add_argument('--config',required=True)
    parser.add_argument('--bronze-version',required=True,type=int)
    parser.add_argument('--inputs',required=True,type=Path)
    parser.add_argument('--registration',required=True,type=Path)
    parser.add_argument('--state-dir',required=True,type=Path,help='지역 실행 로그. 공유 discovery checkpoint와 별도')
    parser.add_argument('--rehearse',action='store_true')
    parser.add_argument('--image-states',type=Path,help='rehearsal 전용 명시 상태 export')
    parser.add_argument('--max-observations',type=int,default=0,help='0=고정 Bronze 전체, 양수=소량 검증')
    parser.add_argument('--scan-id',help='자동 실행의 고정 flow hash. 다른 실행의 부분 cursor 재사용 방지')
    parser.add_argument('--batch-writes',action='store_true',help='제한된 페이지를 테이블별 일괄 저장·검증')
    parser.add_argument('--collector-normalized',action='store_true',help='Mongo danawa_products를 원천 관측과 대조하고 불변 입력 보존')
    parser.add_argument('--selection',type=Path,help='해시로 봉인한 최대 100개 원천 관측 범위')
    parser.add_argument('--reuse-selection',type=Path,help='같은 관측의 이전 완료 평가 후보. 실제 입력·저장 검증 후에만 재사용')
    args=parser.parse_args(argv)
    outcome = run(Request(**vars(args)), progress=emit)
    emit(outcome.report)
    return outcome.code
