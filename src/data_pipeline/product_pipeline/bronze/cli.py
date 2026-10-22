"""Command arguments and JSON presentation for the bronze service."""
import argparse
from pathlib import Path

from ..console import emit
from .service import Request, run
from .service import await_window


def main(argv=None):
    parser = argparse.ArgumentParser(description='Kafka/MongoDB -> product Bronze Delta')
    parser.add_argument('--config', required=True)
    parser.add_argument('--available-now', action='store_true', help='Drain current Kafka backlog then exit')
    parser.add_argument('--max-batches',type=int,default=0,help='0=전체 적체, 양수=확정 배치 단위로 후속 단계 인계')
    parser.add_argument('--result-file',help='정상 종료 확인 후 단계 실행 결과 기록')
    args = parser.parse_args(argv)
    outcome = run(Request(**vars(args)), progress=emit)
    emit(outcome.report)
    return outcome.code
