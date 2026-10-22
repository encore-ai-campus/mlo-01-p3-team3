"""Read current progress and persist the first completed batch of a measurement."""
import argparse,json,sys
from pathlib import Path


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--measurement',type=Path)
    parser.add_argument('--mysql',action='store_true')
    args=parser.parse_args(argv);root=args.root.resolve()
    sys.path[:0]=[str(root),str(root/'source')]
    from product_pipeline.files import read_json,atomic_json
    from product_pipeline.contracts import now
    flow=read_json(root/'flow-progress.json')
    result=dict(at=now(),flow={k:flow.get(k) for k in ('status','stage','epoch','stage_attempt','error_code','updated_at')})
    result['writers']=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try:command=[v.decode(errors='replace') for v in (p/'cmdline').read_bytes().split(b'\0') if v]
        except OSError:continue
        scripts=[Path(a).name for a in command if Path(a).name in {'continuous_work.py','production_work.py','production_window_work.py'}]
        if scripts:result['writers'].append(dict(pid=int(p.name),scripts=scripts))
    epoch=Path(flow.get('execution_root',root/'epochs'/('%06d'%flow.get('epoch',0))))
    for name in ('bronze-result.json','bronze-window.json','windows-progress.json'):
        path=epoch/name
        if path.exists():result[name]=read_json(path)
    progress=result.get('windows-progress.json',{})
    if progress.get('execution_root'):
        cut=Path(progress['execution_root']);phases={}
        for stage in ('silver','gold','publish','activation'):
            path=cut/(stage+'-result.json')
            if path.exists():
                row=read_json(path);phases[stage]={k:row[k] for k in ('status','duration_ms','finished_at','reviewed','silver_normalization_skipped','runtime_metrics') if k in row}
        result['current_batch']=dict(root=str(cut),phases=phases)
    if args.measurement:
        request=read_json(args.measurement);first=args.measurement.with_name('first-measured-batch.json')
        if not first.exists():
            for path in sorted((epoch/'windows').glob('*/activation-result.json')):
                activation=read_json(path)
                if activation.get('status')!='COMPLETED' or activation.get('finished_at','')<request['started_at']:continue
                cut=path.parent
                phases={stage:read_json(cut/(stage+'-result.json')) for stage in ('plan','silver','gold','publish','activation')}
                if any(p.get('status')!='COMPLETED' for p in phases.values()):continue
                selected=read_json(cut/'selection.json')
                atomic_json(first,dict(status='MEASURED',captured_at=now(),batch_root=str(cut),
                    observations=len(selected['observation_keys']),products=len(selected['product_keys']),
                    stage_duration_ms={k:v.get('duration_ms',0) for k,v in phases.items()},
                    recorded_stage_total_ms=sum(v.get('duration_ms',0) for v in phases.values()),
                    activation_normalization_skipped=activation.get('silver_normalization_skipped',0),
                    source_patch=request.get('source_patch'),continuous_processing=True))
                break
        if first.exists():result['first_measured_batch']=read_json(first)
    if args.mysql:
        from production_work import setup
        from product_pipeline.publication.mysql import MySQL
        config,_=setup();db=MySQL(config)
        try:
            with db.read_snapshot():
                counts=db.one("SELECT COUNT(*) products,SUM(status='ACTIVE') active,SUM(status='PENDING_REVIEW') pending FROM products WHERE source_code='danawa'")
            result['mysql']={k:int(v or 0) for k,v in counts.items()}
        finally:db.close()
    print(json.dumps(result));return 0


if __name__=='__main__':raise SystemExit(main())
