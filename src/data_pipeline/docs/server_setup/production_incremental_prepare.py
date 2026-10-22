"""Explicit Master-side index preparation. Default: inspect only."""
import argparse
import json
import os
from pathlib import Path
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--production-root',required=True,type=Path)
    parser.add_argument('--mongo-database',required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();root=args.production_root.resolve()
    sys.path.insert(0,str(root));sys.path.insert(0,str(root/'source'))
    import production_work
    production_work.setup()
    from pymongo import MongoClient,ReadPreference
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
    client=MongoClient(os.environ['PIPELINE_MONGO_URI'],retryWrites=False,serverSelectionTimeoutMS=5000)
    try:
        collection=client.get_database(args.mongo_database,read_preference=ReadPreference.PRIMARY,
            read_concern=ReadConcern('majority'),write_concern=WriteConcern(w='majority',j=True,wtimeout=30000)).ingestion_observations
        name='pipeline_incremental_pending_v1';saved=collection.index_information().get(name)
        matches=bool(saved and saved.get('key')==[('incremental_change',1)] and saved.get('sparse') is True)
        if saved and not matches:raise RuntimeError('INCREMENTAL_INDEX_CONTRACT_CHANGED')
        if args.apply and not saved:
            collection.create_index([('incremental_change',1)],name=name,sparse=True)
            saved=collection.index_information().get(name)
            matches=bool(saved and saved.get('key')==[('incremental_change',1)] and saved.get('sparse') is True)
        print(json.dumps(dict(status='READY' if matches else 'INDEX_REQUIRED',applied=args.apply,
                              database=args.mongo_database,index=name,raw_documents_changed=0)))
        return 0 if matches else 2
    finally:client.close()


if __name__=='__main__':raise SystemExit(main())
