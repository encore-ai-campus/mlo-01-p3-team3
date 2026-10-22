"""Read only new append files in a pinned, unpartitioned Bronze version range.

Unknown operations/schema changes use the original snapshot anti-join. No
offset or source observation is inferred from a missing/unreadable log.
"""
from urllib.parse import urlsplit,unquote
from .spark_session import bump


def validate_actions(actions,expected_commits,root):
    commits=[];files=[]
    for row in actions:
        row=row.asDict(recursive=True) if hasattr(row,'asDict') else row
        if row.get('metaData') or row.get('protocol'):return None
        removed=row.get('remove')
        if removed and removed.get('dataChange') is not False:return None
        commit=row.get('commitInfo')
        if commit:
            if commit.get('operation')=='WRITE':
                if (commit.get('operationParameters') or {}).get('mode','').lower()!='append':return None
            elif commit.get('operation')!='OPTIMIZE':return None
            commits.append(commit)
        added=row.get('add')
        if not added:continue
        if added.get('dataChange') is False:continue
        if added.get('dataChange') is not True or added.get('partitionValues'):return None
        path=unquote(added.get('path') or '');parsed=urlsplit(path)
        if (not path or parsed.scheme or parsed.netloc or path.startswith('/') or '\\' in path
                or any(p in {'','..','.'} for p in path.split('/'))):return None
        files.append(root.rstrip('/')+'/'+path)
    if len(commits)!=expected_commits or len(files)!=len(set(files)):return None
    return files


def append_files(spark,root,base,version):
    if base<0 or version<base or version-base>64:return None
    if version==base:return []
    schema=('add struct<path:string,dataChange:boolean,partitionValues:map<string,string>>,'
            'remove struct<dataChange:boolean>,metaData struct<id:string>,protocol struct<minReaderVersion:int>,'
            'commitInfo struct<operation:string,operationParameters:map<string,string>>')
    paths=[root.rstrip('/')+'/_delta_log/%020d.json'%v for v in range(base+1,version+1)]
    rows=spark.read.schema(schema).json(paths).limit(4097).collect()
    if len(rows)>4096:return None
    files=validate_actions(rows,version-base,root)
    if files is not None:bump('bronze_range_files',len(files));bump('bronze_range_file_reads')
    return files
