"""Cliente didático da Aula 25.

Envia lotes pela API de ingestão e consulta resultados já produzidos na AWS.
Uso: python runner.py initial|delta|replay|fail-demo|status|verify|trace-gold|snapshots|quality|questions|cleanup
"""
import argparse
import base64
import hashlib
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import boto3
from lakehouse.contracts import dataset_for_key

ROOT=Path(__file__).resolve().parent
TF=ROOT/'terraform'
EVID=ROOT/'evidencias'; EVID.mkdir(exist_ok=True)
parser=argparse.ArgumentParser()
parser.add_argument('action',choices=['initial','delta','replay','fail-demo','status','verify','trace-gold','snapshots','quality','questions','cleanup'])
parser.add_argument('--batch-id',help='Identificador do lote, exigido por status.')
a=parser.parse_args()


def outputs():
    raw=json.loads(subprocess.check_output(['terraform','output','-json'],cwd=TF))
    return {key:val['value'] for key,val in raw.items()}


def evidence(label,data):
    path=EVID/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{label}.json"
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return str(path)


if a.action=='cleanup':
    print('Revise os custos e execute: cd terraform && terraform plan -destroy && terraform destroy')
    raise SystemExit()

o=outputs()
session=boto3.Session(region_name=o['region'])
s3=session.client('s3');athena=session.client('athena');ddb=session.client('dynamodb')


def access_token():
    """Obtém token de máquina; credenciais permanecem fora do repositório."""
    client_id=os.environ.get('INGESTION_CLIENT_ID')
    client_secret=os.environ.get('INGESTION_CLIENT_SECRET')
    if not client_id or not client_secret:
        raise RuntimeError('Defina INGESTION_CLIENT_ID e INGESTION_CLIENT_SECRET antes de enviar um lote.')
    basic=base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    request=Request(o['cognito_token_url'],data=urlencode({'grant_type':'client_credentials'}).encode(),
                    headers={'Authorization':f'Basic {basic}','Content-Type':'application/x-www-form-urlencoded'},method='POST')
    with urlopen(request,timeout=30) as response:
        return json.loads(response.read())['access_token']


def api_post(payload, token):
    request=Request(o['ingestion_api_url'],data=json.dumps(payload).encode(),method='POST',
                    headers={'Authorization':f'Bearer {token}','Content-Type':'application/json'})
    try:
        with urlopen(request,timeout=30) as response:
            return response.status,json.loads(response.read())
    except HTTPError as exc:
        body=exc.read().decode()
        raise RuntimeError(f'API de ingestão retornou HTTP {exc.code}: {body}') from exc


def submit(stage, files=None):
    """Envia dados e finalize.json; a AWS cria o manifesto e orquestra."""
    origin=ROOT/'dados'/stage
    operation='initial' if stage=='inicial' else 'incremental'
    batch_id=f'batch_{uuid.uuid4().hex}'
    token=access_token()
    sent=[]
    candidates=sorted(origin.rglob('*')) if files is None else files
    for file in candidates:
        if not file.is_file(): continue
        data=file.read_bytes(); sha=hashlib.sha256(data).hexdigest()
        dataset='pagamentos' if stage=='falha' else dataset_for_key(file.relative_to(origin).as_posix())
        content_type='text/csv' if file.suffix.lower()=='.csv' else 'application/x-ndjson'
        status,upload=api_post({'batch_id':batch_id,'operation':operation,'dataset_type':dataset,
                                'file_name':file.name,'source_system':'runner','schema_version':'1',
                                'content_type':content_type,'size_bytes':len(data),'sha256':sha},token)
        if status != 201: raise RuntimeError(f'Sessão de upload inesperada: HTTP {status}')
        request=Request(upload['upload_url'],data=data,headers=upload['required_headers'],method='PUT')
        with urlopen(request,timeout=60) as response:
            if response.status not in (200,204): raise RuntimeError(f'Upload S3 inesperado: HTTP {response.status}')
        sent.append({'dataset_type':dataset,'file_name':file.name,'ingestion_id':upload['ingestion_id'],
                     's3_key':upload['s3_key'],'sha256':sha,'size_bytes':len(data)})
    control=json.dumps({'type':'batch_finalize','batch_id':batch_id,'operation':operation,'files':[
        {key:item[key] for key in ('dataset_type','s3_key','sha256','size_bytes')} for item in sent
    ]},sort_keys=True,separators=(',',':')).encode()
    control_sha=hashlib.sha256(control).hexdigest()
    status,upload=api_post({'batch_id':batch_id,'operation':operation,'dataset_type':'batch_finalize',
                            'file_name':'finalize.json','source_system':'runner','schema_version':'1',
                            'content_type':'application/json','size_bytes':len(control),'sha256':control_sha},token)
    if status != 201: raise RuntimeError(f'Sessão do finalize.json inesperada: HTTP {status}')
    with urlopen(Request(upload['upload_url'],data=control,headers=upload['required_headers'],method='PUT'),timeout=60) as response:
        if response.status not in (200,204): raise RuntimeError(f'Upload do finalize.json inesperado: HTTP {response.status}')
    evidence('uploads-submetidos',{'batch_id':batch_id,'operation':operation,'files':sent})
    print(json.dumps({'batch_id':batch_id,'status':'FINALIZE_UPLOADED','files':sent,
                      'next_step':f'python runner.py status --batch-id {batch_id}'},ensure_ascii=False,indent=2))
    return batch_id


def batch_status(batch_id):
    item=ddb.get_item(TableName=o['batches_table'],Key={'batch_id':{'S':batch_id},'record_id':{'S':'META'}},ConsistentRead=True).get('Item')
    if not item: raise RuntimeError(f'Lote inexistente: {batch_id}')
    result={key:value.get('S',value.get('N')) for key,value in item.items()}
    evidence('status-lote',result)
    print(json.dumps(result,ensure_ascii=False,indent=2))


def query(sql):
    response=athena.start_query_execution(QueryString=sql,QueryExecutionContext={'Database':o['database']},WorkGroup=o['workgroup'])
    qid=response['QueryExecutionId']
    for _ in range(180):
        info=athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']
        state=info['Status']['State']
        if state=='SUCCEEDED':
            paginator=athena.get_paginator('get_query_results')
            rows=[]
            for page in paginator.paginate(QueryExecutionId=qid):
                rows += [[x.get('VarCharValue','') for x in r.get('Data',[])] for r in page['ResultSet'].get('Rows',[])]
            evidence('athena',{'sql':sql,'query_execution_id':qid,'rows':rows,'statistics':info.get('Statistics')})
            return rows
        if state in ('FAILED','CANCELLED'):
            raise RuntimeError(str(info['Status']))
        time.sleep(2)
    raise TimeoutError(qid)


def verify(expected=None):
    rows=query('SELECT COUNT(*) AS itens, CAST(SUM(receita_liquida) AS DECIMAL(14,2)) AS receita FROM fato_itens_venda')
    itens=int(rows[1][0]); receita=Decimal(rows[1][1])
    assert itens==5,f'Itens esperados=5, encontrados={itens}'
    assert receita in (Decimal('249.70'),Decimal('198.80')),f'Receita nao prevista: {receita}'
    if expected is not None: assert receita==Decimal(expected),f'Esperado {expected}, encontrado {receita}'
    assert query('SELECT COUNT(*) FROM fato_itens_venda WHERE item_id IS NULL')[1][0]=='0'
    assert query('SELECT COUNT(*) FROM fato_itens_venda WHERE origem_item IS NULL OR origem_pedido IS NULL OR origem_cliente IS NULL')[1][0]=='0'
    assert query('SELECT COUNT(*) FROM historico_pagamentos')[1][0] in ('4','6')
    evidence('validacao',{'itens':itens,'receita':str(receita)})
    print(f'VALIDADO: {itens} itens; receita total R$ {receita}')


def snapshots():
    for table in ('pagamentos','historico_pagamentos','fato_itens_venda','dim_cliente'):
        print('\nSNAPSHOTS',table)
        for row in query(f'SELECT committed_at, snapshot_id, operation FROM "{table}$snapshots" ORDER BY committed_at'):
            print(' | '.join(row))


def trace_gold():
    """Confere cada linha Gold até a versão e o hash do objeto Bronze."""
    origins={
        'dim_tempo':['origem_arquivos'],
        'dim_cliente':['origem_cliente'],
        'dim_produto':['origem_produto'],
        'dim_canal':['origem_arquivos'],
        'dim_regiao':['origem_arquivos'],
        'fato_itens_venda':['origem_item','origem_pedido','origem_cliente','origem_produto','origem_pagamento'],
        'fato_sessoes':['origem_eventos'],
    }
    keys={'dim_tempo':'sk_tempo','dim_cliente':'sk_cliente','dim_produto':'sk_produto',
          'dim_canal':'sk_canal','dim_regiao':'sk_regiao',
          'fato_itens_venda':'item_id','fato_sessoes':'sk_sessao'}
    rows_by_table={}
    gold_rows={}
    runs=set()
    source_uris=set()
    for table,columns in origins.items():
        result=query(f"SELECT {keys[table]}, etl_run_id, {', '.join(columns)} FROM {table}")
        assert len(result)>1, f'{table} vazia'
        for row in result[1:]:
            assert row[0] and row[1], f'{table}: chave ou run_id ausente'
            runs.add(row[1])
            for column,value in zip(columns,row[2:]):
                if not value:
                    assert column=='origem_pagamento', f'{table}: {column} ausente'
                    continue
                source_uris.update(value.split(',') if column in ('origem_arquivos','origem_eventos') else [value])
        rows_by_table[table]=len(result)-1
        gold_rows[table]=[dict(zip([keys[table],'etl_run_id',*columns],row)) for row in result[1:]]
    assert len(runs)==1, f'Gold contém mais de um run_id: {runs}'
    run_id=runs.pop()
    results_bucket=o['buckets']['results']
    gold=json.loads(s3.get_object(Bucket=results_bucket,Key=f'manifestos/gold/{run_id}.json')['Body'].read())
    silver_uri=gold['silver_operational_manifest']
    parsed=urlparse(silver_uri)
    assert parsed.scheme=='s3' and parsed.netloc==results_bucket
    silver=json.loads(s3.get_object(Bucket=results_bucket,Key=parsed.path.lstrip('/'))['Body'].read())
    assert gold['run_id']==run_id and silver['run_id']==gold.get('source_silver_run_id',run_id)
    assert len(gold['iceberg_metadata_locations'])==7
    assert len(silver['iceberg_metadata_locations'])==7
    for layer,manifest in [('gold',gold),('silver',silver)]:
        for table,location in manifest['iceberg_metadata_locations'].items():
            assert location and location.startswith('s3://'), f'{layer}.{table}: metadata_location ausente'
            assert manifest.get('snapshot_ids',{}).get(table), f'{layer}.{table}: snapshot ausente'
    bronze={(obj['key'],obj['version_id']):obj for obj in silver['bronze_objects']}
    checked=[]
    for uri in sorted(source_uris):
        parsed=urlparse(uri)
        assert parsed.scheme in ('s3','s3a') and parsed.netloc==o['buckets']['bronze'], uri
        key=parsed.path.lstrip('/')
        version=parse_qs(parsed.query).get('versionId',[None])[0]
        candidates=[obj for (candidate_key,candidate_version),obj in bronze.items() if candidate_key==key and (version is None or candidate_version==version)]
        assert len(candidates)==1, f'{uri}: versão ambígua ou ausente no manifesto Silver'
        obj=candidates[0]
        assert obj['version_id'] and obj['sha256'], f'{key}: versão ou hash ausente'
        response=s3.get_object(Bucket=o['buckets']['bronze'],Key=key,VersionId=obj['version_id'])
        actual=hashlib.sha256(response['Body'].read()).hexdigest()
        assert actual==obj['sha256']==response['Metadata']['sha256'], f'{key}: SHA-256 divergente'
        checked.append({'uri':uri,'version_id':obj['version_id'],'sha256':actual})
    report={'run_id':run_id,'gold_manifest':f's3://{results_bucket}/manifestos/gold/{run_id}.json',
            'silver_manifest':silver_uri,'rows_by_table':rows_by_table,'gold_rows':gold_rows,
            'gold_manifest_content':gold,'silver_manifest_content':silver,
            'verified_bronze_objects':checked}
    path=evidence('rastreabilidade-gold',report)
    print(f'RASTREABILIDADE VALIDADA: {sum(rows_by_table.values())} linhas Gold, '
          f'{len(checked)} objetos Bronze, 7 tabelas. Evidência: {path}')


def quality():
    """Exibe somente resultados produzidos pelo Glue, sem valores esperados fixos."""
    pointer=ddb.get_item(TableName=o['operations_table'],Key={'run_id':{'S':'PUBLISHED'}},ConsistentRead=True).get('Item',{})
    run_id=pointer.get('published_run_id',{}).get('S')
    if not run_id: raise RuntimeError('Nenhuma versão PUBLISHED')
    gold=json.loads(s3.get_object(Bucket=o['buckets']['results'],Key=f'manifestos/gold/{run_id}.json')['Body'].read())
    silver_uri=gold['silver_operational_manifest']; parsed=urlparse(silver_uri)
    silver=json.loads(s3.get_object(Bucket=parsed.netloc,Key=parsed.path.lstrip('/'))['Body'].read())
    report_uri=gold.get('reconciliation_uri')
    report={}
    if report_uri:
        parsed=urlparse(report_uri); report=json.loads(s3.get_object(Bucket=parsed.netloc,Key=parsed.path.lstrip('/'))['Body'].read())
    result={'run_id':run_id,'silver_run_id':silver.get('run_id'),'quality_status':silver.get('quality_status','LEGACY_UNKNOWN'),
            'quality_summary':silver.get('quality_summary'),'quality_report_uri':silver.get('quality_report_uri'),
            'anomalies_uri':silver.get('anomalies_uri'),'reconciliation_status':report.get('status','LEGACY_UNKNOWN'),
            'reconciliation_uri':report_uri,'silver_snapshot_ids':silver.get('snapshot_ids'),'gold_snapshot_ids':gold.get('snapshot_ids')}
    evidence('quality',result)
    print(json.dumps(result,ensure_ascii=False,indent=2,default=str))


if a.action=='initial':
    submit('inicial')
elif a.action=='delta':
    submit('delta')
elif a.action=='replay':
    submit('delta')
elif a.action=='status':
    if not a.batch_id: parser.error('--batch-id é obrigatório para status')
    batch_status(a.batch_id)
elif a.action=='verify':verify()
elif a.action=='trace-gold':trace_gold()
elif a.action=='snapshots':snapshots()
elif a.action=='quality':quality()
elif a.action=='fail-demo':
    submit('falha',[ROOT/'dados'/'falha_pagamento.jsonl'])

elif a.action=='questions':
    queries={
      'estrela': (ROOT/'sql/01_estrela_receita.sql').read_text().split(';')[0],
      'eventos': (ROOT/'sql/02_eventos_passados.sql').read_text().split(';')[0],
      'conversao': (ROOT/'sql/05_conversao_grao.sql').read_text().split(';')[0],
      'idempotencia': (ROOT/'sql/04_acid_idempotencia.sql').read_text().split(';')[0],
      'snapshots_fato': 'SELECT committed_at, snapshot_id, operation FROM "fato_itens_venda$snapshots" ORDER BY committed_at',
      'atual_p1042': "SELECT item_id, resultado, receita_liquida FROM fato_itens_venda WHERE pedido_id='P1042' ORDER BY item_id",
      'origem_p1042': "SELECT item_id, etl_run_id, origem_item, origem_pedido, origem_cliente, origem_produto, origem_pagamento FROM fato_itens_venda WHERE pedido_id='P1042' ORDER BY item_id",
    }
    for name,statement in queries.items():
        print(name,query(statement))
    versions=query(queries['snapshots_fato'])
    first=versions[1][1]
    print('anterior_p1042',query("SELECT item_id, resultado, receita_liquida FROM fato_itens_venda FOR VERSION AS OF "+first+" WHERE pedido_id='P1042' ORDER BY item_id"))
