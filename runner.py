"""Orquestrador da Aula 25, inspirado no runner da Aula 24.

Uso: python runner.py initial|delta|replay|verify|trace-gold|snapshots|fail-demo|questions|cleanup
"""
import argparse
import hashlib
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
import boto3

ROOT=Path(__file__).resolve().parent
TF=ROOT/'terraform'
EVID=ROOT/'evidencias'; EVID.mkdir(exist_ok=True)
parser=argparse.ArgumentParser()
parser.add_argument('action',choices=['initial','delta','replay','verify','trace-gold','snapshots','fail-demo','questions','cleanup'])
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
s3=session.client('s3');states=session.client('stepfunctions');athena=session.client('athena')


def upload(stage):
    origin=ROOT/'dados'/stage
    refs=[]
    for file in sorted(origin.rglob('*')):
        if not file.is_file(): continue
        key=file.relative_to(origin).as_posix()
        data=file.read_bytes()
        sha=hashlib.sha256(data).hexdigest()
        put=s3.put_object(Bucket=o['buckets']['bronze'],Key=key,Body=data,Metadata={'sha256':sha})
        refs.append({'s3_key':key,'sha256':sha,'version_id':put.get('VersionId')})
    manifest_key=f'manifestos/bronze/{stage}-{uuid.uuid4().hex}.json'
    s3.put_object(Bucket=o['buckets']['results'],Key=manifest_key,
                  Body=json.dumps({'stage':stage,'files':refs}).encode('utf-8'),ContentType='application/json')
    evidence('bronze',{'manifest':manifest_key,'files':refs})
    return manifest_key


def start_pipeline(bronze_manifest, expect_failure=False):
    run_id=uuid.uuid4().hex
    execution=states.start_execution(
        stateMachineArn=o['state_machine_arn'],
        name=f"run-{run_id}",
        input=json.dumps({'glue_arguments':{'--RUN_ID':run_id,
                                            '--BRONZE_MANIFEST':bronze_manifest}}))
    arn=execution['executionArn']
    for _ in range(240):
        info=states.describe_execution(executionArn=arn)
        state=info['status']
        if state in ('SUCCEEDED','FAILED','TIMED_OUT','ABORTED'):
            evidence('step-functions',{'execution_arn':arn,'run_id':run_id,
                                       'bronze_manifest':bronze_manifest,'status':state,
                                       'output':info.get('output'),'error':info.get('error'),
                                       'cause':info.get('cause')})
            if expect_failure and state!='SUCCEEDED':
                print('Falha controlada confirmada. Verifique snapshots anteriores.')
                return
            if state!='SUCCEEDED': raise RuntimeError(f"Step Functions: {state}: {info.get('cause')}")
            if expect_failure: raise AssertionError('Falha esperada nao ocorreu')
            return
        time.sleep(5)
    raise TimeoutError(f'Execucao excedeu 20 min: {arn}')


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
    assert silver['run_id']==gold['run_id']==run_id
    assert len(gold['iceberg_metadata_locations'])==7
    assert len(silver['iceberg_metadata_locations'])==7
    glue=session.client('glue')
    for layer,manifest in [('gold',gold),('silver',silver)]:
        for table,location in manifest['iceberg_metadata_locations'].items():
            assert location and location.startswith('s3://'), f'{layer}.{table}: metadata_location ausente'
            current=glue.get_table(DatabaseName=o['database'],Name=table)['Table']['Parameters']['metadata_location']
            assert current==location, f'{layer}.{table}: ponteiro Iceberg divergente do catálogo'
    bronze={obj['key']:obj for obj in silver['bronze_objects']}
    checked=[]
    for uri in sorted(source_uris):
        parsed=urlparse(uri)
        assert parsed.scheme in ('s3','s3a') and parsed.netloc==o['buckets']['bronze'], uri
        key=parsed.path.lstrip('/')
        assert key in bronze, f'{uri} não consta no manifesto Silver'
        obj=bronze[key]
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


if a.action=='initial':
    start_pipeline(upload('inicial'));verify('249.70');snapshots()
elif a.action=='delta':
    start_pipeline(upload('delta'));verify('198.80');snapshots()
elif a.action=='replay':
    start_pipeline(upload('delta'));verify('198.80');snapshots()
elif a.action=='verify':verify()
elif a.action=='trace-gold':trace_gold()
elif a.action=='snapshots':snapshots()
elif a.action=='fail-demo':
    # Arquivo invalido e removido por delete marker ao fim; Bronze conserva a versao auditavel.
    key=f'pagamentos/falha-{uuid.uuid4().hex}.jsonl'
    data=(ROOT/'dados'/'falha_pagamento.jsonl').read_bytes()
    sha=hashlib.sha256(data).hexdigest()
    put=s3.put_object(Bucket=o['buckets']['bronze'],Key=key,Body=data,Metadata={'sha256':sha})
    manifest_key=f'manifestos/bronze/falha-{uuid.uuid4().hex}.json'
    s3.put_object(Bucket=o['buckets']['results'],Key=manifest_key,
                  Body=json.dumps({'stage':'falha','files':[{'s3_key':key,'sha256':sha,
                  'version_id':put.get('VersionId')}]}).encode('utf-8'),ContentType='application/json')
    try:
        start_pipeline(manifest_key,expect_failure=True)
    finally:
        s3.delete_object(Bucket=o['buckets']['bronze'],Key=key)
    verify()

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
