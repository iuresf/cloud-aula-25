#!/usr/bin/env python3
"""Executa na AWS os envios válido e inválidos da ingestão."""
import base64, hashlib, json, subprocess, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
import boto3

ROOT=Path(__file__).resolve().parents[1]; TF=ROOT/'terraform'; EVID=ROOT/'evidencias'
def outputs():
    raw=json.loads(subprocess.check_output(['terraform','output','-json'],cwd=TF))
    return {k:v['value'] for k,v in raw.items()}
def wait(check, label, timeout=120):
    until=time.time()+timeout
    while time.time()<until:
        result=check()
        if result:return result
        time.sleep(3)
    raise TimeoutError(label)
def request_json(url, payload, headers):
    req=Request(url,data=json.dumps(payload).encode(),headers=headers,method='POST')
    with urlopen(req,timeout=30) as response:return response.status,json.loads(response.read())
def upload(url, body, headers):
    req=Request(url,data=body,headers=headers,method='PUT')
    with urlopen(req,timeout=30) as response:return response.status

o=outputs(); session=boto3.Session(region_name=o['region']); cognito=session.client('cognito-idp'); s3=session.client('s3'); ddb=session.client('dynamodb'); sqs=session.client('sqs'); logs=session.client('logs'); sfn=session.client('stepfunctions')
bronze=o['buckets']['bronze']; quarantine=o['buckets']['quarantine']; table='a25-iceberg-ingestion-ledger'; started=int(time.time()*1000)
pool_id=next(p['Id'] for p in cognito.list_user_pools(MaxResults=60)['UserPools'] if p['Name']=='a25-iceberg-ingestors')
client_id=cognito.list_user_pool_clients(UserPoolId=pool_id,MaxResults=60)['UserPoolClients'][0]['ClientId']
client=cognito.describe_user_pool_client(UserPoolId=pool_id,ClientId=client_id)['UserPoolClient']
basic=base64.b64encode(f"{client_id}:{client['ClientSecret']}".encode()).decode()
token_req=Request(o['cognito_token_url'],data=b'grant_type=client_credentials',headers={'Authorization':f'Basic {basic}','Content-Type':'application/x-www-form-urlencoded'},method='POST')
with urlopen(token_req,timeout=30) as response: token=json.loads(response.read())['access_token']
body=(ROOT/'dados/erros/tamanho_divergente.csv').read_bytes(); digest=hashlib.sha256(body).hexdigest()
def api_session(name,size):
    return request_json(o['ingestion_api_url']+'/ingestions',{'file_name':name,'content_type':'text/csv','source_system':'test','schema_version':'1','size_bytes':size,'sha256':digest},{'Authorization':f'Bearer {token}','Content-Type':'application/json'})
def object_version(key):
    versions=s3.list_object_versions(Bucket=bronze,Prefix=key).get('Versions',[])
    return versions[0]['VersionId'] if versions else None
# Caminho válido via API: confirma Bronze/ledger e interrompe SFN antes de Glue em lote parcial.
status,valid=api_session('valido.csv',len(body)); assert status==201; assert upload(valid['upload_url'],body,valid['required_headers']) in (200,204)
def valid_ledger():
    version=object_version(valid['s3_key'])
    if not version:return None
    item=ddb.get_item(TableName=table,Key={'object_version':{'S':f"{bronze}/{valid['s3_key']}/{version}"}}).get('Item')
    return (item,version) if item and item.get('status',{}).get('S')=='ORCHESTRATED' else None
valid_item,valid_version=wait(valid_ledger,'ledger válido'); execution=valid_item.get('execution_arn',{}).get('S')
if execution:
    try:sfn.stop_execution(executionArn=execution,cause='teste de ingestão sem executar Glue com lote parcial')
    except sfn.exceptions.ExecutionDoesNotExist:pass
# Erro de tamanho pela API.
status,size_case=api_session('tamanho-divergente.csv',len(body)+17); assert status==201; assert upload(size_case['upload_url'],body,size_case['required_headers']) in (200,204)
# Erros de metadados e hash: objetos diretos no prefixo Bronze para exercitar Receipt.
direct=[]
for name,metadata in [('metadados-ausentes.csv',{'ingestion-id':str(uuid.uuid4()),'sha256':digest,'source-system':'test','schema-version':'1'}),('hash-divergente.csv',{'ingestion-id':str(uuid.uuid4()),'sha256':'0'*64,'source-system':'test','schema-version':'1','expected-size':str(len(body))})]:
    key=f'ingestion/test/{uuid.uuid4()}/{name}'; put=s3.put_object(Bucket=bronze,Key=key,Body=body,ContentType='text/csv',Metadata=metadata); direct.append({'name':name,'key':key,'version':put['VersionId']})
cases=[{'name':'tamanho-divergente.csv','key':size_case['s3_key']},*direct]
def rejected():
    result=[]
    for case in cases:
        version=case.get('version') or object_version(case['key'])
        if not version:return None
        item=ddb.get_item(TableName=table,Key={'object_version':{'S':f"{bronze}/{case['key']}/{version}"}}).get('Item')
        if not item or item.get('status',{}).get('S')!='REJECTED':return None
        result.append({'name':case['name'],'key':case['key'],'version_id':version,'ledger_status':item['status']['S'],'reason':item['rejection_reason']['S']})
    return result
rejections=wait(rejected,'três rejeições no ledger')
quarantine_items=s3.list_objects_v2(Bucket=quarantine,Prefix='rejected/').get('Contents',[]); assert len(quarantine_items)>=3
def rejection_logs():
    return logs.filter_log_events(logGroupName='/aws/lambda/a25-iceberg-receipt',startTime=started,filterPattern='ingestion_rejected').get('events',[])
log_events=wait(rejection_logs,'logs da Receipt')
def dlq_count(name):
    url=sqs.get_queue_url(QueueName=name)['QueueUrl']; return sqs.get_queue_attributes(QueueUrl=url,AttributeNames=['ApproximateNumberOfMessages'])['Attributes']['ApproximateNumberOfMessages']
report={'tested_at':datetime.now(timezone.utc).isoformat(),'valid_api':{'ingestion_id':valid['ingestion_id'],'s3_key':valid['s3_key'],'version_id':valid_version,'ledger_status':valid_item['status']['S'],'execution_stopped':bool(execution)},'rejections':rejections,'quarantine_object_count':len(quarantine_items),'object_events_dlq_messages':dlq_count('a25-iceberg-object-events-dlq'),'ready_dlq_messages':dlq_count('a25-iceberg-ready-dlq'),'receipt_rejection_log_count':len(log_events)}
EVID.mkdir(exist_ok=True); path=EVID/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-ingestao-aws.json"; path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps({**report,'evidence':str(path)},ensure_ascii=False,indent=2))
