"""Validacao offline dos dados de entrada: nao requer AWS/PySpark."""
import csv
import json
from decimal import Decimal
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def rows(dataset):
    with (ROOT/'dados/inicial'/dataset/'lote_001.csv').open(encoding='utf-8') as f:
        return list(csv.DictReader(f,delimiter=';'))

def test_grain_totals():
    pedidos={x['pedido_id']:x for x in rows('pedidos')}
    assert len(pedidos)==4  # P1042 duplicado na Bronze
    itens=rows('itens')
    assert len(itens)==len({i['item_id'] for i in itens})==5
    totais={}
    for item in itens:
        totais[item['pedido_id']]=totais.get(item['pedido_id'],Decimal('0'))+Decimal(item['quantidade'])*Decimal(item['preco_unitario'].replace(',','.'))
    assert totais['P1042']==Decimal('129.90')
    assert totais['P1044']==Decimal('119.80')
    assert totais['P1045']==Decimal('79.00')

def test_payment_history():
    linhas=[]
    for path in [ROOT/'dados/inicial/pagamentos/lote_001.jsonl',ROOT/'dados/delta/pagamentos/lote_002.jsonl']:
        linhas.extend(json.loads(line) for line in path.read_text().splitlines())
    assert len(linhas)==len({x['evento_pagamento_id'] for x in linhas})==6
    assert [x['resultado'] for x in linhas if x['pedido_id']=='P1042']==['aprovado','estornado']

if __name__=='__main__':
    test_grain_totals();test_payment_history();print('Dados validados offline')


def test_receita_por_estado_de_pagamentos():
    pedidos={x['pedido_id']:x for x in rows('pedidos')}
    itens=rows('itens')
    def receita(paths):
        history={}
        for path in paths:
            for event in (json.loads(line) for line in path.read_text().splitlines()):
                current=history.get(event['pedido_id'])
                if current is None or (event['atualizado_em'],event['evento_pagamento_id'])>(current['atualizado_em'],current['evento_pagamento_id']):
                    history[event['pedido_id']]=event
        return sum((Decimal(item['quantidade'])*Decimal(item['preco_unitario'].replace(',','.'))
                    for item in itens if pedidos[item['pedido_id']]['status']=='pago'
                    and history.get(item['pedido_id'],{}).get('resultado')=='aprovado'),Decimal('0'))
    initial=ROOT/'dados/inicial/pagamentos/lote_001.jsonl'
    delta=ROOT/'dados/delta/pagamentos/lote_002.jsonl'
    assert receita([initial])==Decimal('249.70')
    assert receita([initial,delta])==Decimal('198.80')
