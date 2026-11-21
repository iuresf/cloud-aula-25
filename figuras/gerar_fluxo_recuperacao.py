#!/usr/bin/env python3
"""Diagrama atualizado com os ícones AWS já presentes no repositório."""
from base64 import b64encode
from html import escape
from pathlib import Path

root = Path(__file__).resolve().parent
icons = {name: "data:image/svg+xml;base64," + b64encode((root / "icones" / f"{name}.svg").read_bytes()).decode()
         for name in ("api-gateway", "s3", "sqs", "lambda", "dynamodb", "step-functions", "glue", "athena", "eventbridge")}
parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="920" viewBox="0 0 1600 920">',
         '<rect width="1600" height="920" fill="#f5f8fc"/>',
         '<text x="50" y="55" font-family="sans-serif" font-size="31" font-weight="bold" fill="#18324a">Lakehouse recuperável · Aula 25</text>',
         '<text x="50" y="84" font-family="sans-serif" font-size="17" fill="#446078">Setas azuis: lote normal. Laranjas: recuperação. Publicação: um ponteiro para sete snapshots.</text>']


def box(x, y, title, detail, icon):
    parts.append(f'<rect x="{x}" y="{y}" width="320" height="108" rx="15" fill="white" stroke="#9ab5ce" stroke-width="2"/>')
    parts.append(f'<image x="{x+14}" y="{y+17}" width="70" height="70" href="{icons[icon]}"/>')
    parts.append(f'<text x="{x+95}" y="{y+42}" font-family="sans-serif" font-size="19" font-weight="bold" fill="#18324a">{escape(title)}</text>')
    parts.append(f'<text x="{x+95}" y="{y+72}" font-family="sans-serif" font-size="14" fill="#446078">{escape(detail)}</text>')


def arrow(x1, y1, x2, y2, color="#1684a7"):
    parts.append(f'<path d="M{x1} {y1} L{x2} {y2}" stroke="{color}" stroke-width="4" fill="none" marker-end="url(#{"blue" if color == "#1684a7" else "orange"})"/>')

parts.append('<defs><marker id="blue" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto"><path d="M0 0 L10 5 L0 10" fill="#1684a7"/></marker><marker id="orange" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto"><path d="M0 0 L10 5 L0 10" fill="#d88b1f"/></marker></defs>')
for args in [
    (50, 130, "API / runner", "batch_id e seis contratos", "api-gateway"),
    (440, 130, "Bronze versionado", "VersionId + SHA-256 por arquivo", "s3"),
    (830, 130, "Receipt + lotes", "DynamoDB; finalização explícita", "dynamodb"),
    (1220, 130, "SQS ready", "somente lote completo", "sqs"),
    (1220, 350, "Step Functions", "rota Silver ou Gold", "step-functions"),
    (830, 350, "Glue Silver", "bytes da versão; manifesto", "glue"),
    (440, 350, "Glue Gold", "snapshots Silver fixados", "glue"),
    (50, 350, "Publish", "sete snapshots → um ponteiro", "lambda"),
    (50, 590, "Athena", "FOR VERSION AS OF", "athena"),
    (440, 590, "EventBridge", "eventos + reconciliação", "eventbridge"),
    (830, 590, "Ledger operacional", "run_id, attempt_id, recovery_of", "dynamodb"),
    (1220, 590, "recover.py", "inspecionar, retomar, rollback", "lambda"),
]: box(*args)
for x in (370, 760, 1150): arrow(x, 184, x+65, 184)
arrow(1380, 240, 1380, 344)
for x in (1220, 830, 440): arrow(x, 404, x-65, 404)
arrow(210, 460, 210, 584)
arrow(600, 590, 600, 468, "#d88b1f")
arrow(990, 590, 990, 468, "#d88b1f")
arrow(1380, 590, 1380, 468, "#d88b1f")
parts.append('<text x="50" y="820" font-family="sans-serif" font-size="17" fill="#446078">Falha Gold: recuperar só Gold usando snapshot Silver original. Falha Silver: novo manifesto Bronze corrigido.</text>')
parts.append('<text x="50" y="854" font-family="sans-serif" font-size="17" fill="#446078">Leitura direta do catálogo não integra o contrato de publicação consistente.</text>')
parts.append('</svg>')
(root / 'fluxo-recuperacao.svg').write_text('\n'.join(parts)+'\n', encoding='utf-8')
