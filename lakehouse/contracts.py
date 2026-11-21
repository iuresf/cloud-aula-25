"""Contrato versionado para lotes Bronze; sem dependências AWS."""
import re

DATASETS = frozenset({'pedidos', 'itens', 'clientes', 'produtos', 'pagamentos', 'eventos'})
INITIAL = DATASETS
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
IDENT = re.compile(r'[A-Za-z0-9_-]{2,64}\Z')


def dataset_for_key(key):
    parts = key.split('/')
    if parts[0] in DATASETS:
        return parts[0]
    if parts[0] == 'ingestion' and len(parts) >= 3 and parts[1] in DATASETS:
        return parts[1]
    raise ValueError(f'dataset_type ausente ou inválido: {key}')


def validate_manifest(manifest):
    if not isinstance(manifest, dict) or manifest.get('layer') != 'bronze':
        raise ValueError('manifesto Bronze inválido')
    if manifest.get('operation') not in ('initial', 'incremental'):
        raise ValueError('operação inválida')
    if not IDENT.fullmatch(str(manifest.get('batch_id', ''))):
        raise ValueError('batch_id inválido')
    files = manifest.get('files')
    if not isinstance(files, list) or not files:
        raise ValueError('lote sem arquivos')
    seen = set()
    for item in files:
        if not isinstance(item, dict):
            raise ValueError('arquivo inválido')
        key = item.get('s3_key')
        if not isinstance(key, str) or not key or key.startswith('/') or '..' in key.split('/'):
            raise ValueError('s3_key inválido')
        dataset = item.get('dataset_type') or dataset_for_key(key)
        if dataset not in DATASETS or dataset_for_key(key) != dataset:
            raise ValueError('dataset_type incompatível com chave')
        if not item.get('version_id') or not HEX64.fullmatch(str(item.get('sha256', ''))):
            raise ValueError('versão ou SHA-256 ausente')
        if (key, item['version_id']) in seen:
            raise ValueError('objeto duplicado no lote')
        seen.add((key, item['version_id']))
    present = {f.get('dataset_type') or dataset_for_key(f['s3_key']) for f in files}
    if manifest['operation'] == 'initial' and present != INITIAL:
        raise ValueError(f'lote inicial incompleto: {sorted(INITIAL - present)}')
    return present
