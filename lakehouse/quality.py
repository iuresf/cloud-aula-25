"""Contratos puros de qualidade compartilhados por testes e jobs Glue.

Os identificadores são determinísticos e nunca usam IDs de partição Spark.
"""
import hashlib
import json

QUALITY_SCHEMA_VERSION = "1"
TECHNICAL_COLUMNS = frozenset({"origem_arquivo", "source_position", "record_hash", "occurrence_id"})


def canonical_values(values):
    """Representação estável dos valores de negócio, sem proveniência técnica."""
    return json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def record_hash(values):
    return hashlib.sha256(canonical_values(values).encode("utf-8")).hexdigest()


def occurrence_id(dataset_type, origin_uri, position, values):
    """ID estável por ocorrência física; posição ausente é declarada, nunca inventada."""
    position_value = "unknown" if position is None else str(position)
    raw = "|".join((dataset_type, origin_uri or "", position_value, record_hash(values)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def duplicate_decision(rows, key, business_columns):
    """Classifica ocorrências já normalizadas de uma chave, para testes unitários.

    A menor tupla ``(origem, posição, occurrence_id)`` vence empates idênticos.
    Conteúdo distinto para uma chave é conflito e não seleciona vencedor.
    """
    normalized = []
    for row in rows:
        values = {column: row.get(column) for column in business_columns}
        item = dict(row)
        item["record_hash"] = record_hash(values)
        item["occurrence_id"] = occurrence_id(row.get("dataset_type", ""), row.get("origem_arquivo", ""), row.get("source_position"), values)
        normalized.append(item)
    groups = {}
    for row in normalized:
        groups.setdefault(row[key], []).append(row)
    decisions = []
    for business_key, group in groups.items():
        hashes = {row["record_hash"] for row in group}
        if len(hashes) > 1:
            decisions.extend({"category": "KEY_CONFLICT", "business_key": business_key, "row": row} for row in group)
            continue
        winner = min(group, key=lambda r: (r.get("origem_arquivo") or "", r.get("source_position") is None, r.get("source_position") or 0, r["occurrence_id"]))
        for row in group:
            decisions.append({"category": "ACCEPTED" if row is winner else "DUPLICATE_IDENTICAL",
                              "business_key": business_key, "row": row,
                              "kept_occurrence_id": winner["occurrence_id"]})
    return decisions
