# Massas de erro de ingestão

Estes arquivos são usados pelos testes locais em `tests/test_ingestion_contract.py`.
Eles representam o objeto original que chega ao Bronze; os metadados S3 e o evento
SQS são construídos pelo teste para simular três rejeições permanentes:

- `metadados_ausentes.csv`: ausência de `expected-size` no objeto versionado.
- `tamanho_divergente.csv`: `expected-size` não coincide com `ContentLength`.
- `hash_divergente.csv`: SHA-256 informado é diferente dos bytes do objeto.

Cada caso deve gerar cópia no bucket Quarantine e estado `REJECTED` no ledger, sem
reentrega à SQS nem envio à DLQ. Esses arquivos não devem ser enviados ao pipeline
produtivo como carga válida.
