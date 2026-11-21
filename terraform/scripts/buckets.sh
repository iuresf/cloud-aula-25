#!/usr/bin/env bash
set -euo pipefail
mode="$1"; bucket="$2"; region="$3"
if [[ "$mode" == create ]]; then
  if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    if [[ "$region" == us-east-1 ]]; then
      aws s3api create-bucket --bucket "$bucket" --region "$region"
    else
      aws s3api create-bucket --bucket "$bucket" --region "$region" --create-bucket-configuration "LocationConstraint=$region"
    fi
  fi
  aws s3api put-public-access-block --bucket "$bucket" --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
  aws s3api put-bucket-encryption --bucket "$bucket" --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
  if [[ "$bucket" == *-bronze-* || "$bucket" == *-quarantine-* || "$bucket" == *-audit-* ]]; then
    aws s3api put-bucket-versioning --bucket "$bucket" --versioning-configuration Status=Enabled
  fi
elif [[ "$mode" == destroy ]]; then
  # Excluir versoes da Bronze e objetos dos demais buckets; requer jq.
  while :; do
    objects="$(aws s3api list-object-versions --bucket "$bucket" --output json | jq -c '[.Versions[]?,.DeleteMarkers[]? | {Key:.Key,VersionId:.VersionId}]')"
    [[ "$objects" == '[]' ]] && break
    aws s3api delete-objects --bucket "$bucket" --delete "{\"Objects\":$objects,\"Quiet\":true}" >/dev/null
  done
  aws s3api delete-bucket --bucket "$bucket" --region "$region"
else exit 2; fi
