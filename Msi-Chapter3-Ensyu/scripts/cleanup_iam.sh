#!/usr/bin/env bash
set -euo pipefail
: "${ROLE_NAME:=lambda-dynamodb-ensyu-role}"

aws iam delete-role-policy --role-name "${ROLE_NAME}" --policy-name DynamoDB-CRUD-Items || true
aws iam detach-role-policy --role-name "${ROLE_NAME}" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole || true
aws iam delete-role --role-name "${ROLE_NAME}" || true
echo "[SUCCESS] Cleanup completed."
