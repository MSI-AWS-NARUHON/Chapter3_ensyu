#!/usr/bin/env bash
set -euo pipefail
: "${ROLE_NAME:=lambda-dynamodb-ensyu-role}"
: "${REGION:=ap-northeast-1}"
: "${ACCOUNT_ID:=$(aws sts get-caller-identity --query Account --output text)}"

echo "[INFO] ACCOUNT_ID=${ACCOUNT_ID} REGION=${REGION} ROLE_NAME=${ROLE_NAME}"

cat > trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

cat > dynamodb-crud-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem","dynamodb:BatchGetItem",
        "dynamodb:PutItem","dynamodb:UpdateItem",
        "dynamodb:DeleteItem","dynamodb:BatchWriteItem",
        "dynamodb:Query","dynamodb:Scan",
        "dynamodb:DescribeTable"
      ],
      "Resource": [
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/Items",
        "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/Items/index/*"
      ]
    }
  ]
}
EOF

if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "[INFO] Role already exists: ${ROLE_NAME}"
else
  echo "[INFO] Creating role: ${ROLE_NAME}"
  aws iam create-role     --role-name "${ROLE_NAME}"     --assume-role-policy-document file://trust-policy.json >/dev/null
fi

if ! aws iam list-attached-role-policies --role-name "${ROLE_NAME}"   --query "AttachedPolicies[?PolicyName=='AWSLambdaBasicExecutionRole']" --output text | grep -q AWSLambdaBasicExecutionRole; then
  aws iam attach-role-policy     --role-name "${ROLE_NAME}"     --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  echo "[INFO] Attached AWSLambdaBasicExecutionRole"
fi

aws iam put-role-policy   --role-name "${ROLE_NAME}"   --policy-name DynamoDB-CRUD-Items   --policy-document file://dynamodb-crud-policy.json

echo "[SUCCESS] IAM setup completed. Role: ${ROLE_NAME}"
