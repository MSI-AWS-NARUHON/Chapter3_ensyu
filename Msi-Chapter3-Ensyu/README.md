# AWS Serverless Exercise (Hybrid: GUI + CloudShell)

この教材は、**GUIで Lambda / S3( index.html ) を操作**しつつ、**IAM のみ CloudShell で自動化**するハイブリッド構成です。  
初学者が「何をやっているか」を視覚的に理解でき、かつ手順が煩雑になりがちな IAM はワンコマンドで再現可能です。

## 構成
- DynamoDB (テーブル: `Items`, PK: `id`)
- Lambda (Python 3.13) — CRUD
- API Gateway (HTTP API, `/items`, `/items/{id}` ANY)
- S3 + CloudFront — `frontend/index.html` を配信
- IAM — `lambda-dynamodb-ensyu-role` に `DynamoDB-CRUD-Items` を付与（+ `AWSLambdaBasicExecutionRole`）

## 使い方（学習者向け）
### 1) CloudShell で IAM セットアップ
> すでに ZIP を取得済みなら、CloudShell で以下を実行します。

```bash
unzip -q aws-serverless-exercise.zip && cd aws-serverless-exercise
bash scripts/setup_iam.sh
```

### 2) コンソール(GUI)で Lambda を作成し、コードを貼付
- ランタイム: Python 3.13
- 実行ロール: 既存ロールを使用 → `lambda-dynamodb-ensyu-role`
- コード: `lambda/lambda_function.py` をエディタにコピー＆ペースト → Deploy

### 3) API Gateway (HTTP API) を作成
- `/items` と `/items/{id}` を `ANY` で作成
- 統合: 2) の Lambda を指定
- CORS を有効化（`*`, `content-type`, `GET,PUT,POST,OPTIONS,DELETE`）
- ステージ URL（Invoke URL）を控える

### 4) S3 + CloudFront で `index.html` を配信
- S3 バケット作成 → 静的ウェブサイトホスティング有効化 → インデックス `index.html`
- `frontend/index.html` をアップロード
  - ファイル内の `const apiUrl=""` を **APIの Invoke URL + `/items`** に書き換えて保存したものをアップロードするか、アップロード後に `S3上で置換/再アップロード`
- CloudFront ディストリビューション作成（オリジン: S3, Default root object: `index.html`）
- ブラウザで CloudFront のドメインを開く → CRUD 動作を確認

### 5) 片付け
```bash
bash scripts/cleanup_iam.sh
# S3, CloudFront, API Gateway, Lambda, DynamoDB はコンソールから削除
```

## よくあるエラー
- `ValidationError: policyDocument is invalid` → `--policy-document` は `file://` を使う。  
- `AccessDeniedException (DynamoDB)` → ロール未反映/未付与。`setup_iam.sh` を再実行。
- CORS エラー → API Gateway の CORS 設定・`index.html` の `apiUrl` を確認。

---

**補足（実務との違い）**: 実務では React/Next.js 等を用いることが多いですが、学習段階では HTML + fetch で十分です。将来的には CloudFormation/SAM/CDK 化も検討できます。
