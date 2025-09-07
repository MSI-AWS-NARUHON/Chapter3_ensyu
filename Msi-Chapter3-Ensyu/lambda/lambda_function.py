import os
import json
import logging
import decimal
import boto3
from botocore.exceptions import ClientError

# ==== Settings (env-driven; sensible defaults for the exercise) ====
TABLE_NAME = os.environ.get("TABLE_NAME", "Items")
CORS_ALLOW_ORIGIN  = os.environ.get("CORS_ALLOW_ORIGIN",  "*")
CORS_ALLOW_HEADERS = os.environ.get("CORS_ALLOW_HEADERS", "Content-Type,Authorization")
CORS_ALLOW_METHODS = os.environ.get("CORS_ALLOW_METHODS", "GET,POST,PUT,DELETE,OPTIONS")

# ==== Clients (module-global reuse) ====
db = boto3.resource("dynamodb").Table(TABLE_NAME)

# ==== Logging ====
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ==== JSON helpers ====
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            # Return int when exact integer, else float
            return int(o) if o % 1 == 0 else float(o)
        return super().default(o)

def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, cls=DecimalEncoder)

# ==== HTTP helpers ====
def _headers():
    return {
        "Access-Control-Allow-Origin":  CORS_ALLOW_ORIGIN,
        "Access-Control-Allow-Methods": CORS_ALLOW_METHODS,
        "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
        "Content-Type": "application/json; charset=utf-8",
    }

def R(code: int, body=""):
    return {
        "statusCode": code,
        "headers": _headers(),
        "body": body if isinstance(body, str) else dumps(body),
    }

def _method(event) -> str:
    # API Gateway v1: event["httpMethod"]
    # API Gateway v2 (HTTP API): event["requestContext"]["http"]["method"]
    return (event.get("httpMethod")
            or (event.get("requestContext", {}).get("http", {}) or {}).get("method")
            or "GET").upper()

def _path(event) -> str:
    return event.get("path") or event.get("rawPath") or "/"

def _path_id(event, path: str) -> str | None:
    # 1) Prefer pathParameters.id if provided by API Gateway
    id_from_params = (event.get("pathParameters") or {}).get("id")
    if id_from_params:
        return id_from_params
    # 2) Fallback: parse /items/{id...}
    seg = [s for s in path.split("/") if s]
    if len(seg) >= 2 and seg[0] == "items":
        return "/".join(seg[1:])
    return None

def _parse_json_body(event) -> dict:
    b = event.get("body")
    if not b:
        return {}
    try:
        return json.loads(b)
    except Exception as e:
        raise ValueError("Invalid JSON body") from e

# ==== Dynamo helpers ====
def _scan_all() -> list:
    """Simple full scan for the exercise (paginated up to all items)."""
    items, excl = [], None
    while True:
        resp = db.scan(**({"ExclusiveStartKey": excl} if excl else {}))
        items.extend(resp.get("Items", []))
        excl = resp.get("LastEvaluatedKey")
        if not excl:
            break
    return items

def lambda_handler(event, _context):
    try:
        m = _method(event)
        p = _path(event)
        i = _path_id(event, p)
        body = _parse_json_body(event)

        logger.info("Request: method=%s path=%s id=%s bodyKeys=%s",
                    m, p, i, list(body.keys()) if isinstance(body, dict) else type(body))

        # CORS preflight
        if m == "OPTIONS":
            # No body for preflight
            return {
                "statusCode": 204,
                "headers": _headers()
                | {"Access-Control-Max-Age": "600"},
                "body": ""
            }

        # GET /items or /items/{id}
        if m == "GET":
            if i:
                res = db.get_item(Key={"id": i})
                return R(200, res.get("Item"))
            return R(200, _scan_all())

        # POST /items  (expects body with id, description, date)
        if m == "POST" and not i:
            required = ("id", "description", "date")
            if not isinstance(body, dict) or not all(k in body and str(body[k]).strip() for k in required):
                return R(400, {"message": "必須項目不足: id, description, date を指定してください"})
            item = {
                "id": str(body["id"]).strip(),
                "description": str(body["description"]),
                "date": str(body["date"]),
            }
            try:
                # Avoid accidental overwrite (409 if already exists)
                db.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(#k)",
                    ExpressionAttributeNames={"#k": "id"},
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                    return R(409, {"message": "同じIDが既に存在します"})
                logger.exception("PutItem failed")
                return R(500, {"message": "サーバ内部エラー"})
            return R(200, {"message": "created"})

        # PUT /items/{id} (partial update allowed)
        if m == "PUT" and i:
            if not isinstance(body, dict):
                return R(400, {"message": "JSON本文が必要です"})
            names, values, sets = {}, {}, []
            if "description" in body:
                names["#d"] = "description"
                values[":d"] = None if body["description"] is None else str(body["description"])
                sets.append("#d=:d")
            if "date" in body:
                names["#t"] = "date"
                values[":t"] = None if body["date"] is None else str(body["date"])
                sets.append("#t=:t")
            if not sets:
                return R(400, {"message": "更新対象のフィールドがありません (description / date)"})
            try:
                db.update_item(
                    Key={"id": i},
                    UpdateExpression="SET " + ", ".join(sets),
                    ExpressionAttributeNames=names,
                    ExpressionAttributeValues=values,
                )
            except ClientError:
                logger.exception("UpdateItem failed")
                return R(500, {"message": "サーバ内部エラー"})
            return R(200, {"message": "updated"})

        # DELETE /items or /items/{id}
        if m == "DELETE":
            del_id = i or (body.get("id") if isinstance(body, dict) else None)
            if not del_id:
                return R(400, {"message": "id 必須"})
            try:
                db.delete_item(Key={"id": del_id})
            except ClientError:
                logger.exception("DeleteItem failed")
                return R(500, {"message": "サーバ内部エラー"})
            return R(200, {"message": "deleted"})

        # Method not supported for this path
        return R(405, {"message": "unsupported"})

    except ValueError as ve:
        # e.g., invalid JSON
        logger.warning("Bad Request: %s", ve)
        return R(400, {"message": str(ve)})
    except Exception:
        logger.exception("Unhandled error")
        return R(500, {"message": "サーバ内部エラー"})
