import json, boto3

db = boto3.resource("dynamodb").Table("Items")

CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Content-Type": "application/json"
}

def R(code, body=""):
    return {"statusCode": code, "headers": CORS,
            "body": body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)}

def lambda_handler(e, _):
    m = e.get("httpMethod") or e["requestContext"]["http"]["method"]
    p = e.get("path") or e.get("rawPath","/")
    id_ = (e.get("pathParameters") or {}).get("id")
    if not id_:
        seg = [s for s in p.split("/") if s]
        if len(seg)>=2 and seg[0]=="items": id_ = "/".join(seg[1:])
    b = json.loads(e.get("body") or "{}")

    if m=="OPTIONS": return R(204,"")
    if m=="GET": return R(200, db.get_item(Key={"id":id_}).get("Item") if id_ else db.scan().get("Items",[]))
    if m=="POST" and not id_: 
        if not all(k in b for k in ("id","description","date")): return R(400,{"message":"必須項目不足"})
        db.put_item(Item=b); return R(200,{"message":"created"})
    if m=="PUT" and id_:
        db.update_item(Key={"id":id_},
            UpdateExpression="SET #d=:d,#t=:t",
            ExpressionAttributeNames={"#d":"description","#t":"date"},
            ExpressionAttributeValues={":d":b.get("description"),":t":b.get("date")})
        return R(200,{"message":"updated"})
    if m=="DELETE":
        del_id = id_ or b.get("id")
        if not del_id: return R(400,{"message":"id 必須"})
        db.delete_item(Key={"id":del_id}); return R(200,{"message":"deleted"})
    return R(405,{"message":"unsupported"})
