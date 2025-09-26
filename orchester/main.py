import sqlite3

import requests
import base64

from requests.auth import HTTPBasicAuth
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Dict, List

from orchester.schemas import TaskRequest, ServiceRegistration, RuleOut, RuleIn
from orchester.agent import orchestrator


print(">>> ЗАПУСКАЕТСЯ main.py")
app = FastAPI()

@app.post('/register_service')
def register_service(service: ServiceRegistration):
    orchestrator.register_service(service)
    return {'status': 'registered', 'total_services': len(orchestrator.services)}

@app.get('/services')
def list_services():
    return {'services': orchestrator.db.get_all()}

@app.post("/ask")
async def ask(req: TaskRequest, request: Request):
    raw = await request.json()
    services = raw.get('servicesUUID', None)

    if not (isinstance(services, list) and all(isinstance(x, str) for x in services)):
        services = None

    print("[main] Received query:", req.prompt, "services:", services)
    res = orchestrator.handle_request(req.prompt, services)
    return res

@app.get("/rules", response_model=List[RuleOut])
def list_rules():
    return orchestrator.db.get_all_rules()

@app.get("/rules/{rule_id}", response_model=RuleOut)
def get_rule(rule_id: int):
    rule = orchestrator.db.get_rule_by_id(rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule

@app.post("/rules")
def create_rule(rule: RuleIn):
    orchestrator.db.save_rule(
        rule.name,
        rule.rule,
        int(rule.is_system),
        rule.position
    )
    with sqlite3.connect(orchestrator.db.db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT last_insert_rowid()")
        new_id = cur.fetchone()[0]
    return orchestrator.db.get_rule_by_id(new_id)

@app.post("/rules/{rule_id}", response_model=RuleOut)
def update_rule(rule_id: int, rule: RuleIn):
    if not orchestrator.db.get_rule_by_id(rule_id):
        raise HTTPException(status_code=404, detail="Rule not found")
    orchestrator.db.update_rule(rule_id, rule.name, rule.rule, rule.is_system, rule.position)
    return orchestrator.db.get_rule_by_id(rule_id)

@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: int):
    if not orchestrator.db.get_rule_by_id(rule_id):
        raise HTTPException(status_code=404, detail="Rule not found")
    orchestrator.db.delete_rule(rule_id)
    return {'status': 'deleted', 'id': rule_id}

@app.get("/get_1c_data")
def get_1c_data(limit: int = 10):
    url = "https://1c.infotechsoft.ru/Control_Buy_TD/hs/Data"
    try:
        r = requests.get(
            url,
            auth=HTTPBasicAuth("UserFI", "Ho0de3vi"),
            headers={
                "User-Agent": "PostmanRuntime/7.46.0",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            },
            verify=False,
            timeout=50,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        data = r.json().get("Data", [])
        return {"Data": data[:limit]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка запроса к 1с: {e}")

# @app.post('/internal/xlsx_search')
# def internal_xlsx_search(payload: Dict):
#     q = payload.get('query', '')
#     if orchestrator.local_search_engine is None:
#         raise HTTPException(status_code=500, detail='Функция локального поиска недоступна')
#     results = orchestrator.local_search_engine.search(q)
#     return {'results': results}
