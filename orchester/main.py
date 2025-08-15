from fastapi import FastAPI, HTTPException
from typing import Dict
from orchester.schemas import TaskRequest, ServiceRegistration
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
def ask(req: TaskRequest):
    print("[main] Received query:", req.prompt)
    res = orchestrator.handle_request(req.prompt)
    return res

@app.post('/internal/xlsx_search')
def internal_xlsx_search(payload: Dict):
    q = payload.get('query', '')
    if orchestrator.local_search_engine is None:
        raise HTTPException(status_code=500, detail='Функция локального поиска недоступна')
    results = orchestrator.local_search_engine.search(q)
    return {'results': results}
