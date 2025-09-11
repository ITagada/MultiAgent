from fastapi import FastAPI, HTTPException, Request
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
async def ask(req: TaskRequest, request: Request):
    raw = await request.json()
    services = raw.get('servicesUUID', None)

    if not (isinstance(services, list) and all(isinstance(x, str) for x in services)):
        services = None

    print("[main] Received query:", req.prompt, "services:", services)
    res = orchestrator.handle_request(req.prompt, services)
    return res

# @app.post('/internal/xlsx_search')
# def internal_xlsx_search(payload: Dict):
#     q = payload.get('query', '')
#     if orchestrator.local_search_engine is None:
#         raise HTTPException(status_code=500, detail='Функция локального поиска недоступна')
#     results = orchestrator.local_search_engine.search(q)
#     return {'results': results}
