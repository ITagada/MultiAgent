import requests

from typing import Dict, Optional

from orchester.search_engine import XLSXSearchEngine
from orchester.ollama_llm import OllamaLLM
from orchester.schemas import ServiceRegistration


class Orchestrator:
    def __init__(self, local_search_engine: Optional[XLSXSearchEngine] = None):
        self.services: Dict[str,Dict] = {}
        self.llm = OllamaLLM()
        self.local_search_engine = local_search_engine

    def register_service(self, service: ServiceRegistration):
        self.services[service.name] = {
            'name': service.name,
            'description': service.description,
            'system_prompt': service.system_prompt,
            'request_format': service.request_format or {},
            'endpoint': service.endpoint,
        }
        print(f"[Оркестратор] сервис зарегистрирован: {service.name}")

    def list_services(self):
        return list(self.services.keys())

    def _call_http_service(self, service_info: Dict, query: str):
        endpoint = service_info['endpoint']
        try:
            r = requests.post(endpoint, json={'query': query}, timeout=30)
            r.raise_for_status()
            try:
                return {'service': service_info['name'], 'response': r.json()}
            except Exception:
                return {'service': service_info['name'], 'response': r.text}
        except Exception as e:
            return {'service': service_info['name'], 'error': str(e)}

    def _call_local_service(self, service_info: Dict, query: str):
        if self.local_search_engine and service_info['endpoint'].startswith('internal'):
            res = self.local_search_engine.search(query)
            return {'service': service_info['name'], 'response': res}
        return {'service': service_info['name'], 'error': 'Локальный сервис недоступен'}

    def handle_request(self, query: str):
        q_lower = query.lower()
        for svc in self.services.values():
            kws = svc.get('request_format', {}).get('keywords', [])
            if kws:
                for kw in kws:
                    if kw.lower() in q_lower:
                        print(f"[Оркестратор] ключевое слово '{kw}' -> {svc['name']}")
                        if svc['endpoint'].startswith('http'):
                            return self._call_http_service(svc, query)
                        else:
                            return self._call_local_service(svc, query)

        chosen = self.llm.choose_service(self.services, query)
        if chosen:
            svc = self.services.get(chosen)
            if svc:
                print(f"[Оркестратор] LLM выбрал сервис: {chosen}")
                if svc['endpoint'].startswith('http'):
                    return self._call_http_service(svc, query)
                else:
                    return self._call_local_service(svc, query)

        print("[Оркестратор] Сервис не выбран → LLM отвечает напрямую")
        return {"service": None, "response": self.llm.run(query)}

try:
    LOCAL_XLSX = r"C:\Users\polovnikov.m\PycharmProjects\multiagent\xslx_db\Выгрузка справочника эталонной номенклатуры 2 XLSX.xlsx"
    local_search = XLSXSearchEngine(LOCAL_XLSX)
except Exception as e:
    print("[Оркестратор] Локальный поиск не инициализирован:", e)
    local_search = None

orchestrator = Orchestrator(local_search_engine=local_search)

test_service = ServiceRegistration(
    name="xlsx_agent",
    description="Поиск информации в XLSX-файле",
    system_prompt="Ты агент, который находит УИД и номенклатуру по запросу пользователя.",
    request_format={"keywords": ["найди", "поиск", "uid", "уид", "номенклатура"]},
    endpoint="http://192.168.6.97:8000/search"
)
orchestrator.register_service(test_service)