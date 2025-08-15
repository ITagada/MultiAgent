import requests
import json
import logging
import sqlite3

from pathlib import Path
from typing import Dict, Optional

from orchester.search_engine import XLSXSearchEngine
from orchester.ollama_llm import OllamaLLM
from orchester.schemas import ServiceRegistration


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('orchester.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = Path('services.db')

class ServiceDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS services (
                uuid TEXT PRIMARY KEY,
                service_name TEXT NOT NULL,
                url TEXT NOT NULL
                )
            """)
        logger.info('Таблица services готова')

    def insert_or_update(self, uuid: str, service_name: str, url: str):
        uuid = uuid.replace("'", "")
        service_name = service_name.replace("'", "")
        url = url.replace("'", "")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO services (uuid, service_name, url)
                VALUES (?, ?, ?)
                ON CONFLICT (uuid) DO UPDATE SET
                    service_name=EXCLUDED.service_name, url=EXCLUDED.url
            """, (uuid, service_name, url))
        logger.info(f"Сервис сохранен в БД: {service_name} ({uuid}, {url})")

    def get_all(self) -> Dict[str, Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT uuid, service_name, url FROM services")
            return {
                row[0]: {'uuid': row[0], 'service_name': row[1], 'url': row[2]}
                for row in cursor.fetchall()
            }

class Orchestrator:
    def __init__(self, local_search_engine: Optional[XLSXSearchEngine] = None):
        self.services: Dict[str,Dict] = {}
        self.llm = OllamaLLM()
        self.local_search_engine = local_search_engine
        self.db = ServiceDB(DB_PATH)
        self._load_services()

    def _load_services(self):
        import json
        db_services = self.db.get_all()
        logger.info(f"Загружено {len(db_services)} сервисов из БД")

        for uuid, svc in db_services.items():
            try:
                r = requests.get(svc['url'].rstrip('/') + '/', timeout=10)
                r.raise_for_status()

                remote_data = None

                try:
                    resp = r.json()
                    if isinstance(resp, str):
                        clean = resp.replace("'", '"')
                        remote_data = json.loads(clean)
                    else:
                        remote_data = resp
                except Exception as e:
                    logger.error(f"Не удалось распарсить ответ сервиса {svc['url']}: {e}")
                    continue

                if not isinstance(remote_data, dict):
                    logger.info(
                        f"Сервис {svc['url']} вернул неожиданный тип: {type(remote_data)}, данные: {remote_data}")
                    continue

                remote_name = remote_data.get('name', '').strip()
                remote_endpoint = remote_data.get('endpoint', '').strip('/')

                db_name = svc['service_name'].strip() if svc['service_name'] else ''
                db_endpoint = svc['url'].rstrip('/')

                name_changed = remote_name != db_name
                endpoint_changed = remote_endpoint != db_endpoint

                if name_changed or endpoint_changed:
                    logger.info(
                        f"Обновление сервиса {uuid}: "
                        f"name_changed={name_changed} ({db_name} → {remote_name}), "
                        f"endpoint_changed={endpoint_changed} ({db_endpoint} → {remote_endpoint})"
                    )
                    self.db.insert_or_update(uuid, remote_name, remote_endpoint)

                self.services[remote_name] = {
                    'uuid': uuid,
                    'name': remote_name,
                    'description': remote_data.get('description', ''),
                    'system_prompt': remote_data.get('system_prompt', ''),
                    'request_format': remote_data.get('request_format', {}),
                    'endpoint': remote_endpoint,
                }

            except Exception as e:
                logger.error(f"Ошибка загрузки сервиса {svc['url']}: {e}")

    def register_service(self, service: ServiceRegistration):
        self.services[service.name] = {
            'uuid': service.uuid,
            'name': service.name,
            'description': service.description,
            'system_prompt': service.system_prompt,
            'request_format': service.request_format or {},
            'endpoint': service.endpoint,
        }
        logger.info(f"Сервис зарегистрирован: {service.name} ({service.uuid})")
        self.db.insert_or_update(service.uuid, service.name, service.endpoint)

    def list_services(self):
        return list(self.services.keys())

    def _call_http_service(self, service_info: Dict, query: str):
        endpoint = service_info['endpoint'].rstrip('/') + '/makejob'
        try:
            logger.info(f"Отправка HTTP-запроса на {endpoint}")
            params = {'request': query, 'limit': 5}
            r = requests.post(endpoint, params=params, timeout=10)
            r.raise_for_status()
            try:
                return {'service': service_info['name'], 'response': r.json()}
            except Exception:
                return {'service': service_info['name'], 'response': r.text}
        except Exception as e:
            logger.error(f"Ошибка HTTP-запроса к {endpoint}: {e}")
            return {'service': service_info['name'], 'error': str(e)}

    def _call_local_service(self, service_info: Dict, query: str):
        if self.local_search_engine and service_info['endpoint'].startswith('internal'):
            logger.info(f"Вызов локального сервиса: {service_info['name']}")
            res = self.local_search_engine.search(query)
            return {'service': service_info['name'], 'response': res}
        return {'service': service_info['name'], 'error': 'Локальный сервис недоступен'}

    def handle_request(self, query: str):
        logger.info(f"Обработка запроса: {query}")
        q_lower = query.lower()
        for svc in self.services.values():
            kws = svc.get('request_format', {}).get('keywords', [])
            if kws:
                for kw in kws:
                    if kw.lower() in q_lower:
                        logger.info(f"Ключевое слово '{kw}' → {svc['name']}")
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

        logger.warning("Сервис не выбран → LLM отвечает напрямую")
        print("[Оркестратор] Сервис не выбран → LLM отвечает напрямую")
        return {"service": None, "response": self.llm.run(query)}

try:
    LOCAL_XLSX = r"C:\Users\polovnikov.m\PycharmProjects\multiagent\xslx_db\Выгрузка справочника эталонной номенклатуры 2 XLSX.xlsx"
    local_search = XLSXSearchEngine(LOCAL_XLSX)
except Exception as e:
    logger.error(f"Локальный поиск не инициализирован: {e}")
    print("[Оркестратор] Локальный поиск не инициализирован:", e)
    local_search = None

orchestrator = Orchestrator(local_search_engine=local_search)
