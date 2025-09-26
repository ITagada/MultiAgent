import requests
import json
import logging
import sqlite3

from pathlib import Path
from typing import Dict, Optional, List

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


SYSTEM_RULES = [
    {
        "name": "decompose_task",
        "rule": (
            "Ты — помощник для разбиения задач на подзадачи.\n"
            "Раздели пользовательский запрос только на ключевые шаги (НЕ более 2–3).\n"
            "Не расписывай внутренние действия или подробности выполнения.\n"
            "Ответ верни строго в JSON формате: {\"subtasks\": [\"...\", \"...\"]}\n\n"
            "Пример:\n"
            "Запрос: \"Проверяй доступные заявки на формирование лотов каждый будний день в 10 часов утра\"\n"
            "Подзадачи:\n"
            "{\"subtasks\": [\"Проверять доступные заявки на формирование лотов\", "
            "\"Создать расписание для автоматической проверки каждый будний день в 10:00\"]}\n\n"
            "Задача: {query}"
        ),
        "is_system": 1,
        "position": "before",
    },
    {
        "name": "extract_schedule",
        "rule": (
            "Определи, содержит ли эта подзадача указание на время или период выполнения. "
            "Если да — верни JSON {\"time\": \"2025-09-16 12:00\", \"interval\": 3600}. "
            "Если нет — верни JSON {\"time\": null, \"interval\": null}.\n\n"
            "Подзадача: {query}"
        ),
        "is_system": 1,
        "position": "before",
    },
    {
        "name": "choose_service",
        "rule": (
            "Ты — интеллектуальный оркестратор. У тебя есть список сервисов.\n"
            "Каждый сервис описан своим именем, описанием и системным промптом.\n\n"
            "Запрос пользователя:\n{query}\n\n"
            "Твоя задача — выбрать ОДИН сервис (строго UUID из списка), "
            "который лучше всего подходит для обработки запроса. "
            "Если подходящего сервиса нет — напиши 'NONE'.\n\n"
            "Сервисы:\n{services}"
        ),
        "is_system": 1,
        "position": "before",
    },
]

class ServiceDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()
        for rule in SYSTEM_RULES:
            self.save_rule(rule["name"], rule["rule"], rule["is_system"], rule["position"])

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS services (
                    uuid TEXT PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    description TEXT,
                    system_prompt TEXT,
                    request_format TEXT
                )
            """)
            conn.execute("""
                       CREATE TABLE IF NOT EXISTS rules (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL UNIQUE,
                            rule TEXT NOT NULL,
                            is_system INTEGER DEFAULT 0,
                            position TEXT DEFAULT None,
                            embedding TEXT DEFAULT None
                       )
                   """)
            conn.execute("""
                       CREATE TABLE IF NOT EXISTS scheduled_tasks (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           task TEXT NOT NULL,
                           schedule_time TEXT,
                           interval_seconds INTEGER,
                           status TEXT DEFAULT 'pending'
                       )
                   """)
        logger.info('Таблица services, rules, schedule_tasks готовы')

    def insert_or_update(self, service: Dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO services (uuid, service_name, url, description, system_prompt, request_format)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (uuid) DO UPDATE SET
                    service_name=EXCLUDED.service_name,
                    url=EXCLUDED.url,
                    description=EXCLUDED.description,
                    system_prompt=EXCLUDED.system_prompt,
                    request_format=EXCLUDED.request_format
            """, (
                service['uuid'],
                service['name'],
                service['endpoint'],
                service.get('description', ''),
                service.get('system_prompt', ''),
                json.dumps(service.get('request_format', {}), ensure_ascii=False),
            ))
        logger.info(f"Сервис сохранен в БД: {service})")

    def get_all(self) -> Dict[str, Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT uuid, service_name, url, description, system_prompt, request_format
                FROM services
            """)
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    'uuid': row[0],
                    'name': row[1],
                    'endpoint': row[2],
                    'description': row[3],
                    'system_prompt': row[4],
                    'request_format': json.loads(row[5]) if row[5] else {}
                }
            return result

    def update_rule(self, rule_id: int, name: str, rule: str, is_system: int = 0, position: str = None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE rules SET name=?, rule=?, is_system=?, position=? WHERE id=?",
                (name, rule, is_system, position, rule_id)
            )
            conn.commit()

    def delete_rule(self, rule_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM rules WHERE id=?", (rule_id,))

    def get_all_rules(self) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, name, rule, is_system, position FROM rules")
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "rule": row[2],
                    "is_system": bool(row[3]),
                    "position": row[4],
                }
                for row in cur.fetchall()
            ]

    def get_rule_by_id(self, rule_id: int) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, name, rule, is_system, position FROM rules WHERE id=?", (rule_id,))
            row = cur.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "rule": row[2],
                "is_system": bool(row[3]),
                "position": row[4],
            } if row else None

    def get_rule_by_name(self, name: str) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT id, name, rule, is_system, position FROM rules WHERE name=? LIMIT 1",
                (name,)
            )
            row = cur.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "rule": row[2],
                "is_system": bool(row[3]),
                "position": row[4],
            } if row else None

    def save_rule(self, name: str, rule: str, is_system: int = 0, position: str = "before"):
        embedding = OllamaLLM.embed_text(rule)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO rules (name, rule, is_system, position, embedding)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, rule, is_system, position, json.dumps(embedding))
            )

    def fint_matching_rule(self, text: str, threshold: float = 0.75) -> Optional[Dict]:
        query_emb = OllamaLLM.embed_text(text)
        best = None
        best_score = 0.0

        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, name, rule, is_system, position, embedding FROM rules")
            for row in cur.fetchall():
                try:
                    emb = json.loads(row[5])
                    score = OllamaLLM.cosine_similarity(query_emb, emb)
                    if score > best_score:
                        best_score = score
                        best = {
                            "id": row[0],
                            "name": row[1],
                            "rule": row[2],
                            "is_system": bool(row[3]),
                            "position": row[4],
                            "similarity": score
                        }
                except Exception:
                    continue

        if best and best['similarity'] >= threshold:
            return best
        return None

    def save_scheduled_task(self, task: str, schedule_info: Dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO scheduled_tasks (task, schedule_time, interval_seconds, status)
                VALUES (?, ?, ?, 'pending')
                """,
                (
                    task,
                    schedule_info.get("schedule_time"),
                    schedule_info.get("interval_seconds"),
                ),
            )


class Orchestrator:
    def __init__(self, local_search_engine: Optional[XLSXSearchEngine] = None):
        self.services: Dict[str, Dict] = {}
        self.db = ServiceDB(DB_PATH)
        self.llm = OllamaLLM(self.db)
        self.local_search_engine = local_search_engine
        self._load_services()

    def _normalize_endpoint(self, endpoint: str) -> str:
        """Добавляем http:// если схема не указана"""
        if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
            return "http://" + endpoint
        return endpoint

    def _load_services(self):
        db_services = self.db.get_all()
        logger.info(f"Загружено {len(db_services)} сервисов из БД")

        for uuid, svc in db_services.items():
            try:
                normalized_endpoint = self._normalize_endpoint(svc["endpoint"])
                r = requests.get(normalized_endpoint, timeout=10)
                r.raise_for_status()

                try:
                    remote_data = r.json()
                except Exception as e:
                    logger.error(f"Ошибка JSON от {normalized_endpoint}: {e}")
                    continue

                try:
                    remote_data = r.json()
                    if isinstance(remote_data, str):
                        try:
                            remote_data = json.loads(remote_data)
                        except Exception:
                            pass
                    if not isinstance(remote_data, dict):
                        logger.warning(f"Сервис {normalized_endpoint} вернул неожиданный формат: {type(remote_data)} → {remote_data}")
                        continue
                except Exception as e:
                    logger.error(f"Ошибка JSON от {normalized_endpoint}: {e}")
                    continue

                remote_name = remote_data.get("name", "").strip()
                remote_endpoint = self._normalize_endpoint(remote_data.get("endpoint", "").rstrip("/"))

                db_name = svc["name"].strip() if svc["name"] else ""
                db_endpoint = self._normalize_endpoint(svc["endpoint"].rstrip("/"))

                if remote_name != db_name or remote_endpoint != db_endpoint:
                    logger.info(
                        f"Обновление сервиса {uuid}: {db_name}→{remote_name}, {db_endpoint}→{remote_endpoint}"
                    )
                    svc["name"] = remote_name
                    svc["endpoint"] = remote_endpoint
                    self.db.insert_or_update(svc)

                self.services[uuid] = {
                    "uuid": uuid,
                    "name": remote_name,
                    "description": remote_data.get("description", ""),
                    "system_prompt": remote_data.get("system_prompt", ""),
                    "request_format": remote_data.get("request_format", {}),
                    "endpoint": remote_endpoint,
                }

            except Exception as e:
                logger.error(f"Ошибка загрузки сервиса {svc['endpoint']}: {e}")

    def register_service(self, service: ServiceRegistration):
        svc_dict = {
            "uuid": service.uuid,
            "name": service.name,
            "description": service.description or "",
            "system_prompt": service.system_prompt or "",
            "request_format": service.request_format or {},
            "endpoint": self._normalize_endpoint(service.endpoint),
        }
        self.services[service.uuid] = svc_dict
        self.db.insert_or_update(svc_dict)
        logger.info(f"Сервис зарегистрирован: {service.name} ({service.uuid})")

    def list_services(self):
        return list(self.services.keys())

    def _call_http_service(self, service_info: Dict, query: str):
        endpoint = service_info["endpoint"].rstrip("/") + "/makejob"
        try:
            logger.info(f"Отправка HTTP-запроса на {endpoint}")
            params = {"request": query}
            r = requests.post(endpoint, params=params, timeout=10, proxies={"http": None, "https": None})
            logger.info(f"Формат запроса: {r}")
            r.raise_for_status()
            try:
                return {"service": service_info["name"], "response": r.json()}
            except Exception:
                return {"service": service_info["name"], "response": r.text}
        except Exception as e:
            logger.error(f"Ошибка HTTP-запроса к {endpoint}: {e}")
            return {"service": service_info["name"], "error": str(e)}

    def _call_local_service(self, service_info: Dict, query: str):
        if self.local_search_engine and service_info["endpoint"].startswith("internal"):
            logger.info(f"Вызов локального сервиса: {service_info['name']}")
            res = self.local_search_engine.search(query)
            return {"service": service_info["name"], "response": res}
        return {"service": service_info["name"], "error": "Локальный сервис недоступен"}

    def _extract_response(self, raw_response):
        if isinstance(raw_response, dict):
            if "response" in raw_response:
                return raw_response["response"]
            return json.dumps(raw_response, ensure_ascii=False)
        return raw_response

    def _format_response(self, service_name: Optional[str], res: str, uuid: Optional[str] = None):
        item = {"service": service_name, "res": self._extract_response(res)}
        if uuid:
            item["serviceUUID"] = uuid
        return {"response": [item]}

    def plan_and_execute(self, query: str):
        logger.info(f"[Оркестратор] Планирование для запроса: {query}")
        subtasks = self.llm.decompose_task(query)
        logger.info(f"[Оркестратор] Получены подзадачи: {subtasks}")
        results = []

        for sub in subtasks:
            logger.info(f"[Оркестратор] Обработка подзадачи: {sub}")
            rule = self.db.get_rule(sub)
            if not rule:
                logger.warning(f"[Оркестратор] Правило для подзадачи '{sub}' не найдено — пропуск")
                results.append({"subtask": sub, "status": "no_rule"})
                continue
            logger.info(f"[Оркестратор] Используем правило: {rule}")

            schedule_info = self.llm.extract_schedule(sub)
            if schedule_info:
                logger.info(f"[Оркестратор] Подзадача является расписанием: {schedule_info}")
                self.db.save_scheduled_task(sub, schedule_info)
                results.append({"subtask": sub, "status": "scheduled"})
                continue

            chosen_uuid = self.llm.choose_service(self.services, sub)
            if chosen_uuid and chosen_uuid in self.services:
                svc = self.services[chosen_uuid]
                logger.info(f"[Оркестратор] LLM выбрал сервис {svc['name']} ({svc['uuid']}) для подзадачи '{sub}'")
                raw = self._call_http_service(svc, sub)
                results.append(
                    {
                        "subtask": sub,
                        "service": svc["name"],
                        "response": self._extract_response(raw.get("response") or raw.get("error")),
                    }
                )
            else:
                logger.warning(f"[Оркестратор] Нет подходящего сервиса для подзадачи '{sub}'")
                results.append({"subtask": sub, "error": "No suitable service"})

        logger.info(f"[Оркестратор] Планирование завершено, результаты: {results}")
        return {"results": results}

    def handle_request(self, query: str, services_uuids: Optional[list] = None):
        logger.info(f"Обработка запроса: {query}")

        start_rule = self.db.fint_matching_rule("Prompt1") or {}
        end_rule = self.db.fint_matching_rule("Prompt2") or {}
        wrapper_query = f"{start_rule.get('rule', '')}\n{query}\n{end_rule.get('rule', '')}"

        subtasks = self.llm.decompose_task(wrapper_query)
        results = []

        for sub in subtasks:
            rule = self.db.fint_matching_rule(sub)
            if not rule:
                results.append({"subtask": sub, "status": "no_rule"})
                continue

            if rule["name"] == "extract_schedule":
                schedule_info = self.llm.extract_schedule(sub)
                self.db.save_scheduled_task(sub, schedule_info)
                results.append({"subtask": sub, "status": "scheduled", "schedule": schedule_info})

            elif rule["name"] == "choose_service":
                chosen_uuid = self.llm.choose_service(self.services, sub)
                if chosen_uuid and chosen_uuid in self.services:
                    svc = self.services[chosen_uuid]
                    raw = self._call_http_service(svc, sub)
                    results.append({
                        "subtask": sub,
                        "service": svc["name"],
                        "response": raw.get("response") or raw.get("error"),
                    })
                else:
                    results.append({"subtask": sub, "status": "need_user_input"})
            else:
                response = self.llm.apply_rule(rule, sub)
                results.append({"subtask": sub, "rule": rule["name"], "response": response})

        return self._format_response("orchstrator", {"algorithm": results})


try:
    LOCAL_XLSX = r"C:\Users\polovnikov.m\PycharmProjects\multiagent\xslx_db\Выгрузка справочника эталонной номенклатуры 2 XLSX.xlsx"
    local_search = XLSXSearchEngine(LOCAL_XLSX)
except Exception as e:
    logger.error(f"Локальный поиск не инициализирован: {e}")
    local_search = None

orchestrator = Orchestrator(local_search_engine=local_search)
