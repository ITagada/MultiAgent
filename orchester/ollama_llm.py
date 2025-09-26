import json
import requests
import math
import numpy as np
import logging

from typing import List, Dict, Optional, Union

from sentence_transformers import SentenceTransformer


logger = logging.getLogger(__name__)


class OllamaLLM:
    _embedder = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')

    def __init__(self, db, model="qwen2.5:32b", host="http://192.168.6.97:11434"):
        self.model = model
        self.host = host.rstrip("/")
        self.db = db

    def run(self, query: Union[str, List[Dict[str, str]]]) -> str:
        """
        Универсальный вызов Ollama:
        - query=str → /api/generate
        - query=list(messages) → /api/chat
        """
        if isinstance(query, str):
            url = f"{self.host}/api/generate"
            payload = {'model': self.model, 'prompt': query, 'stream': False}
        elif isinstance(query, list):
            url = f"{self.host}/api/chat"
            payload = {'model': self.model, 'messages': query, 'stream': False}
        else:
            raise ValueError("query должен быть str или список сообщений [{'role','content'}]")

        try:
            r = requests.post(url, json=payload, timeout=120)
            r.raise_for_status()
            try:
                data = r.json()
                if isinstance(data, dict):
                    if 'response' in data:
                        return data['response']
                    if 'messages' in data and 'content' in data['message']:
                        return data['messages']['content']
                    if 'choices' in data and len(data['choices']) > 0:
                        choice = data['choices'][0]
                        if 'message' in choice and 'content' in choice['message']:
                            return choice['message']['content']
                return str(data)
            except json.JSONDecodeError:
                return r.text
        except Exception as e:
            return f"[LLM error] {e}"

    def run_with_prompts(self, query: str) -> str:
        pass
    def decompose_task(self, query: str) -> List[str]:
        rule = self.db.get_rule_by_name("decompose_task")
        if not rule:
            return [query]

        prompt = rule['rule'].replace("{query}", query)
        resp = self.run(prompt)
        try:
            data = json.loads(resp)
            return data.get("subtask", [])
        except:
            return [query]

    def extract_schedule(self, subtask: str) -> Optional[Dict]:
        rule = self.db.get_rule_by_name("extract_schedule")
        if not rule:
            return None

        prompt = rule["rule"].replace("{query}", subtask)
        resp = self.run(prompt)
        try:
            return json.loads(resp)
        except:
            return None

    def choose_service(self, services: Dict[str, Dict], query: str) -> Optional[str]:
        """
        Просим LLM выбрать наиболее подходящий сервис.
        Учитываем description, system_prompt.
        Возвращает uuid сервиса или None.
        """
        if not services:
            return None

        description_lines = []
        for uuid, info in services.items():
            desc = info.get("description", "")
            system = info.get("system_prompt", "")
            description_lines.append(
                f"- {info['name']} (UUID: {uuid}):\n Description: {desc}\n System prompt: {system}\n"
            )
        services_str = "\n".join(description_lines)

        rule = self.db.get_rule_by_name("choose_service")
        if not rule:
            return None

        prompt = rule["rule"].replace("{query}", query).replace("{services}", services_str)

        resp = self.run(prompt).strip()
        resp_upper = resp.upper()
        for uuid in services.keys():
            if uuid.upper() in resp_upper:
                return uuid
        if resp_upper == "NONE":
            return None
        for name in services.keys():
            if resp.strip().lower() == name.lower():
                return name
        return None

    @staticmethod
    def embed_text(text: str) -> list:
        try:
            return OllamaLLM._embedder.encode(text).tolist()
        except Exception as e:
            logger.error(f"Ошибка получения эмбеддинга: {e}")
            return []

    @staticmethod
    def cosine_similarity(vec1: list, vec2: list) -> float:
        if not vec1 or not vec2:
            return 0.0
        v1, v2 = np.array(vec1), np.array(vec2)
        denom = (np.linalg.norm(v1) * np.linalg.norm(v2))
        if denom == 0:
            return 0.0
        return float(np.dot(v1, v2) / denom)

    def apply_rule(self, rule: dict, query: str) -> str:
        if not rule or "rule" not in rule:
            return "Правило не найдено"
        prompt = rule["rule"].replace("{query}", query)
        logger.info(f"Применяем правило '{rule.get('name')}' -> LLM")
        try:
            resp = requests.post(
                "http://192.168.6.97:11434/api/generate",
                json={"model": "llama3", "prompt": prompt, "stream": False},
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("response") or data.get("message", "")
        except Exception as e:
            logger.error(f"Ошибка применения правила: {e}")
            return f"Ошибка применения правила: {e}"