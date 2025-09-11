import json
import requests

from typing import List, Dict, Optional, Union

class OllamaLLM:
    def __init__(self, model="qwen2.5:32b", host="http://192.168.6.97:11434"):
        self.model = model
        self.host = host.rstrip("/")

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

    def choose_service(self, services: Dict[str, Dict], query: str) -> Optional[str]:
        """
        Просим LLM выбрать наиболее подходящий сервис.
        Учитываем description, system_prompt и keywords.
        Возвращает имя сервиса или None.
        """
        if not services:
            return None
        description_lines = []
        for name, info in services.items():
            desc = info.get("description", "")
            system = info.get("system_prompt", "")
            kws = info.get("request_format", {}).get("keywords", [])
            description_lines.append(f"- {name}:\n  Description: {desc}\n  System prompt: {system}\n  Keywords: {', '.join(kws)}")
        prompt = (
                "Ты — помощник-оркестратор. Вот список доступных сервисов (имя: описание + ключевые слова):\n"
                + "\n".join(description_lines)
                + "\n\nЗапрос пользователя:\n"
                + query
                + "\n\nВыбери одно единственное имя сервиса (в точности как указано выше), "
                "который лучше всего подходит для обработки запроса. "
                "Если подходящего сервиса нет — напиши 'NONE'."
        )
        resp = self.run(prompt).strip()
        # normalize and try to find exact match
        # LLM may reply with extra text, so check for any service name substring
        resp_upper = resp.upper()
        for name in services.keys():
            if name.upper() in resp_upper:
                return name
        if resp_upper == "NONE":
            return None
        # fallback: if response equals one of names (case-insensitive)
        for name in services.keys():
            if resp.strip().lower() == name.lower():
                return name
        return None
