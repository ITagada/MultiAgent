import json
import requests

from typing import List, Dict, Optional

class OllamaLLM:
    def __init__(self, model="mistral-small:latest", host="http://192.168.6.97:11434", mode: str = "auto"):
        self.model = model
        self.host = host.rstrip("/")
        self.mode = mode

    def run(self, prompt: str) -> str:
        try:
            url = f"{self.host}/v1/chat/completions"
            payload = {
                'model': self.model,
                'messages': [{'role': 'user', 'content': prompt}],
                'stream': False
            }
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data['choices'][0]['message']['content']
        except Exception:
            try:
                url = f"{self.host}/api/generate"
                payload = {
                    'model': self.model,
                    'prompt': prompt
                }
                r = requests.post(url, json=payload, timeout=60, stream=False)
                r.raise_for_status()
                try:
                    data = r.json()
                    if isinstance(data, dict):
                        if 'choices' in data and data['choices']:
                            c = data['choices'][0]
                            if isinstance(c, dict) and 'message' in c:
                                return c['message'].get('content', '')
                        if 'response' in data:
                            return data['response']
                    return str(data)
                except ValueError:
                    return r.text
            except Exception as e:
                return f"[LLM error] {e}"

    def choose_service(self, services: Dict[str, Dict], query: str) -> Optional[str]:
        """
        Просим LLM выбрать наиболее подходящий сервис.
        Возвращает имя сервиса или None.
        """
        if not services:
            return None
        description_lines = []
        for name, info in services.items():
            desc = info.get("description", "")
            kws = info.get("request_format", {}).get("keywords", [])
            description_lines.append(f"- {name}: {desc}. keywords: {', '.join(kws)}")
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
