from pydantic import BaseModel
from typing import List, Dict, Optional

class TaskRequest(BaseModel):
    prompt: str

class ServiceRegistration(BaseModel):
    name: str
    description: Optional[str] = ''
    system_prompt: Optional[str] = ''
    request_format: Optional[Dict] = {}
    endpoint: str