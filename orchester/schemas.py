from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class TaskRequest(BaseModel):
    prompt: str

class ServiceRegistration(BaseModel):
    uuid: str
    name: str
    description: Optional[str] = ''
    system_prompt: Optional[str] = ''
    request_format: Optional[Dict] = {}
    endpoint: str

class RuleIn(BaseModel):
    name: str
    rule: str
    is_system: Optional[bool] = False
    position: Optional[str] = None

class RuleOut(BaseModel):
    id: int
    name: str
    rule: str
    is_system: Optional[bool] = False
    position: Optional[str] = None

