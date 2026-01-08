from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class AnthropicAPIError(RuntimeError):
    status_code: int
    message: str
    request_id: Optional[str] = None
    body: Any = None
    text: str = ""

    def __str__(self) -> str:
        rid = self.request_id or "unknown"
        return f"Anthropic API error {self.status_code}: {self.message} (request_id={rid})"