import json
from typing import Any, List
import anthropic
import warnings

from utils.exceptions import AnthropicAPIError

try:
    # pydantic v2
    from pydantic.warnings import PydanticSerializationUnexpectedValue
except Exception:  # pragma: no cover
    PydanticSerializationUnexpectedValue = Warning

def _to_dict(obj: Any) -> Any:
    if isinstance(obj, dict):
        return obj

    # Prefer pydantic v2 JSON-mode dumps to avoid Union serialization warnings/errors
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        for kwargs in (
            {"mode": "json", "warnings": "none"},
            {"mode": "json"},
            {"warnings": "none"},
            {},
        ):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", PydanticSerializationUnexpectedValue)
                    return model_dump(**kwargs)
            except TypeError:
                continue
            except Exception:
                break

    model_dump_json = getattr(obj, "model_dump_json", None)
    if callable(model_dump_json):
        for kwargs in (
            {"warnings": "none"},
            {},
        ):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", PydanticSerializationUnexpectedValue)
                    return json.loads(model_dump_json(**kwargs))
            except TypeError:
                continue
            except Exception:
                break

    dict_method = getattr(obj, "dict", None)
    if callable(dict_method):
        try:
            return dict_method()
        except Exception:
            pass

    try:
        return json.loads(json.dumps(obj, default=lambda x: getattr(x, "__dict__", str(x))))
    except Exception:
        return {"_raw": str(obj)}


def _raise_anthropic_error(e: Exception) -> None:
    status_code = getattr(e, "status_code", None)
    request_id = getattr(e, "request_id", None)
    body = getattr(e, "body", None)
    message = str(getattr(e, "message", None) or str(e))

    # anthropic.APIStatusError などは response/body を持つことがある
    resp = getattr(e, "response", None)
    if not request_id and resp is not None:
        try:
            headers = getattr(resp, "headers", None) or {}
            request_id = headers.get("request-id") or headers.get("x-request-id")
        except Exception:
            pass

    raise AnthropicAPIError(
        status_code=int(status_code) if status_code is not None else 0,
        message=message,
        request_id=str(request_id) if request_id else None,
        body=_to_dict(body) if body is not None else None,
        text=message,
    ) from e

def call_claude_with_motherduck_mcp(prompt: str, anthropic_key: str, motherduck_token : str, tools: List, max_token: int=1000, timeout: int|float = 240) -> dict:

    client = anthropic.Anthropic(api_key=anthropic_key, timeout=timeout)

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=max_token,
            messages=[{"role": "user", "content": prompt}],
            extra_headers={
                # MCP connector beta header (required)
                "anthropic-beta": "mcp-client-2025-11-20",
            },
            extra_body={
                "mcp_servers": [
                    {
                        "type": "url",
                        "name": "motherduck",
                        "url": "https://api.motherduck.com/mcp",
                        "authorization_token": motherduck_token,
                    }
                ],
                "tools": tools,
                "tool_choice": {"type": "auto"},
            },
        )
    except Exception as e:
        _raise_anthropic_error(e)

    return _to_dict(msg)