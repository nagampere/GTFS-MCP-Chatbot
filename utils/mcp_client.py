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


def _split_system_messages(messages: list[dict]) -> tuple[str, list[dict]]:
    system_parts: list[str] = []
    filtered: list[dict] = []

    for m in messages or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")

        if role == "system":
            if isinstance(content, str) and content.strip():
                system_parts.append(content.strip())
            continue

        # Anthropic Messages API expects roles like "user" / "assistant"
        filtered.append(m)

    return "\n\n".join(system_parts).strip(), filtered

def call_claude_with_motherduck_mcp(
        messages: list[dict],
        client: anthropic.Anthropic, 
        claude_model: str,
        max_token: int,
        tools: List,
        mcp_servers: list[dict],
    ) -> Any:
    '''
    MCP (MotherDuck Connector for Claude) を使用して、Anthropic Claude モデルにメッセージを送信し、ストリーミングレスポンスを受け取ります。
    Args:
        messages (list[dict]): 送信するメッセージのリスト。各メッセージは辞書形式で、'role' と 'content' キーを含む必要があります。
        client (anthropic.Anthropic): Anthropic API クライアントのインスタンス。
        claude_model (str): 使用するClaudeモデルの名前。
        max_token (int): レスポンスで生成される最大トークン数。
        tools (List): MCPで使用するツールのリスト。
        mcp_servers (list[dict]): MCPサーバーの設定リスト。
    '''


    system, filtered_messages = _split_system_messages(messages)
    if not filtered_messages:
        raise ValueError("messages must include at least one non-system message")

    try:
        with client.messages.stream(
            model=claude_model,
            max_tokens=max_token,
            system=system or None,
            messages=filtered_messages,
            extra_headers={
                # MCP connector beta header (required)
                "anthropic-beta": "mcp-client-2025-11-20",
            },
            extra_body={
                "mcp_servers": mcp_servers,
                "tools": tools,
                "tool_choice": {"type": "auto"},
            },
        ) as stream:
            # Process all stream events
            for event in stream:
                event_type = getattr(event, 'type', None)
                
                # Handle content_block_start for tool_use, mcp_tool_use, and mcp_tool_result
                if event_type == 'content_block_start':
                    content_block = getattr(event, 'content_block', None)
                    if content_block:
                        block_type = getattr(content_block, 'type', None)
                        if block_type in ('tool_use', 'mcp_tool_use'):
                            # Yield tool_use block immediately for real-time display
                            tool_block = {
                                'type': block_type,
                                'id': getattr(content_block, 'id', None),
                                'name': getattr(content_block, 'name', None),
                                'input': getattr(content_block, 'input', {}),
                                'server_name': getattr(content_block, 'server_name', None),
                            }
                            yield {'event': 'tool_use', 'block': tool_block}
                        elif block_type in ('tool_result', 'mcp_tool_result'):
                            # Yield tool_result block immediately for real-time display
                            result_block = {
                                'type': block_type,
                                'tool_use_id': getattr(content_block, 'tool_use_id', None),
                                'is_error': getattr(content_block, 'is_error', False),
                                'content': getattr(content_block, 'content', []),
                            }
                            yield {'event': 'tool_result', 'block': result_block}
                
                # Handle text deltas for streaming text display
                elif event_type == 'content_block_delta':
                    delta = getattr(event, 'delta', None)
                    if delta and getattr(delta, 'type', None) == 'text_delta':
                        text = getattr(delta, 'text', '')
                        if text:
                            yield text
            
            # Yield the final message object after streaming completes
            final_message = stream.get_final_message()
            yield {'event': 'final_message', 'message': _to_dict(final_message)}

    except Exception as e:
        _raise_anthropic_error(e)
