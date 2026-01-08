import json
from typing import Any
import streamlit as st
import streamlit.components.v1 as components

def _try_parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _render_tool_result_text(text: str) -> None:
    parsed = _try_parse_json(text)
    if parsed is not None:
        st.json(parsed)
    else:
        st.code(text)


def render_blocks(blocks: list[Any], main, key_prefix: str = "") -> None:
    pending_tool_uses: dict[str, dict[str, Any]] = {}

    for idx, block in enumerate(blocks):
        if not isinstance(block, dict):
            continue

        btype = block.get("type")

        # 1) Plain text
        if btype == "text":
            st.markdown(block.get("text", ""))
            continue

        # 2) Tool call (model -> client)
        if btype == "tool_use":
            tool_name = block.get("name")
            tool_use_id = block.get("id")
            tool_input = block.get("input") if isinstance(block.get("input"), dict) else {}

            if isinstance(tool_use_id, str):
                pending_tool_uses[tool_use_id] = {
                    "name": tool_name,
                    "input": tool_input,
                }

            # html_viewer: inputにhtmlが入るケース/入らないケースの両対応
            if tool_name == "html_viewer":
                html = tool_input.get("html")
                with main:
                    if isinstance(html, str) and html.strip():
                        tab1, tab2 = st.tabs(["プレビュー", "ソースコード"])
                        with tab1:
                            components.html(html, height=640, scrolling=True)
                        with tab2:
                            st.markdown(f"```html\n{html}\n```")
                        st.download_button(
                            label="HTMLをダウンロード",
                            data=html,
                            file_name="output.html",
                            mime="text/html",
                            key=f"{key_prefix}html_download_{tool_use_id or idx}",
                        )
                    else:
                        st.markdown("（html_viewer が呼び出されました。入力HTMLが無いので結果待ちです）")
            else:
                # デバッグ用に最低限表示
                with st.expander(f"tool_use: {tool_name}"):
                    st.json({
                        "id": tool_use_id,
                        "name": tool_name,
                        "input": tool_input,
                    })
            continue

        # 3) Tool result (client/tool -> model) 互換
        if btype in ("mcp_tool_result", "tool_result"):
            tool_use_id = block.get("tool_use_id")
            is_error = block.get("is_error")
            header = "tool_result"
            if isinstance(tool_use_id, str) and tool_use_id in pending_tool_uses:
                header = f"tool_result: {pending_tool_uses[tool_use_id].get('name')}"

            with st.expander(header, expanded=False):
                st.write({
                    "tool_use_id": tool_use_id,
                    "is_error": is_error,
                })

                content_blocks = block.get("content") if isinstance(block.get("content"), list) else []
                for cb in content_blocks:
                    if isinstance(cb, dict) and cb.get("type") == "text":
                        text = cb.get("text")
                        if isinstance(text, str):
                            _render_tool_result_text(text)
            continue
            
        # 4) MCP Tool use (client -> model)
        if btype == "mcp_tool_use":
            tool_name = block.get("name")
            tool_input = block.get("input") if isinstance(block.get("input"), dict) else {}
            # デバッグ用に最低限表示
            with st.expander(f"mcp_tool_use: {tool_name}"):
                st.json({
                    "name": tool_name,
                    "input": tool_input,
                })
            continue

        # Fallback
        with st.expander(f"unhandled block: {btype}"):
            st.json(block)