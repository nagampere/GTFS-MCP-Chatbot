import json
from typing import Any, Dict, List

import anthropic
import streamlit as st


from utils.exceptions import AnthropicAPIError
from utils.usage_renderer import render_usage_and_cost
from utils.block_renderer import render_blocks
from utils.mcp_client import call_claude_with_motherduck_mcp

# APIキー等の設定
ANTHROPIC_API_KEY = st.secrets["ANTHROPIC_API_KEY"]
MOTHERDUCK_TOKEN = st.secrets["MOTHERDUCK_TOKEN"]
# トークン制限・タイムアウト設定
max_token = 10000
timeout = 240.0
# 料金（USD / 1M tokens）
PRICE_INPUT_PER_MTOK = 3.0
PRICE_OUTPUT_PER_MTOK = 15.0
# MCP用ツール定義
TOOLS = [
    # MotherDuck MCP（リモート）
    {
        "type": "mcp_toolset",
        "mcp_server_name": "motherduck",
    },
    # 表示用ツール
    {
        "name": "html_viewer",
        "description": "HTMLを表示します。",
        "input_schema": {
            "type": "object",
            "properties": {
                "html": {
                    "type": "string",
                    "description": "HTML Document",
                }
            },
            "required": ["html"],
        },
    },
]

# チャットエリアとプレビューエリアを分離
main = st.container()
sidebar = st.sidebar

# 履歴の初期化
if "messages" not in st.session_state:
    st.session_state.messages = []



# タイトルと説明
with main:
    st.title("MotherDuck + Anthropic MCP チャットデモ")
    st.markdown(
        """
        - MotherDuck上のデータをAnthropic Claudeモデルに問い合わせるチャットデモです。
        - MCP (Model Connector Protocol) クライアントを使用して、MotherDuckをClaudeモデルに接続しています。
        - ツール呼び出しやHTML表示もサポートしています。
        """
    )

# チャット履歴表示エリア
with sidebar:
    st.subheader("チャットエリア")

    # 履歴の表示
    for i, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            if message["role"] == "user":
                st.markdown(message["content"])
            else:
                render_usage_and_cost(message["content"].get("usage", {}), PRICE_INPUT_PER_MTOK, PRICE_OUTPUT_PER_MTOK)
                render_blocks(message["content"].get("content", []), main, key_prefix=f"msg{i}_")

# ユーザ入力
if prompt := st.chat_input("どんなテーブルが存在しますか？ MotherDuck上のデータを使って教えてください。"):
    with sidebar:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        try:
            response = call_claude_with_motherduck_mcp(prompt, ANTHROPIC_API_KEY, MOTHERDUCK_TOKEN, TOOLS, max_token, timeout)
        except AnthropicAPIError as e:
            with st.chat_message("assistant"):
                st.error(str(e))
                with st.expander("詳細"):
                    st.write({
                        "status_code": e.status_code,
                        "request_id": e.request_id,
                    })
                    if e.body is not None:
                        st.json(e.body)
                    elif e.text:
                        st.code(e.text)
            st.stop()
        except Exception as e:
            with st.chat_message("assistant"):
                st.error(f"Anthropic API呼び出しに失敗しました: {e}")
            st.stop()

        with st.chat_message("assistant"):
            st.session_state.messages.append({"role": "assistant", "content": response})
            render_usage_and_cost(response.get("usage", {}), PRICE_INPUT_PER_MTOK, PRICE_OUTPUT_PER_MTOK)
            blocks = response.get("content") or []
            render_blocks(blocks, main, key_prefix=f"msg{len(st.session_state.messages) - 1}_")

