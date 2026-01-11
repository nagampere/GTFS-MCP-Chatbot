import json
from typing import Any, Dict, List

import anthropic
import streamlit as st


from utils.exceptions import AnthropicAPIError
from utils.usage_renderer import render_usage_and_cost
from utils.block_renderer import render_blocks
from utils.mcp_client import call_claude_with_motherduck_mcp
from utils.rag_examples import build_rag_system_message
from utils.loading_animation import show_loading_animation

# APIキー等の設定
ANTHROPIC_API_KEY = st.secrets["ANTHROPIC_API_KEY"]
MOTHERDUCK_TOKEN = st.secrets["MOTHERDUCK_TOKEN"]
# システムプロンプト・クックブック読み込み
system = open("prompts/system_gtfs.md").read()
cookbook = open("gtfs/cookbook.md").read()
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

# セッションステート初期化
if "messages" not in st.session_state:
    st.session_state.messages = []

# 初回（送信前）に使い方をチャットで表示
if not st.session_state.messages:
    intro_text = (
        "こんにちは！Claude API×MCPサーバー×Motherduckを基盤にしたAIチャットボット「**ハチ公のりものレポート**」です。"
        "GTFSデータを使って、路線や時刻表、運行情報の分析をお手伝いします。\n\n"
        "- 例1: 『東京駅から半径300m以内にいくつバス停がありますか？』\n"
        "- 例2: 『六本木駅からバスに乗ってどこまで行けますか？』\n"
        "- 例3: 『バス停「浅草雷門」についてのHTMLレポートを作成してください。』\n"
        "\n"
    )
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": {"usage": {}, "content": [{"type": "text", "text": intro_text}]},
        }
    )


# タイトルと説明
with main:
    st.title("🐶ハチ公のりものレポート🐶")
    st.image("image.png", width='stretch')


# トークン制限・タイムアウト設定の入力
with sidebar:
    st.header("設定")
    demo = st.toggle("【期間限定】デモ版を使用", value=True, key="demo_mode_toggle", help="ODPT開催期間中のみ有効なデモ版を使用します。")
    if demo:
        st.info("デモ版では、Claude APIとMotherduckの利用料金は開発者が負担します。")
    else:
        MOTHERDUCK_TOKEN = st.text_input("Motherduck Token", type="password", help="MotherduckのMCPトークンを入力してください。")
        ANTHROPIC_API_KEY = st.text_input("Anthropic API Key", type="password", help="AnthropicのAPIキーを入力してください。")
        CLAUDE_MODEL = st.selectbox("Claude Model", options=["claude-opus-4-5", "claude-sonnet-4-5", "claude-haiku-4-5"], index=1, help="使用するClaudeモデルを選択してください。")
    max_token = st.number_input("Max Tokens", value=10000, min_value=1, max_value=100000, step=1000)
    timeout = st.number_input("Timeout (seconds)", value=180.0, min_value=1.0, max_value=600.0, step=10.0)

# チャット履歴表示エリア  
with main:
    # 履歴の表示
    for i, message in enumerate(st.session_state.messages):
        avatar = "🐶" if message["role"] == "assistant" else None
        with st.chat_message(message["role"], avatar=avatar):
            if message["role"] == "user":
                st.markdown(message["content"])
            else:
                render_usage_and_cost(message["content"].get("usage", {}), PRICE_INPUT_PER_MTOK, PRICE_OUTPUT_PER_MTOK)
                render_blocks(message["content"].get("content", []), main, key_prefix=f"msg{i}_")

# ユーザ入力（Enterで送信しない: text_area + ボタン）
with main:
    st.divider()
    st.caption("Enterは改行、送信はボタン")
    with st.form("chat_form", clear_on_submit=True):
        prompt = st.text_area(
            "入力",
            placeholder="東京駅から半径300m以内にいくつバス停がありますか？",
            height=120,
            key="chat_prompt",
            label_visibility="collapsed",
        )
        send = st.form_submit_button("送信", type="primary")

if send:
    prompt = (prompt or "").strip()
    if not prompt:
        st.stop()

    with main:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="🐶"):
            try:
                loading = st.empty()
                show_loading_animation(loading)
                rag_context = build_rag_system_message(prompt, k=4, max_chars=6000)
                messages = [
                    {"role": "system", "content": system},
                    {"role": "system", "content": cookbook},
                ]
                if rag_context:
                    messages.append({"role": "system", "content": rag_context})

                messages.append({"role": "user", "content": prompt})
                response = call_claude_with_motherduck_mcp(
                    messages,
                    ANTHROPIC_API_KEY,
                    MOTHERDUCK_TOKEN,
                    CLAUDE_MODEL if not demo else "claude-sonnet-4-5",
                    TOOLS,
                    max_token,
                    timeout,
                )
                loading.empty()
            except AnthropicAPIError as e:
                try:
                    loading.empty()
                except Exception:
                    pass
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
                try:
                    loading.empty()
                except Exception:
                    pass
                st.error(f"Anthropic API呼び出しに失敗しました: {e}")
                st.stop()

            # レスポンスの表示
            st.session_state.messages.append({"role": "assistant", "content": response})
            # 使用量とコストの表示
            render_usage_and_cost(response.get("usage", {}), PRICE_INPUT_PER_MTOK, PRICE_OUTPUT_PER_MTOK)
            # ブロックのレンダリング
            blocks = response.get("content") or []
            render_blocks(blocks, main, key_prefix=f"msg{len(st.session_state.messages) - 1}_")

            # レンダリング後にリロードしてフォームをクリア
            st.rerun()