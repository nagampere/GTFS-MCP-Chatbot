import streamlit as st

def _extract_usage(usage: dict) -> tuple[int, int]:
    if not isinstance(usage, dict):
        return 0, 0
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    return int(input_tokens or 0), int(output_tokens or 0)


def _estimate_cost_usd(input_tokens: int, output_tokens: int, price_input: int|float, price_output: int|float) -> float:
    return (input_tokens * price_input + output_tokens * price_output) / 1_000_000.0


def render_usage_and_cost(usage: dict, price_input: int|float, price_output: int|float) -> None:
    input_tokens, output_tokens = _extract_usage(usage)
    if input_tokens == 0 and output_tokens == 0:
        return
    cost = _estimate_cost_usd(input_tokens, output_tokens, price_input, price_output)
    st.caption(f"tokens: input={input_tokens}, output={output_tokens} / 概算=${cost:.6f}")
