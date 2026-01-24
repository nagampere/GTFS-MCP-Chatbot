import streamlit as st
from typing import Any, Dict, Generator, Optional

def render_event_stream(stream_generator: Generator[Any, None, None]) -> Optional[Dict[str, Any]]:
    # Track tool uses and results for real-time display
    tool_use_containers = {}
    tool_result_containers = {}
    text_container = st.empty()
    accumulated_text = ""
    response = None

    # Consume the stream and handle events
    for chunk in stream_generator:
        if isinstance(chunk, str):
            # Text chunk for streaming display
            accumulated_text += chunk
            text_container.markdown(accumulated_text)
        elif isinstance(chunk, dict):
            event_type = chunk.get('event')
            if event_type == 'tool_use':
                # Display tool_use block immediately
                tool_block = chunk.get('block', {})
                tool_id = tool_block.get('id')
                tool_name = tool_block.get('name')
                
                # Create expander for this tool use
                if tool_id not in tool_use_containers:
                    with st.expander(f"🔧 {tool_name}", expanded=False):
                        st.json(tool_block)
                        tool_use_containers[tool_id] = True
            elif event_type == 'tool_result':
                # Display tool_result block immediately
                result_block = chunk.get('block', {})
                tool_use_id = result_block.get('tool_use_id')
                is_error = result_block.get('is_error', False)
                
                # Create expander for this tool result
                if tool_use_id not in tool_result_containers:
                    header = f"{'❌' if is_error else '✅'} Tool Result"
                    with st.expander(header, expanded=False):
                        st.write({"tool_use_id": tool_use_id, "is_error": is_error})
                        content_blocks = result_block.get('content', [])
                        for cb in content_blocks:
                            if isinstance(cb, dict) and cb.get('type') == 'text':
                                text = cb.get('text', '')
                                try:
                                    import json as json_module
                                    parsed = json_module.loads(text)
                                    st.json(parsed)
                                except (json_module.JSONDecodeError, ValueError):
                                    st.code(text)
                        tool_result_containers[tool_use_id] = True
            elif event_type == 'final_message':
                # Final message object
                response = chunk.get('message')
    
    return response
