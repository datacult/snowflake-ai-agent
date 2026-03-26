import os, json, requests


def get_agent_url():
    account = os.environ['SNOWFLAKE_ACCOUNT']
    db      = os.environ['AGENT_DATABASE']
    schema  = os.environ['AGENT_SCHEMA']
    name    = os.environ['AGENT_NAME']
    return (
        f'https://{account}.snowflakecomputing.com'
        f'/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run'
    )


def ask_agent(prompt: str, history: list = None) -> dict:
    """
    Call the Cortex Agent and return:
      { 'text': str, 'citations': list[str] }

    history: list of previous {'role': ..., 'content': [...]} messages for this thread.
    The current prompt is appended as the final user message before sending.
    """
    headers = {
        'Authorization':    f'Bearer {os.environ["SNOWFLAKE_PAT"]}',
        'Content-Type':     'application/json',
        'Accept':           'text/event-stream',
        'X-Snowflake-Role': 'CORTEX_AGENT_USER_ROLE',
    }

    messages = list(history or [])
    messages.append({'role': 'user', 'content': [{'type': 'text', 'text': prompt}]})

    body = {
        'messages': messages,
        'stream':   True,
    }

    resp = requests.post(
        get_agent_url(),
        headers=headers,
        json=body,
        stream=True,
        timeout=300,
    )
    if not resp.ok:
        print(f"Snowflake error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return parse_sse(resp)


def parse_sse(response) -> dict:
    """
    Parse the SSE stream from Cortex Agent.

    Snowflake streams two kinds of events:
      A) Incremental thinking/planning chunks:
         {"content_index": N, "sequence_number": N, "text": "..."}
         --> These are internal reasoning. We IGNORE them.

      B) Final content block (the real answer):
         {"content": [{"type": "text", "text": "..."}, ...], "sequence_number": N}
         --> We extract the answer from here.

    We only return the final content block. If no final block arrives
    (e.g. error), we surface the error message instead.
    """
    final_text = None
    citations  = []
    errors     = []

    for raw in response.iter_lines():
        if not raw:
            continue
        line = raw.decode('utf-8') if isinstance(raw, bytes) else raw
        if not line.startswith('data:'):
            continue
        payload = line[5:].strip()
        if not payload or payload == '[DONE]':
            continue

        try:
            evt = json.loads(payload)
        except json.JSONDecodeError:
            continue

        # ── Ignore incremental thinking/planning chunks ───────────────────
        # These have {"content_index": N, "text": "..."} — internal reasoning
        if 'content_index' in evt and 'text' in evt:
            continue  # skip — this is the thinking text, not the answer

        # ── Final content block ───────────────────────────────────────────
        # {"content": [...], "sequence_number": N}
        if 'content' in evt and isinstance(evt['content'], list):
            for block in evt['content']:
                if not isinstance(block, dict):
                    continue

                # Error inside content block
                if 'json' in block and isinstance(block['json'], dict):
                    err = block['json'].get('error', {})
                    if err:
                        errors.append(err.get('message', 'Unknown Snowflake error'))
                    continue

                # Internal thinking block — skip
                if 'thinking' in block:
                    continue

                # The real answer
                if block.get('type') == 'text':
                    final_text = block.get('text', '').strip()
                    for ann in block.get('annotations', []):
                        title = ann.get('doc_title') or ann.get('text', '')
                        if title:
                            citations.append(title)

    # Surface errors if no final answer arrived
    if errors and not final_text:
        return {
            'text':      f"⚠️ Agent error: {errors[0]}",
            'citations': [],
        }

    return {
        'text':      final_text or '(no response)',
        'citations': citations,
    }