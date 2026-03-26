import os
import threading
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from aws_secrets import load_secrets
from cortex_chat import ask_agent
from thread_store import get_history, save_history

load_secrets("SlackAIBotSecret")

app = App(
    token=os.environ['SLACK_BOT_TOKEN'],
    signing_secret=os.environ['SLACK_SIGNING_SECRET'],
)

# Fetch bot's own user ID so the message handler can ignore @mention messages
# (those are already handled by app_mention)
bot_user_id = app.client.auth_test()['user_id']

# Thread state is persisted in DynamoDB (SlackBotThreadStore table).
# get_thread / put_thread replace the former in-memory dict.

# Slack formatting instruction appended to every prompt
SLACK_FORMAT_INSTRUCTION = (
    "\n\n[RESPONSE FORMAT - SLACK]\n"
    "You are responding inside Slack. Follow these rules strictly:\n"
    "- Do NOT reference charts, visualizations, or graphs — they cannot be rendered in Slack.\n"
    "- Present all data as plain text tables using this format:\n"
    "  Week of Jan 5:   $353,456\n"
    "  Week of Jan 12:  $310,234\n"
    "- Use bullet points or numbered lists for summaries.\n"
    "- Keep responses concise and scannable.\n"
    "- Bold key numbers using *asterisks* e.g. *$353,456*.\n"
    "- End with a 1-2 sentence insight summarizing the trend."
)

# CSV format instruction — used when the user explicitly asks for a CSV file.
# Asks Cortex to return insights first, then the raw CSV separated by a delimiter.
CSV_FORMAT_INSTRUCTION = (
    "\n\n[RESPONSE FORMAT - CSV + SLACK]\n"
    "Return your response in TWO parts separated by the exact delimiter: ---CSV---\n\n"
    "Part 1 (before the delimiter): Normal Slack-formatted insights.\n"
    "- Do NOT reference charts, visualizations, or graphs.\n"
    "- Use bullet points or numbered lists for summaries.\n"
    "- Bold key numbers using *asterisks* e.g. *$353,456*.\n"
    "- End with a 1-2 sentence insight summarizing the trend.\n\n"
    "Part 2 (after the delimiter): Raw CSV data only.\n"
    "- First line must be a header row with column names.\n"
    "- No markdown, no code blocks, no extra text — just the CSV rows.\n\n"
    "Example:\n"
    "Here are the results:\n"
    "- *$100k* in January\n"
    "---CSV---\n"
    "month,revenue\n"
    "January,100000\n"
)

CSV_KEYWORDS = ('csv file', 'as a csv', 'export csv', 'download csv', 'export as csv', 'in csv', 'as csv')


def _is_csv_request(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in CSV_KEYWORDS)


def _run_agent_in_thread(channel_id, slack_user, slack_thread_ts, prompt, client):
    """Post a thinking indicator, call the agent, then update with the answer."""
    thinking = client.chat_postMessage(
        channel=channel_id,
        thread_ts=slack_thread_ts,
        text=f"<@{slack_user}> ⏳ Analyzing your question, this may take up to 60 seconds...",
    )
    thinking_ts = thinking['ts']

    history       = get_history(slack_thread_ts)
    csv_requested = _is_csv_request(prompt)
    format_instr  = CSV_FORMAT_INSTRUCTION if csv_requested else SLACK_FORMAT_INSTRUCTION
    formatted_prompt = prompt + format_instr

    def run_agent():
        try:
            result    = ask_agent(formatted_prompt, history=history)
            answer    = result['text']
            citations = result['citations']

            # Save history using the original prompt (no format instructions) so
            # Cortex sees clean context on follow-ups. For CSV responses store
            # only the insights part, not the raw CSV rows.
            assistant_text = answer.split('---CSV---', 1)[0].strip() if csv_requested else answer
            save_history(slack_thread_ts, history + [
                {'role': 'user',      'content': [{'type': 'text', 'text': prompt}]},
                {'role': 'assistant', 'content': [{'type': 'text', 'text': assistant_text}]},
            ])

            if csv_requested:
                # Split the response into insights and CSV data
                parts    = answer.split('---CSV---', 1)
                insights = parts[0].strip()
                csv_data = parts[1].strip() if len(parts) > 1 else ''

                # Update the thinking message with the insights
                if citations:
                    insights += '\n\n*Sources:* ' + ', '.join(citations)
                client.chat_update(
                    channel=channel_id,
                    ts=thinking_ts,
                    text=f"<@{slack_user}> {insights}",
                )

                # Upload the CSV as a file in the same thread
                if csv_data:
                    client.files_upload_v2(
                        channel=channel_id,
                        thread_ts=slack_thread_ts,
                        filename="data.csv",
                        content=csv_data,
                        title="Raw Data Export",
                    )
            else:
                if citations:
                    answer += '\n\n*Sources:* ' + ', '.join(citations)

                client.chat_update(
                    channel=channel_id,
                    ts=thinking_ts,
                    text=f"<@{slack_user}> {answer}",
                )

        except Exception as e:
            client.chat_update(
                channel=channel_id,
                ts=thinking_ts,
                text=f"<@{slack_user}> ⚠️ Sorry, I ran into an error: {str(e)}",
            )

    threading.Thread(target=run_agent, daemon=True).start()


@app.event('app_mention')
def handle_mention(event, say, client):
    """Handle @mention of the bot. Always replies in the same Slack thread."""
    slack_thread_ts = event.get('thread_ts') or event['ts']
    _run_agent_in_thread(event['channel'], event['user'], slack_thread_ts, event['text'], client)


@app.event('message')
def handle_thread_reply(event, client):
    """
    Respond to plain replies in a thread the bot is already part of.
    Users do not need to @mention the bot — just reply in the thread.
    """
    # Ignore bot messages, edits, deletes, and any other subtypes
    if event.get('subtype') or event.get('bot_id'):
        return

    # Ignore @mentions — app_mention handles those to avoid double responses
    if f'<@{bot_user_id}>' in event.get('text', ''):
        return

    slack_thread_ts = event.get('thread_ts')

    # Only respond if this is a reply in a thread the bot started
    if not slack_thread_ts or not get_history(slack_thread_ts):
        return

    _run_agent_in_thread(event['channel'], event['user'], slack_thread_ts, event['text'], client)


@app.command('/ask')
def handle_command(ack, body, client):
    """
    Handle /ask slash command.
    - If used inside an existing bot thread: continues that conversation.
    - Otherwise: starts a new thread.
    """
    ack()

    question   = body.get('text', '').strip()
    channel_id = body['channel_id']
    slack_user = body['user_id']

    if not question:
        client.chat_postMessage(
            channel=channel_id,
            text=f"<@{slack_user}> Please provide a question. Usage: `/ask what was our CAC last week?`",
        )
        return

    # If /ask is typed inside an existing bot thread, continue that conversation
    slack_thread_ts = body.get('thread_ts')
    if slack_thread_ts and get_history(slack_thread_ts):
        _run_agent_in_thread(channel_id, slack_user, slack_thread_ts, question, client)
        return

    # Otherwise start a fresh thread
    root = client.chat_postMessage(
        channel=channel_id,
        text=f"<@{slack_user}> asked: _{question}_",
    )
    _run_agent_in_thread(channel_id, slack_user, root['ts'], question, client)


if __name__ == '__main__':
    print("Bot starting...")
    SocketModeHandler(
        app, os.environ['SLACK_APP_TOKEN']
    ).start()
