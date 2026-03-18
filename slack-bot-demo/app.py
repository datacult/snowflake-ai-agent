import os
import threading
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from aws_secrets import load_secrets
from cortex_chat import ask_agent

load_secrets("SlackAIBotSecret")

app = App(
    token=os.environ['SLACK_BOT_TOKEN'],
    signing_secret=os.environ['SLACK_SIGNING_SECRET'],
)

# Fetch bot's own user ID so the message handler can ignore @mention messages
# (those are already handled by app_mention)
bot_user_id = app.client.auth_test()['user_id']

# In-memory thread store: maps Slack thread_ts -> { cortex_thread_id, last_message_id }
thread_store: dict = {}

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


def _run_agent_in_thread(channel_id, slack_user, slack_thread_ts, prompt, client):
    """Post a thinking indicator, call the agent, then update with the answer."""
    thinking = client.chat_postMessage(
        channel=channel_id,
        thread_ts=slack_thread_ts,
        text=f"<@{slack_user}> ⏳ Analyzing your question, this may take up to 60 seconds...",
    )
    thinking_ts = thinking['ts']

    state            = thread_store.get(slack_thread_ts, {})
    cortex_thread_id = state.get('cortex_thread_id')
    last_message_id  = state.get('last_message_id')
    formatted_prompt = prompt + SLACK_FORMAT_INSTRUCTION

    def run_agent():
        try:
            result    = ask_agent(formatted_prompt, thread_id=cortex_thread_id, last_message_id=last_message_id)
            answer    = result['text']
            citations = result['citations']

            if result['thread_id']:
                thread_store[slack_thread_ts] = {
                    'cortex_thread_id': result['thread_id'],
                    'last_message_id':  result.get('message_id'),
                }

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
    if not slack_thread_ts or slack_thread_ts not in thread_store:
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
    if slack_thread_ts and slack_thread_ts in thread_store:
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
