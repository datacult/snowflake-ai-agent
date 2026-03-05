import os
import threading
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
from cortex_chat import ask_agent

load_dotenv()

app = App(
    token=os.environ['SLACK_BOT_TOKEN'],
    signing_secret=os.environ['SLACK_SIGNING_SECRET'],
)

# In-memory thread store: maps Slack thread_ts -> Cortex thread_id
# This ensures each Slack thread maintains its own conversation context
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


@app.event('app_mention')
def handle_mention(event, say, client):
    """Handle @mention of the bot. Always replies in the same Slack thread."""
    user_text  = event['text']
    channel_id = event['channel']
    slack_user = event['user']

    # Use the thread_ts if this is already inside a thread,
    # otherwise use the message ts to START a new thread
    slack_thread_ts = event.get('thread_ts') or event['ts']

    # Post thinking indicator IN the thread
    thinking = client.chat_postMessage(
        channel=channel_id,
        thread_ts=slack_thread_ts,
        text=f"<@{slack_user}> ⏳ Analyzing your question, this may take up to 60 seconds...",
    )
    thinking_ts = thinking['ts']

    # Get existing Cortex thread_id keyed to this Slack thread
    cortex_thread_id = thread_store.get(slack_thread_ts)

    # Append Slack formatting instruction to the prompt
    formatted_prompt = user_text + SLACK_FORMAT_INSTRUCTION

    def run_agent():
        try:
            result    = ask_agent(formatted_prompt, thread_id=cortex_thread_id)
            answer    = result['text']
            citations = result['citations']

            # Persist Cortex thread_id keyed to this Slack thread
            if result['thread_id']:
                thread_store[slack_thread_ts] = result['thread_id']

            # Append citations if present
            if citations:
                answer += '\n\n*Sources:* ' + ', '.join(citations)

            # Update the thinking message with the real answer (stays in thread)
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


@app.command('/ask')
def handle_command(ack, body, client):
    """Handle /ask slash command. Posts and replies in a thread."""
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

    # Post the question as the thread root message
    root = client.chat_postMessage(
        channel=channel_id,
        text=f"<@{slack_user}> asked: _{question}_",
    )
    slack_thread_ts = root['ts']

    # Post thinking indicator inside the thread
    thinking = client.chat_postMessage(
        channel=channel_id,
        thread_ts=slack_thread_ts,
        text=f"⏳ Analyzing your question, this may take up to 60 seconds...",
    )
    thinking_ts = thinking['ts']

    cortex_thread_id = thread_store.get(slack_thread_ts)
    formatted_prompt = question + SLACK_FORMAT_INSTRUCTION

    def run_agent():
        try:
            result    = ask_agent(formatted_prompt, thread_id=cortex_thread_id)
            answer    = result['text']
            citations = result['citations']

            if result['thread_id']:
                thread_store[slack_thread_ts] = result['thread_id']

            if citations:
                answer += '\n\n*Sources:* ' + ', '.join(citations)

            client.chat_update(
                channel=channel_id,
                ts=thinking_ts,
                text=answer,
            )

        except Exception as e:
            client.chat_update(
                channel=channel_id,
                ts=thinking_ts,
                text=f"⚠️ Error: {str(e)}",
            )

    threading.Thread(target=run_agent, daemon=True).start()


if __name__ == '__main__':
    print("Bot starting...")
    SocketModeHandler(
        app, os.environ['SLACK_APP_TOKEN']
    ).start()