
import os
import threading
import time
import json
import re

from datetime import datetime
from sqlalchemy import desc
from db.session import init_db
from db.session import SessionLocal
from db.models import Email

from services.email_loader_gmail import Email_loader_Gmail
from services.email_loader_unix import Email_loader_Unix
from services.email_embedder import create_collection, remove_embed_email_thread, embed_email_thread

EMBED_MODEL = "bge-large-en-v1.5"
CHUNK_SIZE =  1800
COLLECTION_NAME = "email_threads"
DUMP_TEXT_BLOCK = "emails_dump.txt"


def run_pipeline(source="gmail"):

    if os.path.exists(DUMP_TEXT_BLOCK):
        os.remove(DUMP_TEXT_BLOCK)

    init_db()
    create_collection(COLLECTION_NAME, EMBED_MODEL)

    poll_t = threading.Thread(target=email_polling_worker, args=(source,), daemon=True)
    embed_t = threading.Thread(target=embedding_worker, daemon=True)

    poll_t.start()
    embed_t.start()

    poll_t.join()
    embed_t.join()


def email_polling_worker(source):

    while True:

        session = SessionLocal()
        latest_email = session.query(Email).order_by(desc(Email.date)).first()
        session.close()

        last_seen = None
        if latest_email and latest_email.date:
            last_seen = latest_email.date

        print(f"[{datetime.now()}] Fetching emails since: {last_seen}")

        if source == "gmail":
            gmail = Email_loader_Gmail()
            gmail.load_emails(since=last_seen, max_results=1000)
        else:
            unix = Email_loader_Unix()
            unix.load_emails(since=last_seen)

        time.sleep(20)


def embedding_worker():

    while True:
        embed_unprocessed_threads()
        time.sleep(10)


def embed_unprocessed_threads():

    session = SessionLocal()

    # Get thread_ids where at least one email is not embedded
    thread_ids = session.query(Email.thread_id).filter(Email.is_embedded == False).distinct().all()

    if not thread_ids:
        print("[INFO] No pending email threads found for embedding.")
        session.close()
        return

    for (thread_id,) in thread_ids:

        emails = session.query(Email).filter(Email.thread_id == thread_id).order_by(Email.date).all()
        if not emails:
            continue

        # Remove old embeddings for this thread
        status, output = remove_embed_email_thread(COLLECTION_NAME, thread_id)
        if not status:
            print(f"Error: {output}")
            continue

        text_block = get_thread_text(emails)

        print(f"""[INFO] Embedding thread {thread_id}:
            Subject           : "{emails[0].subject}"
            Email Count       : {len(emails)}
            Attachments Count : {sum(len(e.attachments) for e in emails)}
            Thread Length     : {len(text_block)}""")

        status, output = embed_thread(emails, thread_id, text_block)
        if not status:
            print(f"Error: {output}")
            continue

        try:

            # Mark all emails in this thread as embedded
            for email in emails:
                email.is_embedded = True

            session.commit()

        except Exception as e:
            print(f"[ERROR] Failed to embed thread {thread_id}: {e}")

    session.close()


def get_thread_text(emails):

    text_block_list = []

    for i, email in enumerate(emails, 1):

        subject = email.subject.strip() if email.subject else "(no subject)"

        part = (
            f"--- Email {i} ---\n"
            f"Date: {email.date}\n"
            f"Subject: {subject}\n\n"
            f"{email.body.strip()}"
        )

        for att in email.attachments:
            if att.text_content:
                part += (
                    f"\n\n--- Begin Attachment: {att.filename} ({att.extension}) ---\n"
                    f"{att.text_content.strip()}\n"
                    f"--- End Attachment ---"
                )

        part += "\n--- End Email ---"
        text_block_list.append(part)

    text_block = "\n\n".join(text_block_list)

    return remove_links(text_block)


def remove_links(text):

    url_pattern = r'https?://\S+|www\.\S+|ftp://\S+'
    return re.sub(url_pattern, '', text)


def embed_thread(emails, thread_id, text_block):

    metadata = {
        "thread_id"         : thread_id,
        "subject"           : emails[0].subject,
        "sender"            : emails[0].sender,
        "email_count"       : len(emails),
        "attachments_count" : sum(len(e.attachments) for e in emails),
        "first_email_date"  : str(min(e.date for e in emails if e.date)),
        "last_email_date"   : str(max(e.date for e in emails if e.date)),
        "text_block_len"    : len(text_block)
    }

    separators = [
        "\n--- End Email ---",        # Primary: split cleanly at the end of each email
        "--- Email",                  # Secondary: split at the start of each email (redundant with End, but safe)
        "\n\n--- Begin Attachment",   # Split before large attachments to isolate them
        "\n\n",                       # Paragraph boundary (typical in email bodies)
        "\n",                         # Line break (for denser formatting)
        " ",
        ""
    ]

    status, output = embed_email_thread(text_block, COLLECTION_NAME, EMBED_MODEL, metadata, separators, CHUNK_SIZE)
    if not status:
        return False, output

    # record only on successful embedding!
    save_thread_to_file(text_block, metadata)

    return True, None


def save_thread_to_file(text_block, metadata):

    with open(DUMP_TEXT_BLOCK, "a", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("METADATA:\n")
        json.dump(metadata, f, indent=4)
        f.write("\n\nTEXT BLOCK:\n")
        f.write(text_block.strip())
        f.write("\n\n")


if __name__ == "__main__":

    run_pipeline(source="gmail")  # or "unix"
