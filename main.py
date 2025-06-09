
import os
import sys
import threading
import time
import argparse

from datetime import datetime
from datetime import timezone
from sqlalchemy import desc
from db.session import init_db
from db.session import SessionLocal
from db.models import Email

from services.email_loader_gmail import Email_loader_Gmail
from services.email_loader_mbox import Email_loader_mbox
from services.rag_talk_remote import load_model, create_collection
from services.email_embedder_worker import embed_thread_start


def run_pipeline(source, mailbox, llm_model, embed_model, chunk_size, collection_name, dump_text_block):

    if os.path.exists(dump_text_block):
        os.remove(dump_text_block)

    init_db()

    print(f"Loading embedding model: {embed_model}...")
    status, output = load_model([embed_model])
    if not status:
        print(f"Error: caanot load model: {output}")
        sys.exit(1)

    create_collection(collection_name, embed_model)

    if source == "gmail":
        poll_t = threading.Thread(target=email_polling_worker, daemon=True)
        poll_t.start()
    elif source == "mbox":
        poll_t = threading.Thread(target=email_loader_worker, args=(mailbox,), daemon=True)
        poll_t.start()
    else:
        print(f"Error: invalid source {source}")
        sys.exit(1)

    embed_t = threading.Thread(target=embedding_worker, args=(llm_model, embed_model, collection_name, chunk_size, dump_text_block), daemon=True)
    embed_t.start()

    poll_t.join()
    embed_t.join()


def email_polling_worker():

    while True:

        session = SessionLocal()
        latest_email = session.query(Email).order_by(desc(Email.date)).first()
        session.close()

        last_seen = None
        if latest_email and latest_email.date:
            last_seen = latest_email.date
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)

        print(f"[{datetime.now()}] Fetching emails since: {last_seen}")

        gmail = Email_loader_Gmail()
        gmail.load_emails(since=last_seen, max_results=1000)

        time.sleep(20)


def email_loader_worker(mailbox):

    unix = Email_loader_mbox(mailbox)
    unix.load_emails()


def embedding_worker(llm_model, embed_model, collection_name, chunk_size, dump_text_block):

    while True:

        session = SessionLocal()

        # Get thread_ids where at least one email is not embedded
        thread_ids = session.query(Email.thread_id).filter(Email.is_embedded == False).distinct().all()

        if not thread_ids:

            print("[INFO] No pending email threads found for embedding.")

        else:

            for (thread_id,) in thread_ids:

                emails = session.query(Email).filter(Email.thread_id == thread_id).order_by(Email.date).all()
                if not emails:
                    continue

                status, output = embed_thread_start(
                    emails,
                    thread_id,
                    llm_model,
                    embed_model,
                    collection_name,
                    chunk_size,
                    dump_text_block)

                if not status:
                    print(output)
                    continue

                try:

                    # Mark all emails in this thread as embedded
                    for email in emails:
                        email.is_embedded = True

                    session.commit()

                except Exception as e:
                    print(f"[ERROR] Failed to embed thread {thread_id}: {e}")
                    continue

        session.close()

        time.sleep(10)


def parse_arguments():

    parser = argparse.ArgumentParser(
        description="RAG-Mail: Process and embed emails from a local mailbox file or directly from Gmail."
    )

    parser.add_argument(
        '--source',
        choices=['gmail', 'mbox'],
        default='mbox',
        help="Specify the email source: 'gmail' for OAuth Gmail access or 'mbox' to load from a local mailbox file."
    )

    parser.add_argument(
        '--mailbox',
        type=str,
        metavar='PATH',
        help="Path to the MBOX email file (required if source is 'mbox')."
    )

    parser.add_argument(
        '--llm_model',
        type=str,
        default='llama3:8b',
        help="Name of the LLM model to use."
    )

    parser.add_argument(
        '--embed_model',
        type=str,
        default='bge-large-en-v1.5',
        help="Name of the embedding model to use."
    )

    parser.add_argument(
        '--chunk_size',
        type=int,
        help="Number of characters per chunk when splitting emails."
    )

    parser.add_argument(
        '--collection_name',
        type=str,
        help="Qdrant collection name (defaults to 'email_threads_<embed_model>')."
    )

    parser.add_argument(
        '--dump_text_block',
        type=str,
        default='emails_dump.txt',
        help="File path to save raw email thread text blocks (default: emails_dump.txt)."
    )

    args = parser.parse_args()

    # mailbox path must be set if source is 'mbox'
    if args.source == 'mbox' and not args.mailbox:
        parser.error("--mailbox is required when source is 'mbox'.")

    # default collection name if not provided
    if not args.collection_name:
        args.collection_name = f"email_threads_{args.embed_model}"

    return args


if __name__ == "__main__":

    parser = parse_arguments()

    run_pipeline(source=parser.source,
                 mailbox=parser.mailbox,
                 llm_model=parser.llm_model,
                 embed_model=parser.embed_model,
                 chunk_size=parser.chunk_size,
                 collection_name=parser.collection_name,
                 dump_text_block=parser.dump_text_block)
