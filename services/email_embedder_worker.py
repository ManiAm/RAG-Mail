
import json
import re

from services.email_embedder_remote import remove_embed_email_thread, embed_email_thread
from db.session import SessionLocal
from db.models import Email


def embed_unprocessed_threads(collection_name, embed_model, chunk_size, dump_text_block):

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
        status, output = remove_embed_email_thread(collection_name, thread_id)
        if not status:
            print(f"Error: {output}")
            continue

        text_block = get_thread_text(emails)

        print(f"""[INFO] Embedding thread {thread_id}:
            Subject           : "{emails[0].subject}"
            Email Count       : {len(emails)}
            Attachments Count : {sum(len(e.attachments) for e in emails)}
            Thread Length     : {len(text_block)}""")

        status, output = embed_thread(emails,
                                      thread_id,
                                      text_block,
                                      collection_name,
                                      embed_model,
                                      chunk_size,
                                      dump_text_block)
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


def embed_thread(emails, thread_id, text_block, collection_name, embed_model, chunk_size, dump_text_block):

    metadata = {
        "type"              : "email",
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

    status, output = embed_email_thread(text_block,
                                        collection_name,
                                        embed_model,
                                        metadata,
                                        separators,
                                        chunk_size)
    if not status:
        return False, output

    # record only on successful embedding!
    save_thread_to_file(dump_text_block, text_block, metadata)

    return True, None


def save_thread_to_file(dump_text_block, text_block, metadata):

    with open(dump_text_block, "a", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("METADATA:\n")
        json.dump(metadata, f, indent=4)
        f.write("\n\nTEXT BLOCK:\n")
        f.write(text_block.strip())
        f.write("\n\n")
