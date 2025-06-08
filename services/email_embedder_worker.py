
import re
import json
import difflib

from services.email_embedder_remote import remove_embed_email_thread, embed_email_thread

quoted_reply_patterns = [
    r"On .+?wrote:",                    # Gmail-style replies
    r"From: .+",                        # Outlook
    r"Sent: .+",
    r"To: .+",
    r"Subject: .+",
    r"---+ ?Forwarded message ?---+",   # Forwarded
    r"----+ Original Message ----+",
]


def embed_thread_start(emails, thread_id, collection_name, embed_model, chunk_size, dump_text_block):

    # Remove old embeddings for this thread
    status, output = remove_embed_email_thread(collection_name, thread_id)
    if not status:
        return False, f"Error: {output}"

    text_block = get_thread_text(emails)
    if not text_block:
        return True, None

    print(f"""[INFO] Embedding thread {thread_id}:
        Subject           : "{emails[0].subject}"
        Email Count       : {len(emails)}
        Attachments Count : {sum(len(e.attachments) for e in emails)}
        Thread Length     : {len(text_block)}""")

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
        "===== End Email Thread =====",   # End marker for the entire email thread
        "===== Begin Email Thread =====", # Start marker for the entire email thread
        "--- End Email ---",              # End of an individual email within the thread
        "--- Email",                      # Start of an individual email (used for redundancy)
        "--- Begin Attachment",           # Start of an attachment section
        "\n\n",                           # Paragraph boundary (typical in email bodies)
        "\n",                             # Line break (for denser formatting)
        " ",                              # Word boundary (used when no larger separator found)
        ""                                # Character-level fallback (last resort for splitting)
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


def get_thread_text(emails):

    subject = emails[0].subject.strip() if emails[0].subject else "(no subject)"

    text_block_list = []

    for i, email in enumerate(emails):

        if i == 0:
            body = email.body.strip()
        else:
            body = remove_quoted_body(i, emails).strip()

        if not body:
            continue

        part = (
            f"--- Email {i+1} ---\n"
            f"Date: {email.date}\n\n"
            f"{body}"
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

    if not text_block_list:
        return ""

    text_block = "\n\n".join(text_block_list)

    text_block = (
        f"===== Begin Email Thread =====\n\n"
        f"Subject: {subject}\n\n"
        f"{text_block}\n\n"
        f"===== End Email Thread ====="
    )

    return remove_links(text_block)


def remove_quoted_body(idx, emails):

    c_email = emails[idx]
    body = c_email.body
    in_reply_to = c_email.in_reply_to

    referenced_email = next((e for e in emails if e.id == in_reply_to), None)
    if not referenced_email:
        return body

    for pattern in quoted_reply_patterns:

        match = re.search(pattern, body, re.IGNORECASE)
        if not match:
            continue

        current_body = body[:match.start()].strip()
        quoted_body = body[match.end():].strip()
        body_original = referenced_email.body.strip()

        similarity = difflib.SequenceMatcher(None, quoted_body, body_original).ratio()

        if similarity >= 0.8:
            return current_body

        return body

    return body


def remove_links(text):

    url_pattern = r'https?://\S+|www\.\S+|ftp://\S+'
    return re.sub(url_pattern, '', text)


def save_thread_to_file(dump_text_block, text_block, metadata):

    header = (
        "============================================\n"
        "=============== Email Thread ===============\n"
        "============================================\n\n"
    )

    with open(dump_text_block, "a", encoding="utf-8") as f:

        f.write(header)
        f.write("METADATA:\n")
        json.dump(metadata, f, indent=4)

        indented_text = "\n".join(
            (" " * 9 + line if line.strip() else "") for line in text_block.strip().splitlines()
        )

        f.write("\n\n")
        f.write(indented_text)
        f.write("\n\n")
