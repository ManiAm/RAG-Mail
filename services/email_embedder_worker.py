
import re
import json
import difflib

import services.rag_talk_remote

quoted_reply_patterns = [
    r"On .+?wrote:",                    # Gmail-style replies
    r"From: .+",                        # Outlook
    r"Sent: .+",
    r"To: .+",
    r"Subject: .+",
    r"---+ ?Forwarded message ?---+",   # Forwarded
    r"----+ Original Message ----+",
]

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


def embed_thread_start(emails, thread_id, llm_model, embed_model, collection_name, chunk_size, dump_text_block, max_chunks=3):

    # Remove old embeddings for this thread
    status, output = services.rag_talk_remote.remove_embed_email_thread(collection_name, thread_id)
    if not status:
        return False, f"Error: {output}"

    text_block = get_thread_text(emails)
    if not text_block:
        return True, None

    #######

    chunk_size_original = compute_chunk_size(text_block, embed_model, chunk_size)
    if chunk_size_original is None:
        return False, "Cannot compute chunk size"

    text_block_summarized = ""
    should_summarize = chunk_size_original > max_chunks

    if should_summarize:

        status, output = summarize_thread_text(text_block, llm_model)
        if not status:
            return False, f"Summarization failed: {output}"

        text_block_summarized = output

        if not isinstance(text_block_summarized, str):
            return False, f"Unexpected format type: {type(text_block_summarized)}"

    text_block_final = text_block_summarized or text_block

    #######

    print(f"""[INFO] Embedding thread {thread_id}:
        Subject           : "{emails[0].subject}"
        Email Count       : {len(emails)}
        Attachments Count : {sum(len(e.attachments) for e in emails)}
        Thread Length     : {len(text_block_final)}""")

    metadata = {
        "type"                : "email",
        "thread_id"           : thread_id,
        "subject"             : emails[0].subject,
        "sender"              : emails[0].sender,
        "email_count"         : len(emails),
        "attachments_count"   : sum(len(e.attachments) for e in emails),
        "first_email_date"    : str(min(e.date for e in emails if e.date)),
        "last_email_date"     : str(max(e.date for e in emails if e.date)),
        "text_block_len"      : len(text_block_final),
        "chunk_size_original" : chunk_size_original,
        "is_summarized"       : should_summarize
    }

    status, output = services.rag_talk_remote.embed_email_thread(text_block_final,
        collection_name,
        embed_model,
        metadata,
        separators,
        chunk_size)

    if not status:
        return False, output

    # record only on successful embedding!
    save_thread_to_file(dump_text_block, text_block, text_block_summarized, metadata)

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


def compute_chunk_size(text_block, embed_model, chunk_size):

    if not chunk_size:

        status, output = get_max_characters_embedding(embed_model)
        if not status:
            print(f"Error: get_max_characters_embedding: {output}")
            return None

        if not isinstance(output, int):
            print(f"unexpected format: {type(output)}")
            return None

        chunk_size = output

    #######

    status, output = services.rag_talk_remote.split_document(text_block,
                                                             embed_model,
                                                             chunk_size,
                                                             separators)
    if not status:
        print(f"Error: split_document: {output}")
        return None

    chunks_map = output

    return chunks_map.get("count", None)


def get_max_characters_embedding(embed_model):

    status, output = services.rag_talk_remote.get_max_tokens(embed_model)
    if not status:
        return False, output

    max_tokens = output

    avg_chars_per_token = 3.5  # in English
    approx_max_characters = max_tokens * avg_chars_per_token

    return True, int(approx_max_characters)


def get_max_characters_llm(llm_model):

    status, output = services.rag_talk_remote.get_llm_info(llm_model)
    if not status:
        return False, f"cannot get LLM model info: {output}"

    context_len = output.get("llama.context_length", None)
    if not context_len:
        return False, f"cannot get context length of LLM model {llm_model}"

    avg_chars_per_token = 3.5  # in English
    approx_max_characters = int(context_len) * avg_chars_per_token

    return True, int(approx_max_characters)


def summarize_thread_text(text_block, llm_model):

    status, output = get_max_characters_llm(llm_model)
    if not status:
        return False, f"Error: get_max_characters_llm: {output}"

    context_length_characters = int(output)

    summarization_question = f"""
You are an assistant that summarizes long email threads.

The email thread is structured as follows:
- The entire thread is wrapped with: `===== Begin Email Thread =====` and `===== End Email Thread =====`
- Each email starts with `--- Email N ---` and ends with `--- End Email ---`
- If there are any attachments, they are shown between `--- Begin Attachment: <filename> (<filetype>) ---` and `--- End Attachment ---`

Provide a concise summary of the overall thread, capturing:
- The main topic or subject of the conversation
- Any requests for help, questions being asked, or issues raised by the sender(s)
- Any key decisions or actions mentioned
- Important dates or numbers if mentioned

Only summarize what's relevant. Do not repeat all emails.
Highlight help requests clearly (if any) so a human can follow up if needed.

Here is the email thread:

{text_block}
    """

    if len(summarization_question) > context_length_characters:
        print(
            f"[WARNING] Summarization question length ({len(summarization_question)} chars) exceeds "
            f"the maximum supported context size ({context_length_characters} chars) for LLM '{llm_model}'"
        )

    status, output = services.rag_talk_remote.llm_chat(summarization_question, llm_model, session_id="llm_summarize")
    if not status:
        return False, output

    return True, output


def save_thread_to_file(dump_text_block, text_block, text_block_summarized, metadata):

    header = (
        "============================================\n"
        "=============== Email Thread ===============\n"
        "============================================\n\n"
    )

    with open(dump_text_block, "a", encoding="utf-8") as f:

        f.write(header)
        f.write("METADATA:\n")
        json.dump(metadata, f, indent=4)

        indented = "\n".join(
            (" " * 9 + line if line.strip() else "") for line in text_block.strip().splitlines()
        )

        f.write("\n\n")
        f.write(indented)
        f.write("\n\n")

        if text_block_summarized:

            indented = "\n".join(
                (" " * 9 + line if line.strip() else "") for line in text_block_summarized.strip().splitlines()
            )

            f.write("Text Block Summarized:\n\n")
            f.write(indented)
            f.write("\n\n")
