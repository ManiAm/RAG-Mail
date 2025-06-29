
import os
import sys
import uuid
import io
from datetime import datetime

from email import message_from_binary_file
from email.utils import parsedate_to_datetime
from email.utils import parseaddr
from email.header import decode_header, make_header

from db.session import SessionLocal
from db.models import Email, Attachment
from services.email_loader import Email_loader


class Email_loader_mbox(Email_loader):

    def __init__(self, mbox_path):

        self.mbox_path = mbox_path

        if not os.path.exists(mbox_path):
            print(f"Error: mbox_path is not accessible: {mbox_path}")
            sys.exit(2)


    def load_emails(self, max_results=-1, batch_size=5):

        session = SessionLocal()
        batch_counter = 0

        for idx, message in enumerate(self._iter_mbox_stream()): # 78970

            if max_results != -1 and idx >= max_results:
                break

            message_id = message.get("Message-ID", None)
            if not message_id:
                continue

            if session.query(Email).filter_by(id=message_id).first():
                continue

            subject = self._decode_header_value(message.get("Subject", ""))
            sender = self._decode_header_value(message.get("From", ""))

            recipients_raw = message.get_all("To", [])

            recipients = []
            for r in recipients_raw:
                decoded = self._decode_header_value(r)
                _, email = parseaddr(decoded)
                if email:
                    recipients.append(email)

            # Parse date
            date = message.get("Date")
            parsed_date = parsedate_to_datetime(date) if date else None

            print(f"\n({idx+1}) Processing new mbox email:")
            print(f"  Date: {date}")
            print(f"  Subject: {subject}")
            print(f"  From: {sender}")

            references_raw = message.get('References', '')
            references_list = references_raw.split() if references_raw else []

            in_reply_to = self._decode_header_value(message.get("In-Reply-To", ""))

            thread_id = self._build_thread_id(message)
            body = self._get_body(message)
            attachments = self._get_attachments(message)

            email_obj = Email(
                id=message_id,
                thread_id=thread_id,
                references=references_list,
                in_reply_to=in_reply_to,
                sender=sender,
                recipients=recipients,
                date=parsed_date or datetime.utcnow(),
                subject=subject,
                body=body
            )

            session.add(email_obj)

            for filename, meta in attachments.items():

                attachment = Attachment(
                    id=str(uuid.uuid4()),
                    email_id=message_id,
                    filename=filename,
                    mime_type=meta["mime_type"],
                    extension=meta["extension"],
                    size=meta["size"],
                    text_content=meta["text"]
                )
                session.add(attachment)

            batch_counter += 1

            if batch_counter >= batch_size:
                session.commit()
                batch_counter = 0

        if batch_counter > 0:
            session.commit()

        session.close()
        print("\nAll emails are processed!")


    def _iter_mbox_stream(self):

        separator = b"From "

        with open(self.mbox_path, "rb") as f:

            buffer = bytearray()
            first = True

            for line in f:
                if line.startswith(separator):
                    if not first:
                        yield message_from_binary_file(io.BytesIO(buffer))
                    else:
                        first = False
                    buffer = bytearray()
                buffer.extend(line)

            if buffer:
                yield message_from_binary_file(io.BytesIO(buffer))


    def _decode_header_value(self, value: str) -> str:

        try:
            decoded_parts = decode_header(value)
            return str(make_header(decoded_parts))
        except Exception as e:
            print(f"[WARN] Failed to decode header: {value}\nReason: {e}")
            return value


    def _build_thread_id(self, message):

        references = message.get("References", None)
        in_reply_to = message.get("In-Reply-To", None)
        message_id = message.get("Message-ID", None)

        if references:
            ref_ids = references.split()
            thread_id = ref_ids[0]
        elif in_reply_to:
            thread_id = in_reply_to
        else:
            thread_id = message_id

        return thread_id


    def _get_body(self, message) -> str:
        """ Extract plain text body from the email. """

        plain_text = ""
        html_text = ""

        if message.is_multipart():

            for part in message.walk():

                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", "")).lower()

                if "attachment" in content_disposition:
                    continue  # Skip attachments

                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="ignore")

                    if content_type == "text/plain":
                        plain_text += text
                    elif content_type == "text/html":
                        html_text += text

                except Exception:
                    continue
        else:

            try:
                payload = message.get_payload(decode=True)
                if payload:
                    charset = message.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="ignore")
                    content_type = message.get_content_type()
                    if content_type == "text/plain":
                        plain_text = text
                    elif content_type == "text/html":
                        html_text = text
            except Exception:
                pass

        if html_text:
            status, output = self.html_to_text(html_text)
            if status and output:
                plain_text = output

        return plain_text.replace('\x00', '').strip()


    def _get_attachments(self, message) -> dict:

        attachments = {}

        if not message.is_multipart():
            return attachments

        for part in message.walk():

            content_disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in content_disposition:

                filename = part.get_filename()
                if not filename:
                    continue

                try:
                    binary_data = part.get_payload(decode=True) or b""
                    mime_type = part.get_content_type()
                    effective_mime = self.get_mime_type(mime_type, filename, binary_data)
                    text_data = self.extract_text(effective_mime, binary_data)
                    attachments[filename] = {
                        "mime_type": effective_mime,
                        "extension": self.get_file_extension(filename),
                        "size": len(binary_data),
                        "text": text_data,
                    }
                except Exception:
                    continue

        return attachments
