
import sys
import os
import uuid
import pickle
from datetime import datetime
from datetime import timezone
from base64 import urlsafe_b64decode
from bs4 import BeautifulSoup

from email.utils import parsedate_to_datetime
from email.utils import getaddresses

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

from db.session import SessionLocal
from db.models import Email, Attachment
from services.email_loader import Email_loader


class Email_loader_Gmail(Email_loader):

    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    def __init__(self):

        self.service = self.get_gmail_service()


    def get_gmail_service(self):

        creds = None

        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:

            credential_file = "credentials.json"
            if not os.path.exists(credential_file):
                print("Gmail credential JSON file cannot be accessed!")
                sys.exit(1)

            flow = InstalledAppFlow.from_client_secrets_file(credential_file, Email_loader_Gmail.SCOPES)
            creds = flow.run_local_server(port=5008, open_browser=False)

            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        return build('gmail', 'v1', credentials=creds)


    def load_emails(self, since=None, query="", max_results=50, batch_size=5):

        print("Getting a list of emails from Gmail...")

        header_map = self.get_email_list(since, query, max_results)
        if not header_map:
            return

        print(f"Found '{len(header_map)}' new emails.")

        self.save_to_db(header_map, batch_size)


    def get_email_list(self, since, query, max_results):

        if not query:
            query = ""

        if since:
            query += f" after:{since.strftime('%Y/%m/%d')}"

        query = query.strip()

        response = self.service.users().messages().list(
            userId='me',
            q=query,
            labelIds=['INBOX'],
            maxResults=max_results
        ).execute()

        messages = response.get('messages', [])

        header_map = self.get_msg_header(messages)

        # Sort by internalDate (oldest first)
        sorted_map = dict(
            sorted(header_map.items(), key=lambda item: item[1]["internalDate"])
        )

        if not since:
            return sorted_map

        # Convert 'since' datetime to Gmail internalDate (ms since epoch)
        since_ms = int(since.timestamp() * 1000)

        # Filter using internalDate
        filtered = {}
        for _id, message_header in sorted_map.items():
            internal_date = message_header.get("internalDate")
            if internal_date > since_ms:
                filtered[_id] = message_header

        return filtered


    def get_msg_header(self, messages):

        msg_dict = {}

        for msg in messages:

            metadata = self.service.users().messages().get(userId='me', id=msg['id'], format='metadata').execute()

            headers = metadata.get('payload', {}).get('headers', [])

            header_map = {
                h['name'].lower(): h['value'] for h in headers
            }

            header_map["threadId"] = metadata.get("threadId", "")
            header_map["labelIds"] = metadata.get("labelIds", [])
            header_map["internalDate"] = int(metadata.get('internalDate', 0))

            msg_dict[metadata["id"]] = dict(sorted(header_map.items()))

        return msg_dict


    def save_to_db(self, header_map, batch_size):

        new_email_count = len(header_map)
        batch_counter = 0
        session = SessionLocal()

        for idx, (_id, message_header) in enumerate(header_map.items()):

            message_id = message_header.get("message-id")

            if session.query(Email).filter_by(id=message_id).first():
                continue

            print(f"\n({idx+1}/{new_email_count}) Processing new email:")
            print(f"  Date: {message_header.get('date', '')}")
            print(f"  Subject: {message_header.get('subject', '')}")
            print(f"  From: {message_header.get('from', '')}")

            msg_info = self.get_email_detail(_id, message_header)

            thread_id = msg_info["thread_id"]
            message_id = msg_info["message_id"]
            subject = msg_info["subject"]
            sender = msg_info["sender"]
            recipients = msg_info["recipients"]
            body = msg_info["body"]
            date = msg_info["date"]
            attachments = msg_info["attachments"]

            email_obj = Email(
                id=message_id,
                thread_id=thread_id,
                sender=sender,
                recipients=recipients,
                subject=subject,
                body=body,
                date=date or datetime.utcnow()
            )
            session.add(email_obj)

            for filename, meta in attachments.items():

                attachment = Attachment(
                    id=str(uuid.uuid4()),
                    email_id=message_id,
                    filename=filename,
                    mime_type=meta.get("mime_type"),
                    extension=meta.get("extension"),
                    size=meta.get("size"),
                    text_content=meta.get("text")
                )
                session.add(attachment)

            batch_counter += 1

            if batch_counter >= batch_size:
                session.commit()
                batch_counter = 0

        # Final commit for remaining
        if batch_counter > 0:
            session.commit()

        session.close()


    def get_email_detail(self, _id, message_header):

        recipients_raw = message_header.get('to', '')
        recipients = [email for _, email in getaddresses([recipients_raw])]

        # Date in UTC
        date_str = message_header.get('date')
        date_obj = None
        if date_str:
            try:
                date_obj = parsedate_to_datetime(date_str).astimezone(timezone.utc)
            except Exception:
                print(f"Error: parsing email date: {date_str}")

        full = self.service.users().messages().get(userId='me', id=_id, format='full').execute()
        payload = full.get('payload', {})
        body = self.extract_body(payload)
        attachments = self.extract_attachments(full)

        return {
            "thread_id": message_header.get('threadId'),
            "message_id": message_header.get('message-id', str(uuid.uuid4())),
            "subject": message_header.get('subject', ''),
            "sender": message_header.get('from', ''),
            "recipients": recipients,
            "date": date_obj,
            "body": body,
            "attachments": attachments
        }


    def extract_body(self, payload):

        def decode(data):
            if not data:
                return ""
            try:
                data += '=' * (-len(data) % 4)
                return urlsafe_b64decode(data).decode(errors="ignore")
            except Exception:
                return ""

        text_plain_body = ""
        text_html_body = ""

        if 'parts' in payload:
            for part in payload['parts']:
                mime = part.get('mimeType')
                data = part.get('body', {}).get('data')

                if mime == 'text/plain' and data:
                    text_plain_body += decode(data)
                elif mime == 'text/html' and data:
                    text_html_body += decode(data)
                elif 'parts' in part:
                    nested_body = self.extract_body(part)
                    if nested_body:
                        if not text_plain_body:
                            text_plain_body = nested_body

        elif payload.get('body', {}).get('data'):
            mime = payload.get('mimeType')
            data = payload['body']['data']
            if mime == 'text/plain':
                text_plain_body = decode(data)
            elif mime == 'text/html':
                text_html_body = decode(data)

        if text_plain_body:
            return text_plain_body
        elif text_html_body:
            soup = BeautifulSoup(text_html_body, 'html.parser')
            return soup.get_text(separator="\n", strip=True)

        return ""


    def extract_attachments(self, message):

        attachments = {}

        def process_parts(parts, msg_id):

            for part in parts:

                filename = part.get("filename")
                mime_type = part.get("mimeType", "")
                body = part.get("body", {})
                attachment_id = body.get("attachmentId")

                # Recurse into nested parts
                if "parts" in part:
                    process_parts(part["parts"], msg_id)

                if not filename or not attachment_id:
                    continue

                # Download attachment
                attachment = self.service.users().messages().attachments().get(
                    userId='me', messageId=msg_id, id=attachment_id
                ).execute()

                # Decode attachment
                binary_data = urlsafe_b64decode(attachment['data'])

                effective_mime = self.get_mime_type(mime_type, filename, binary_data)

                text_data = self.extract_text(effective_mime, binary_data)

                ext = self.get_file_extension(filename)

                attachments[filename] = {
                    "mime_type": effective_mime,
                    "extension": ext,
                    "size": len(binary_data),
                    "text": text_data
                }

        if "parts" in message["payload"]:
            process_parts(message["payload"]["parts"], message["id"])

        return attachments
