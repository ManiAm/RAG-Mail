
import os
import mimetypes
import magic
import json
import pdfplumber
import zipfile
import re
import pytesseract
import html2text

from email import message_from_bytes
from email.policy import default
from docx import Document
from io import BytesIO
from PIL import Image


class Email_loader():

    def get_mime_type(self, mime_type, filename, binary_data):

        try:
            detected_type = magic.from_buffer(binary_data, mime=True)
            if detected_type:
                return detected_type
        except Exception:
            pass  # Ignore magic failures silently

        if mime_type:
            return mime_type

        # Guess file extension and type
        guessed_type, _ = mimetypes.guess_type(filename)
        if guessed_type:
            return guessed_type

        return "application/octet-stream"


    def get_file_extension(self, filename):

        return os.path.splitext(filename)[-1].lower()


    def extract_text(self, effective_mime, binary_data):

        text_data = ""

        if effective_mime in ["text/csv", "text/plain", "application/x-wine-extension-ini"]:

            try:
                text_data = binary_data.decode("utf-8", errors="ignore")
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "application/json":

            try:
                json_str = binary_data.decode("utf-8")
                parsed = json.loads(json_str)
                text_data = json.dumps(parsed, indent=2)
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "text/html":

            try:
                html_txt = binary_data.decode('utf-8', errors='ignore')
                status, output = self.html_to_text(html_txt)
                if status and output:
                    text_data = output
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":

            try:
                doc = Document(BytesIO(binary_data))
                text_data = "\n".join([para.text for para in doc.paragraphs])
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "message/rfc822":

            try:
                parsed = self.parse_rfc822_email(binary_data)

                text_data = ""
                text_data += f"From: {parsed['sender']}\n"
                text_data += f"To: {parsed['receiver']}\n"
                text_data += f"Subject: {parsed['subject']}\n"
                text_data += f"\n"
                text_data += f"{parsed['body']}"
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "application/pdf":

            try:
                with pdfplumber.open(BytesIO(binary_data)) as pdf:
                    all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                text_data = all_text.strip()
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime in ["image/jpeg", "image/png"]:

            try:
                image = Image.open(BytesIO(binary_data))
                text = pytesseract.image_to_string(image)
                text_data = text.strip()
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        elif effective_mime == "application/zip":

            try:
                text_data = self.extract_text_from_zip_binary(binary_data)
            except Exception as e:
                print(f"Error: extract_text: {e}")
                pass

        else:

            print(f"Warning: unsupported MIME type: {effective_mime}")

        return text_data.replace('\x00', '')


    def parse_rfc822_email(self, content):

        msg = message_from_bytes(content, policy=default)

        sender = msg.get("From")
        receiver = msg.get("To")
        subject = msg.get("Subject")

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain" and not part.get_content_disposition():
                    body = part.get_content()
                    break
        else:
            body = msg.get_content()

        return {
            "sender": sender,
            "receiver": receiver,
            "subject": subject,
            "body": body.strip()
        }


    def extract_text_from_zip_binary(self, binary_data):

        result = []

        with zipfile.ZipFile(BytesIO(binary_data)) as zip_file:

            for file_name in zip_file.namelist():

                if file_name.lower().endswith((".txt", ".log", ".md", ".csv", ".json", ".yaml", ".yml")):

                    with zip_file.open(file_name) as f:

                        try:
                            text = f.read().decode("utf-8", errors="ignore")
                            result.append(f"===== {file_name} =====\n{text}")
                        except Exception as e:
                            result.append(f"===== {file_name} =====\n[Could not read file: {e}]")

        return "\n\n".join(result).strip()


    def html_to_text(self, html_text):

        try:

            h = html2text.HTML2Text()

            h.ignore_links = True           # Do not include hyperlinks
            h.ignore_images = True          # Skip image tags
            h.body_width = 0                # Do not wrap text
            h.ignore_emphasis = True        # Remove **bold** and _italics_
            h.skip_internal_links = True    # Avoid internal anchors
            h.ignore_tables = True          # Do not include tables
            h.protect_links = True

            text = h.handle(html_text)

            # Remove invisible or formatting Unicode characters
            text = re.sub(r"[\u200c\u200d\u200e\u200f\u202a-\u202e\u2060-\u206f\u00ad\xa0]", " ", text)

            # Replace multiple consecutive spaces or tabs with a single space
            text = re.sub(r"[ \t]+", " ", text)

            # Normalize newlines (remove lines that are empty or have only whitespace)
            text = re.sub(r"\n\s*\n+", "\n\n", text)

            return True, text.strip()

        except Exception as E:

            return False, f"Error in parsing html: {str(E)}"
