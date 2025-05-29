
import mailbox
import uuid
from db.session import SessionLocal
from db.models import Email
from email.utils import parsedate_to_datetime

from db.session import SessionLocal
from db.models import Email, Attachment
from services.email_loader import Email_loader


class Email_loader_Unix(Email_loader):

    def __init__(self) :

        mbox_path = "/var/mail/youruser"


    def load_emails(self, since=None, query="", max_results=50):

        mbox = mailbox.mbox(mbox_path)
        session = SessionLocal()
        thread_id = "unix-local-thread"

        if not session.query(Thread).filter_by(thread_id=thread_id).first():
            thread = Thread(thread_id=thread_id, subject="Unix Mailbox Import", participants=[])
            session.add(thread)

        for msg in mbox:

            try:
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(errors="ignore")
                            break
                else:
                    body = msg.get_payload(decode=True).decode(errors="ignore")

                email_obj = Email(
                    id=str(uuid.uuid4()),
                    thread_id=thread_id,
                    sender=msg.get("From"),
                    recipients=[msg.get("To")],
                    subject=msg.get("Subject"),
                    body=body,
                    date=parsedate_to_datetime(msg.get("Date"))
                )
                session.add(email_obj)
            except Exception as e:
                print("Skipping email:", e)

        session.commit()
        session.close()
