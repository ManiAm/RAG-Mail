
from sqlalchemy import Column, String, Text, DateTime, Boolean, ForeignKey, Integer, LargeBinary
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()


class Email(Base):

    __tablename__ = "emails"

    id = Column(String, primary_key=True, nullable=False)
    thread_id = Column(String, index=True)
    sender = Column(String, nullable=False)
    recipients = Column(ARRAY(String), nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=False)
    date = Column(DateTime)
    is_embedded = Column(Boolean, default=False)

    attachments = relationship("Attachment", back_populates="email", cascade="all, delete-orphan")


class Attachment(Base):

    __tablename__ = "attachments"

    id = Column(String, primary_key=True, nullable=False)
    email_id = Column(String, ForeignKey("emails.id"), nullable=False)
    filename = Column(String, nullable=False)
    mime_type = Column(String)
    extension = Column(String)
    size = Column(Integer)
    text_content = Column(Text, nullable=True)

    email = relationship("Email", back_populates="attachments")
