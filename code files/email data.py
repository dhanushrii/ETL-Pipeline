import imaplib, email, os
from email.header import decode_header
from dotenv import load_dotenv
from data_process import get_connection, process_file

load_dotenv()

def run_email_ingestion(cursor):
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
    imap.select('"[Gmail]/Sent Mail"')

    _, messages = imap.search(None, '(SUBJECT "Invoice")')

    for mail_id in messages[0].split():
        _, msg_data = imap.fetch(mail_id, "(RFC822)")

        for part in msg_data:
            if isinstance(part, tuple):
                msg = email.message_from_bytes(part[1])

                for p in msg.walk():
                    if p.get_content_disposition() == "attachment":
                        filename = p.get_filename()

                        if filename:
                            decoded, enc = decode_header(filename)[0]
                            if isinstance(decoded, bytes):
                                filename = decoded.decode(enc or "utf-8")

                        process_file(cursor, filename, p.get_payload(decode=True), "EMAIL")

    imap.logout()

if __name__ == "__main__":
    conn = get_connection()
    cursor = conn.cursor()

    run_email_ingestion(cursor)

    conn.commit()
    cursor.close()
    conn.close()