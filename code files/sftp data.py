import paramiko, os
from dotenv import load_dotenv
from data_process import get_connection, process_file

load_dotenv()

def run_sftp_ingestion(cursor):
    transport = paramiko.Transport(
        (os.getenv("SFTP_HOST"), int(os.getenv("SFTP_PORT", 22)))
    )
    transport.connect(
        username=os.getenv("SFTP_USER"),
        password=os.getenv("SFTP_PASS")
    )

    sftp = paramiko.SFTPClient.from_transport(transport)

    for file in sftp.listdir(os.getenv("SFTP_PATH")):
        with sftp.open(f"{os.getenv('SFTP_PATH')}/{file}", 'rb') as f:
            process_file(cursor, file, f.read(), "SFTP")

    sftp.close()
    transport.close()

if __name__ == "__main__":
    conn = get_connection()
    cursor = conn.cursor()

    run_sftp_ingestion(cursor)

    conn.commit()
    cursor.close()
    conn.close()