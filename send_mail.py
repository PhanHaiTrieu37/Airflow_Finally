import json
import pandas as pd
import numpy as np
import ssl, os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
from datetime import datetime
ssl._create_default_https_context = ssl._create_default_https_context

def email():
    out_csv_file_path = '/opt/airflow/data/crawlData/phanhaitrieu_dataSTVL.csv'
    # out_csv_file_path = '/opt/airflow/data/heart.csv'
    # out_csv_file_path = '/Workspace/study/BigData/OnCK/PhanHaiTrieu20089981/data/heart.csv'

    # Adjusting the recipient's email format
    recipients = ['phamthixuanhien@iuh.edu.vn']
    # recipients = ['phantrieu3701@gmail.com']

    message = Mail(
    # from_email='trieuphan3701@gmail.com',
    from_email='phantrieu3701@gmail.com',
    to_emails=recipients,
    subject='Big Data - 18042024 - Nhóm 01',
    html_content='Chao Co,<br>file crawl data<br>Bai thi hoàn tất!'
    )
    
    with open(out_csv_file_path, 'rb') as f:
        data = f.read()

    encoded_file = base64.b64encode(data).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('data.csv'),
        FileType('text/csv'),
        Disposition('attachment')
    )

    message.attachment = attachedFile
    
    try:
        sg = SendGridAPIClient('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')
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
        print(datetime.now())
        
    except Exception as e:
        print(str(e))

    return True
email()