import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from datetime import datetime


cWN = datetime.now().isocalendar()[1]
def send_emil(user, pwd, recipient, subject, body):

    gmail_user= user
    gmail_pwd = pwd
    FROM = user
    To = recipient if type(recipient) is list else [recipient]
    SUBJECT = subject
    TEXT = body

    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = ", ".join(To)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = SUBJECT
    msg.attach(MIMEText(TEXT))

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(open("weekly_report_WN" +str(cWN)+".xlsx", "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="weekly_report_WN'+ str(cWN) +'.xlsx"')
    msg.attach(part)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, To, msg.as_string())
        server.close()
        print("Email Sent Successfully")
    except:
        print("Email Sent Failed")


if __name__ == "__main__":

    account = "lixueqian@growing.io"
    pwd = "gogo9966gogo"
    subject = "Week " + str(cWN) + " Product Data "
    recipients = ["product@growing.io", "lixueqian@growing.io", "liulinxi@growing.io"]
    text = "数据在附件"

    send_emil(user=account, pwd=pwd, subject=subject, recipient=recipients, body=text )