from requests import get
import smtplib
import sys

# Account from where the email will be sent
username = 'goldenswanip@gmail.com'
password = '321gspros'

# Accounts to send the email to
To = ['guillempros@gmail.com', "rogerprosr@gmail.com"]


# Get the current IP
ip = get('https://api.ipify.org').text

# Get the lattest known IP
ip_path = sys.argv[1]
handle = open(ip_path, 'r')
current_ip = handle.read()

# If the IP changed:
if ip != current_ip:

    # Send an email with the new IP:
    email = "\r\n".join([
        "Subject: New IP for GoldenSwan!",
        "",
        "Hey there!",
        "Here is the new IP:",
        ip
    ])

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(username, password)
    server.sendmail(username, To, email)
    server.quit()

    # Writhe the new IP on the text file
    handle1 = open(ip_path, 'w')
    handle1.write(ip)
    handle1.close()
