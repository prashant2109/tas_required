import smtplib

sender = 'prashant.k@tas.in'
receivers = ['prashant.k@tas.in']

message = """From: From Person <from@fromdomain.com>
To: To Person <to@todomain.com>
Subject: SMTP e-mail test

This is a test e-mail message.
"""

smtpObj = smtplib.SMTP('webmail.tas.in', 80)
smtpObj.sendmail(sender, receivers, message)         
print "Successfully sent email"
