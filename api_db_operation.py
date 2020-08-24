#!/usr/bin/python
# -*- coding:utf-8 -*-

import MySQLdb
import ConfigParser
import datetime
import base64
import os,json
import shutil
import csv, ast
import string

import pytz,time
from dateutil import parser
from dateutil.relativedelta import relativedelta
import sets,smtplib,base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from os.path import basename
from email.mime.base import MIMEBase
from email import encoders
from random import *

from cryptography.fernet import Fernet


config_file = '/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/login_common/project_config.ini'

class DB_Operation:
    def __init__(self, project_name):
        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file)
        self.dbstr = self.config.get(project_name, 'Database')
        self.secret_code = self.config.get(project_name, 'ciphercode')
        self.conn, self.cur = self.getConn(self.dbstr)

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def getConn(self, dbstr):
        host, user, pwd, db = dbstr.split('#')[:4]
        conn = MySQLdb.connect(host, user, pwd, db)
        cursor = conn.cursor()
        return conn, cursor

    def convert_encrypt_pwd(self, pwd_str):
        key = b'%s'%(self.secret_code)
        cipher_suite = Fernet(key)
        ciphered_text = cipher_suite.encrypt(b"%s"%(pwd_str))
        return ciphered_text 

    def convert_decrypt_pwd(self, pwd_str):
        key = b'%s'%(self.secret_code)
        cipher_suite = Fernet(key)
        ciphered_text = cipher_suite.decrypt(b"%s"%(pwd_str))
        return ciphered_text

    def generate_pwd(self):
        #characters = string.ascii_letters + string.punctuation  + string.digits
	characters = "abcdefghijklmnopqrstuvwxyz01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()?"
        password =  "".join(choice(characters) for x in range(randint(8, 16)))[0:7]
        password = "TAS@"+password
        return password

    def Send_Emails(self,user_name ,subject, table_content,toaddr):
        sender = 'admin.hsbc@tas.in'#change sender email
        receivers = toaddr
        msg = MIMEMultipart()
        msg["From"] = 'admin.hsbc@tas.in'#change from address and password
        msg["To"] = toaddr
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        htmls_str = '<html>'
        htmls_str+= '<head>'
        htmls_str+= '<meta http-equiv="Content-Type" content="text/html; charset=utf-8">'
        htmls_str+= '</head>'
        htmls_str+= '<body>'
        htmls_str+= '<p style="margin-bottom: 7px;">%s %s,</p>'%('Hi ',user_name)
        htmls_str+= '<p>%s</p>'%('We received a request to reset your FactSheet UI Password.')
        htmls_str+= '<p>%s</p>'%('Alternatively you can enter the following password reset code:')
        htmls_str+= '<h3>%s</h3>'%(table_content)
        htmls_str+= '<p>%s</p>'%('Note:if you did not request a new password, let us know immediately')
        htmls_str+= '<p>%s</p>'%('')
        htmls_str+= '<p>%s</p>'%('Regards,')
        htmls_str+= '<p>%s</p>'%('Admin HSBC')
        htmls_str+= '</body>'
        htmls_str+= '</html>'
        html_body = MIMEText(htmls_str.encode('utf-8'),'html','UTF-8')
        msg.attach(html_body)
        res="Done"
        try:
            smtpObj = smtplib.SMTP('smtp.tas.in')
            smtpObj.ehlo()
            smtpObj.starttls()
            smtpObj.ehlo()
            status = smtpObj.login(sender,"fSbGqSE7")
            smtpObj.sendmail(sender, receivers, msg.as_string())
        except Exception:
            res="Error"
        return res

    def user_validation(self,userid,old_pwd):
        qry = 'select user_passwd,user_name,user_role,pwd_flg,old_passwd from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        res = self.cur.fetchone()
        fin_res={'msg':'User ID and Password Mismatch'}
        if not res:return fin_res
        decrp_pwd = self.convert_decrypt_pwd(res[0])        
        decrp_oldpwd="";
        if res[4]:decrp_oldpwd = self.convert_decrypt_pwd(res[4])        
        if decrp_pwd == old_pwd:
            fin_res={'msg':'valid user','user_name':res[1],'user_role':res[2],'user_sts':res[3]}
        elif decrp_oldpwd == old_pwd:
            fin_res={'msg':'Sorry!,You entered an old password'}
        return fin_res 
    
    def all_user_credential(self):
        qry = "select user_id,user_name,user_role from login_dashboard"
        self.cur.execute(qry)
        res = self.cur.fetchall()
        fin_res={'msg':'Done','data':res}
        return fin_res

    def add_user(self,userid,username,userpwd,userrole,useremail):
        fin_res={'msg':'User Id Already Exists'}
        qry = 'select user_name from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        res = self.cur.fetchone()
        if res:return fin_res
        fin_res={'msg':'Successfully Added'}
        encrp_pwd = self.convert_encrypt_pwd(userpwd)
        qry = "insert into login_dashboard (user_id,user_passwd,old_passwd,user_email_id,user_name,user_role,login_status,pwd_flg) values('%s','%s','%s','%s','%s','%s','%s','%s')"
        self.cur.execute(qry%(userid,encrp_pwd,'',useremail,username,userrole,'N',1))
        self.conn.commit()
        return fin_res
        
    def delete_user(self,userid):
        fin_res={'msg':'User Id Not Exists'}
        qry = 'select user_name from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        res = self.cur.fetchone()
        if not res:return fin_res
        fin_res={'msg':'Successfully Deleted'}
        qry = 'delete from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        self.conn.commit()
        return fin_res

    def reset_password(self,userid,old_pwd,new_pwd):
        qry = 'select user_passwd from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        res = self.cur.fetchone()
        fin_res={'msg':'User ID and  Password Mismatch'}
        if not res:return fin_res
        decrp_pwd = self.convert_decrypt_pwd(res[0])        
        if decrp_pwd in old_pwd: 
            old_pwd = self.convert_encrypt_pwd(old_pwd)
            new_pwd = self.convert_encrypt_pwd(new_pwd)
            qry = "update login_dashboard set pwd_flg=%s,user_passwd='%s',old_passwd='%s' where user_id='%s'"
            self.cur.execute(qry%(1,new_pwd,old_pwd,userid))
            self.conn.commit()
            fin_res={'msg':'Successfully Reset'}
        return fin_res

    def forgot_password(self,userid):
        qry = 'select user_email_id,user_id,user_passwd,user_name,pwd_flg,old_passwd from login_dashboard where user_id = "%s"'
        self.cur.execute(qry%(userid))
        res = self.cur.fetchone()
        fin_res={'msg':'UserID Invalid'}
        if not res:return fin_res 
        if not res[0]:fin_res={'msg':'User Email ID is not registered'}
        if res[0] and res[1]:
            generate_new_pwd = self.generate_pwd();
            generate_cryp = self.convert_encrypt_pwd(generate_new_pwd);
            user_email_id,user_passwd = res[0],res[2]
            if res[4] in '2':user_email_id,user_passwd = res[0],res[5]
            qry = "update login_dashboard set pwd_flg='%s',user_passwd='%s',old_passwd='%s' where user_id='%s'"
            self.cur.execute(qry%(2,generate_cryp,user_passwd,userid))
            self.conn.commit()
            mail_res = self.Send_Emails(res[3],'Forgotten Password Reset', generate_new_pwd,user_email_id)
            fin_res={'msg':'reset done'}
            if mail_res in "Error":fin_res={'msg':'Email Service Error Occur'}
        return fin_res



            



if __name__ == '__main__':
    obj = DB_Operation('INC')
    #res = obj.generate_pwd()
    #res = obj.delete_user('madan')
    #res = obj.add_user('sky','Sky','sky@tas_2504','Admin','manikandan.g@tas.in')
    #res = obj.user_validation('sky','mani@tas_2504')
    #res = obj.forgot_password('sky')
    #res = obj.reset_password('sky','TAS@\|4$xRn','mani@tas_2504')
    #res = obj.convert_encrypt_pwd('sky@tas_2504')
    #res = obj.convert_decrypt_pwd('gAAAAABddzPXOFUg4_OlG4Kfm3x7ZTorCps6caiebOqryDhIYBYcs8RHkMTW6GfrYpANkOPWGAPMGDSJ4rJRjdYbWwHJYLm26g==')
    res = obj.convert_decrypt_pwd('gAAAAABdQoq5dlHxlhguVOiq9iJeJ8nPv36CS07DBt3tqbxphze9Bi6hfZ7X4iAeIsTQvM_Vu3Qe3fcOF66TmggjIHBcOe34IQ==')
    print res
    #res = obj.convert_decrypt_pwd('gAAAAABdeJXHlVhdXN4inxepsn7mt3y1ZzYYJOoUR3cpG1onwQyIhJ51OB19ANI97e71j6zOLPEljEIg7mXnTNJ2gin8FPcn6w==')
