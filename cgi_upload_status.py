#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import ast
import delta_html_creation
import cgi, cgitb
cgitb.enable()
print "Content-Type: text/html\n\n"

def display_data(fileitem, user_name):
    res = []
    obj = delta_html_creation.Index()
    ijson   = {'fileitem':fileitem, 'user_name':user_name}
    res = obj.upload_status(ijson)
    return json.dumps(res)

if __name__=="__main__":
    form = cgi.FieldStorage()
    if (form.has_key("file")):
        tmp = display_data(form['file'], form['user_name'].value)
        print tmp

