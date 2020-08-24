'''
import sqlite3
from xlsxwriter.workbook import Workbook
workbook = Workbook('/var/www/html/output2.xlsx')
worksheet = workbook.add_worksheet()

conn=sqlite3.connect('test.sqlite')
c=conn.cursor()
c.execute("select * from abc")
mysel=c.execute("select * from abc ")
for i, row in enumerate(mysel):
    print row
    worksheet.write(i, 0, row[0])
    worksheet.write(i, 1, row[1])
workbook.close()
To handle multiple columns you could do this
'''
import sqlite3
from xlsxwriter.workbook import Workbook
workbook = Workbook('/var/www/html/output2.xlsx')
worksheet = workbook.add_worksheet()

conn=sqlite3.connect('/tmp/linkbase_data1.db')
c=conn.cursor()
c.execute("select * from LogicalMeta")
mysel=c.execute("select * from LogicalMeta")
for i, row in enumerate(mysel):
    for j, value in enumerate(row):
        worksheet.write(i, j, value)
workbook.close()
