import sys, os
from convert import convert
import openpyxl
from openpyxl import load_workbook
from datetime import *

class xlsxReader(object):
    def __init__(self):
        self.conObj = convert()

    def readExcel(self, fname, data_flg=False):
        excel_data = {}
        wb = load_workbook(filename=fname, data_only=data_flg)
        sheet_names = wb.get_sheet_names()
        for idx, sheet_name in enumerate(sheet_names):
            sheetObj = wb.get_sheet_by_name(sheet_name)
            excel_data[sheet_name] = []
            for rowid, rowObjs in enumerate(sheetObj.rows):
                row = []
                for colid, cellObj in enumerate(rowObjs):
                    x = cellObj.value
                    if x == None : x = ''
                    try: x = str(x)
                    except: pass
                    x = ' '.join(map(lambda x:x.strip(), x.split()))
                    row.append(x)
                excel_data[sheet_name].append(row[:])
        return  excel_data

