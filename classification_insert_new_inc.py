#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime,sqlite3, MySQLdb
#import numpy as np
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
from utils.meta_info import MetaInfo
from collections import defaultdict as DD, OrderedDict as OD
from itertools import combinations
def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__
    
class PYAPI(MetaInfo):
    def __init__(self, pypath="/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc"):
        MetaInfo.__init__(self, pypath)
        self.doc_path          = self.config['doc_path'] 
        self.model_db          = self.config['modeldb'] 
        self.bkup_name       = '' #_user_10_dec'
        self.db_path         =  '/mnt/yMB_db/%s_%s'+ self.bkup_name+'/'
        self.taxo_path       = '/mnt/eMB_db/%s/%s/'
        self.output_path    = self.config['table_path']
        self.bbox_path      = '/var/www/html/fill_table/'

    def update_populate_db(self, company_id, table_ids, company_name):
        project_id, url_id = company_id.split('_')
        table_str = ', '.join(["'"+e+"'" for e in table_ids])
        db_name = 'tfms_urlid_%s'%(company_id)
        m_conn = MySQLdb.connect('172.16.20.229', 'root', 'tas123', db_name)
        m_cur =  m_conn.cursor()
        rd_qry = """ SELECT norm_resid, docid, source_table_info FROM norm_data_mgmt WHERE norm_resid in (%s) """%(table_str)
        m_cur.execute(rd_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_dct = {}
        import MRD.table_normalized_data as table_normalized_data
        Tobj = table_normalized_data.Delta('/root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/dbConfig.ini', '%s_%s'%(project_id, url_id))
        for row in t_data:
            norm_resid, docid, source_table_info = row
            doc, page, grid= eval(source_table_info)
            normalized_data, row_col_d = Tobj.get_triplet_data(project_id, url_id, str(norm_resid), doc, 'Y')
            if normalized_data:continue
            res_dct.setdefault(doc, []).append('-'.join([page, grid]))
        ijson = {"company_name":company_name, 'project_id':project_id, 'popul':res_dct, 'deal_id':url_id, 'model_number':project_id, 'from_auto_inc':'Y', 'user':'system'}
        import populate_wrapper as py
        obj = py.Wrapper()
        obj.insert_populate_info_cgi_lang_table_doc(ijson)
        print res_dct

    
    def populate_classified_table(self, company_id, company_name):
        import model_view.pr_company_docTablePh_details_tableType as py
        pObj = py.Company_docTablePh_details()
        classified_tables = pObj.get_all_classified_tables(company_id)
        print '#'.join(classified_tables)
        #self.update_populate_db(company_id, classified_tables, company_name) 
    
    def max_node_mapping(self):
        db_path = '/mnt/eMB_db/node_mapping.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT sheet_id, node_name, description FROM node_mapping """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        node_chk_dct = {}
        mx_sheet_id = 0
        for row in t_data:
            sheet_id, node_name, description = int(row[0]), str(row[1]), str(row[2])
            ds = ' '.join(description.split()).lower()
            node_chk_dct[ds] = (sheet_id, node_name)
            node_chk_dct[node_name.lower()] = (sheet_id, node_name)
            get_mx = max(sheet_id, mx_sheet_id)
            mx_sheet_id = copy.deepcopy(get_mx)
        mx_sheet_id += 1
        return  mx_sheet_id, node_chk_dct

    def new_old_table_map(self, company_id, new_inc_doc_dct):
        doc_str = ', '.join({'"'+e+'"' for e in new_inc_doc_dct})
        #print doc_str;sys.exit()
        
        db_name = 'tfms_urlid_%s'%(company_id)
        m_conn = MySQLdb.connect('172.16.20.229', 'root', 'tas123', db_name)
        m_cur =  m_conn.cursor()
        read_qry = """ SELECT document_id, document_name FROM ir_document_master;  """
        m_cur.execute(read_qry)
        dc_name_map  = {str(row[0]):str(row[1]) for row in m_cur.fetchall()}
        rd_qry = """ SELECT norm_resid, docid, source_table_info FROM norm_data_mgmt WHERE source_table_info in (%s) """%(doc_str)
        m_cur.execute(rd_qry)
        res_dct = {}
        for row in m_cur.fetchall():
            tab = str(row[0])
            doc = str(row[1])
            source_table_info = eval(row[2])
            res_dct[source_table_info] = (tab, doc, dc_name_map[doc])
        m_conn.close()
        #print res_dct;sys.exit()  
        return res_dct

    def get_company_id_pass_company_name(self, project_id):
        db_path  = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT company_name, toc_company_id FROM company_info WHERE project_id="%s";'%(project_id)
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        c_details = {}  
        for row in table_data:
            company_name, toc_company_id = map(str, row)
            c_details[project_id +'_'+toc_company_id] = company_name
        return c_details
    
    def insert_auto_inc_classification(self, company_id, doc_ids, table_source_map):
        project_id, url_id  = company_id.split('_')    

        company_name = self.get_company_id_pass_company_name(project_id)[company_id]
        model_number = project_id

        import model_view.pr_company_docTablePh_details_tableType as py
        pObj = py.Company_docTablePh_details()
        classified_tables = pObj.auto_inc_get_all_classified_tables(company_id)

        doc_str = ', '.join(["'"+e+"'" for e in doc_ids.split('#')])
        
        m_conn = MySQLdb.connect('172.16.20.52', 'root', 'tas123', 'AECN_INC')
        m_cur =  m_conn.cursor()
        read_qry = """ SELECT doc_id, table_name, page_no, grid_id FROM Focus_Data_mgmt WHERE doc_id in (%s) """%(doc_str)  
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()

        all_tabs_new_inc = {}
        for row in t_data:
            doc_id, table_name, page_no, grid_id = map(str, row)
            all_tabs_new_inc[str((doc_id, page_no, grid_id))] = table_name
        
        
        max_node_map, node_chk_dct = self.max_node_mapping()
        mapping_class = {'Income Statement':('1', 'IS'), "Balance Statement":('2', 'BS'), 'Cash Flows':('3', 'CF'), 'Revenue by segment':('4', 'RBS'), 'Revenue by geography':('5', 'RBG')}
       
        for k, v in mapping_class.iteritems():
            tss = ' '.join(k.split()).lower()
            if tss not in node_chk_dct:
                node_chk_dct[tss] = v
 
        node_mapping_lst = []
        classification_lst = []
        update_rows = []
        for tab, source_info in table_source_map.iteritems():
            dc, pg, grid = eval(source_info)
            class_get = all_tabs_new_inc.get(source_info, '')

            if (not class_get): continue# or (tab in classified_tables):continue  #or tab in classified_tables:continue 
                

            main_header = 'Main Table'
            sub_header  = 'Group'
            review_flg  = 0
            c_clas = ''.join([e.capitalize() for e in class_get.split()])
            node_name =  copy.deepcopy(c_clas)
            description = copy.deepcopy(class_get)
            ds = ' '.join(description.split()).lower()
            if (ds not in node_chk_dct):
                sheet_id = str(copy.deepcopy(max_node_map))
                node_tup = (main_header, sub_header, sheet_id, node_name, description, review_flg)
                node_mapping_lst.append(node_tup)
                node_chk_dct[ds] = (sheet_id, node_name)
                max_node_map += 1
            elif ds in node_chk_dct:
                sheet_id, node_name = map(str, node_chk_dct[ds])
            if tab in classified_tables: 
                get_tt_old = classified_tables[tab]
                if get_tt_old != node_name:
                    up_tup = (sheet_id, node_name, tab, dc)
                    update_rows.append(up_tup)
            else:
                tab_tup = (sheet_id, dc, '', tab, node_name)
                classification_lst.append(tab_tup)
        print update_rows

        db_path = '/mnt/eMB_db/node_mapping.db' 
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        cur.executemany(""" INSERT INTO node_mapping(main_header, sub_header, sheet_id, node_name, description, review_flg) VALUES(?, ?, ?, ?, ?, ?) """, node_mapping_lst)
        conn.commit()
        conn.close()
        
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        if classification_lst:
            cur.executemany(""" INSERT INTO table_group_mapping(sheet_id, doc_id, doc_name, table_id, parent_table_type) VALUES(?, ?, ?, ?, ?) """, classification_lst)
        if update_rows:
            cur.executemany(""" UPDATE table_group_mapping SET sheet_id=?, parent_table_type=? WHERE table_id=? AND doc_id=?   """, update_rows)
        conn.commit()
        conn.close()
        return
       
    def all_table_types_sheet_doc(self):
        pass 
    



if __name__ == '__main__':
    obj  = PYAPI()
    #print obj.insert_classification_info('20_8', '1982#1983#1984#1985#1986#1987#1988#1989#1990#1991#1992#1993#1994#1995#1996#1997#1998#1999#2000#2001#2002#2003#2004#2005#2006#2007#2008#2009#2010#2011#2012#2013#2014#2015#2016#2017#2020#2069')
    #print obj.insert_classification_info('20_8', '1982#1983')
