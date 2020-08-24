#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, MySQLdb 
import copy, json, ast, datetime,sqlite3
import shelve
import db.webdatastore as wb
lmdb_obj = wb.webdatastore()
import mysql.connector
from multiprocessing import Pool

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass
def enableprint():
    sys.stdout = sys.__stdout__
    pass

def run_html_process(process):
    path, html_str = process
    print path
    f = open(path)
    f.write(html_str)
    f.close()
        
def run_lmdb_process(process):
    celldata_db_path, full_table_cell_dict, full_table_cell_dict_keys, data_rows, company_name, model_number, table_id = process
    lmdb_obj.write_to_lmdb(celldata_db_path, full_table_cell_dict, full_table_cell_dict_keys) 

    db_path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/%s.db'%(company_name, model_number, table_id)   
    os.system('rm -rf %s'%(db_path))
    print db_path
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        del_stmt = "drop table TableCsvPhInfo"
        try:
            cur.execute(del_stmt)
        except:pass
        crt_stmt = """CREATE TABLE IF NOT EXISTS TableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, org_row VARCHAR(20), org_col VARCHAR(20), nrow VARCHAR(20), ncol VARCHAR(20), cell_type VARCHAR(20), txt  TEXT, xml_id TEXT, period_type TEXT, usr_period_type TEXT, period TEXT, usr_period TEXT, currency TEXT, usr_currency TEXT, value_type TEXT, usr_value_type TEXT, scale TEXT, usr_scale VARCHAR(20))"""
        cur.execute(crt_stmt)
        insert_stmt = """INSERT INTO TableCsvPhInfo(org_row, org_col, nrow, ncol, cell_type, txt, xml_id, period_type, usr_period_type, period, usr_period, currency, usr_currency, value_type, usr_value_type, scale, usr_scale) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cur.executemany(insert_stmt, data_rows)
        conn.commit()
        conn.close()       
    except:
        print 'ERROR ',(db_path)



class Company_populate_new(object):
    def __init__(self):
        pass
    
    def alter_mysql_table(self, m_cur, m_conn, table_name, lst_columns):
        pass
        

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def create_databases_mysql(self, database_name):
        m_conn = mysql.connector.connect(
        host="172.16.20.229",
        user="root",
        passwd="tas123"
        )
        mcur = m_conn.cursor()
        mcur.execute("CREATE DATABASE %s"%(database_name))
        return 'done'
    def get_new_document_info(self, to_company_id):
        to_db_name = 'tfms_urlid_%s'%(to_company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', to_db_name]
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT old_doc, new_doc, old_table, new_table FROM old_new_doc_table_page_map;"""  
        m_cur.execute(read_qry)
        table_data = m_cur.fetchall()        
        m_conn.close()
        ft_2_td_map = {}
        od_2_nd_map = {}
        ot_2_nt_map = {}
        doc_map = {}
        for row in table_data:
            old_doc, new_doc, old_table, new_table = map(str, row)
            od_2_nd_map[old_doc] = new_doc
            od_2_nd_map[old_table] = new_table
            ft_2_td_map[old_doc] = old_table
            doc_map[new_doc] = 1
        return doc_map

    def doc_table_data(self, company_id, data_rows, data_rows_2, data_rows_3):

        #dc_dct = self.get_new_document_info(company_id)
        #dc_str = ', '.join(["'"+e+"'" for e in dc_dct])
        project_id, deal_id = company_id.split('_')
        crt_db_name = 'tfms_urlid_%s'%(company_id)
        try:
            self.create_databases_mysql(crt_db_name)
            print ['>>>>', crt_db_name]
        except:pass
        #print crt_db_name
        #sys.exit()
        #sys.exit() 
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        conn, cur = self.mysql_connection(db_data_lst)
        #print conn, cur
        if 0:
            dr = 'delete from norm_data_mgmt where docid not in (%s);'%(dc_str)
            cur.execute(dr)
            dr2 = 'delete from data_mgmt where docid not in (%s);'%(dc_str)
            cur.execute(dr2)
            dr3 = 'delete from ir_document_master where document_id not in (%s)'%(dc_str)
            cur.execute(dr3)
        #except:pass
        #sys.exit()
        crt_stmt = """CREATE TABLE IF NOT EXISTS norm_data_mgmt(norm_resid BIGINT(20), norm_training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), ref_resid BIGINT(20), ref_training_id varchar(600), pageno INT(11), doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), review_flag VARCHAR(1), process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, table_type TEXT, user_info TEXT, mflag INT(2), system_flag INT(2))""" 
        try:
            cur.execute(crt_stmt) 
            conn.commit()
        except:pass
        #dr = ((norm_resid, table_id), 1, project_id, deal_id, 1, 1, 1, doc_id, doc_id, doc_id, table_id, 'None', page_no, '', 'Y', 'Y', '', 'Y', '', '', 1, 1)
        
        crt2_stmt = """CREATE TABLE IF NOT EXISTS data_mgmt(resid BIGINT(20), training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), pageno INT(11), taxoid INT(11), taxoname TEXT, doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), data0 TEXT, data1 MEDIUMTEXT, process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, training_tableid INT(11), training_group_id INT(11), applicator_key TEXT, reviewed_user TEXT)"""
        try:
            cur.execute(crt2_stmt)
            conn.commit()
        except:pass
        # dr2 = (table_id, 1, project_id, deal_id, 1, 1, 1, doc_id, doc_id, doc_id, page_no, 1, '', '', 'Y', 'Y', '', '', '', 1, 1, 1, 1)

        crt3_stmt = """CREATE TABLE IF NOT EXISTS ir_document_master(document_id BIGINT(20), project_id INT(11), url_id INT(11), agent_id INT(11), mgmt_id INT(11), user_id INT(11), upload_id int(11), batch_id VARCHAR(64), document_name TEXT, src_type VARCHAR(16), doc_type VARCHAR(16), format_type VARCHAR(64), doc_status VARCHAR(1), total_pages INT(11), page_width INT(11), page_height INT(11), process_date_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, remote_ip VARCHAR(50), stage_id TEXT, reviewed_status VARCHAR(1), reviwed_by VARCHAR(100), active_status VARCHAR(1), display_status_internal VARCHAR(1), display_status_external VARCHAR(1))"""
        try:
            cur.execute(crt3_stmt)
            conn.commit()
        except:pass
        # dr3 = (doc_id, project_id, deal_id, str(doc_id)+'.pdf', 'Y')
        #dr = ((norm_resid, table_id), 1, project_id, deal_id, doc_id, table_id, page_no, 'Y', 'Y', 'Y')
        # dr2 = (table_id, project_id, deal_id, doc_id, page_no, 'Y', 'Y')
        #sys.exit()
        if data_rows:
            cur.executemany("insert into norm_data_mgmt(norm_resid, project_id, url_id, docid, ref_resid,  pageno,  active_status , process_status, review_flag) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)", data_rows)
            conn.commit()
        if data_rows_2:
            cur.executemany("""insert into data_mgmt(resid, project_id, url_id, docid, pageno, active_status , process_status, taxoname) values(%s, %s, %s, %s, %s, %s, %s, %s)""", data_rows_2)
            conn.commit()
        if data_rows_3:
            cur.executemany("""insert into ir_document_master(document_id, project_id, url_id, document_name, active_status) values(%s, %s, %s, %s, %s)""", data_rows_3)
            crt_qry = """CREATE TABLE IF NOT EXISTS old_new_doc_table_page_map(old_doc TEXT, new_doc TEXT, old_table TEXT, new_table TEXT, pageno TEXT)"""
            cur.execute(crt_qry)
            conn.commit()
        conn.close()
        return 'done'

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def get_update_lmdb(self, lmdbPath, lmdbdict):
        env = lmdb.open(lmdbPath, map_size=2**39)
        with env.begin(write=True) as txn:
            for key, val in lmdbdict.iteritems():
                txn.put(str(key), str(val))
        return 'done'

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
            c_details[company_name] = project_id +'_'+toc_company_id
        return c_details


    def table_dict_insert_func(self, company_name, model_number, company_id, data_rows):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
            
        if 1:
            del_stmt = 'drop table table_dict_phcsv_info;'
            try:
                cur.execute(del_stmt)
                conn.commit()
            except:pass
        
        crt_stmt = 'CREATE TABLE IF NOT EXISTS table_dict_phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id TEXT, page_no TEXT, table_id TEXT, table_dict BLOB, phcsv_dict BLOB)'
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'table_dict_phcsv_info', ['status_flg', 'table_type'])
        cur.executemany("""INSERT INTO table_dict_phcsv_info(doc_id, page_no, table_id, table_dict, phcsv_dict, status_flg, table_type) VALUES(?, ?, ?, ?, ?, ?, ?)""", data_rows)
        conn.commit() 
        conn.close()
        return        

    def write_fye_txt(self, company_name_dct, proj_id):
        fiscal_years_data_dict = {'Jan':'1', 'Feb':'2', 'Mar':'3', 'Apr':'4', 'May':'5', 'Jun':'6', 'Jul':'7', 'Aug':'8', 'Sep':'9', 'Oct':'10', 'Nov':'11', 'Dec':'12'} 
        get_db = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(get_db)
        cur  = conn.cursor()

        f_path = "/root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/data/company_info.txt"
        f1 =  open(f_path)
        all_d = f1.readlines()

        resd1 = {}
        for rw in all_d:
            comp_name, display_name, p_id, dl_id, template_name, model_no = rw.split('\t')
            cid_s = '_'.join([p_id, dl_id])
            resd1[(comp_name, cid_s)] = 1
    
        resd2 = {}
        file_path = '/root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/fiscal_ending_year.txt' 
        f2 = open(file_path)
        tx_dt = f2.readlines()
        for ln in tx_dt:
            cd, fy = ln.split('#')
            resd2[cd] = fy
        
        resd3 = {}
        fp_path = '/mnt/eMB_db/dealid_company_info.txt'
        f3 = open(fp_path)
        t_dt = f3.readlines()
        for line in t_dt:
            cdi, comp, ip_addr = line.split(':$$:')
            resd3[cdi] = (comp, ip_addr) 
        f1.close()   
        f2.close() 
        f3.close()
        f1 =  open(f_path, 'a')
        f2 = open(file_path, 'a')
        f3 = open(fp_path, 'a')
        
        read_qry = """SELECT company_name, company_display_name , project_id, toc_company_id, reporting_year, model_number FROM company_info WHERE project_id='%s';"""%(proj_id)
        cur.execute(read_qry)
        for row in cur.fetchall():
            company_name, company_display_name , project_id, toc_company_id, reporting_year, model_number = row[:]
            if company_name not in company_name_dct:continue
            cd_st = '_'.join([project_id, toc_company_id])
            ############################################################# 
            if (company_name, cd_st) not in resd1:
                dt_str = '\t'.join([company_name, company_display_name, project_id, toc_company_id, 'TAS-MRD', model_number]) 
                f1.write(dt_str + '\n') 
            #############################################################
            if cd_st not in resd2:
                fy_month = reporting_year.split(' - ')[1]
                fye = fiscal_years_data_dict[fy_month]
                dt_str = '#'.join([cd_st, fye])
                f2.write(dt_str+'\n')
            #############################################################
            #1_24:$$:CopaHoldingsSA:$$:172.16.20.229
            if cd_st not in resd3:
                dt_str = ':$$:'.join([cd_st, company_name, '172.16.20.229'])
                f3.write(dt_str + '\n')
            #############################################################
        f1.close()   
        f2.close() 
        f3.close()
        conn.close()
        return 'done'
    
    def crt_doc_map(self, company_name, model_number, company_id):
        # /var/www/html/TASFundamentalsV2/tasfms/data/output/1/79/1_1/21/sdata/doc_map.txt
        project_id, deal_id = company_id.split('_')
                 
        db_path_dir = '/mnt/eMB_db/%s/%s/'%(company_name, model_number)
        db_path = os.path.join(db_path_dir, 'tas_company.db')
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT doc_id, old_doc_name, document_type, period, filing_type, doc_release_date, doc_name, reporting_year, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date FROM company_meta_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        #print table_data;sys.exit()
        conn.close()
        all_headers = ['Doc Id', 'Old Name', 'Document Type', 'Period', 'Filing Type', 'Document Release Date', 'Document Name', 'Financial Year', 'Doc From', 'Doc To', 'Download Date', 'Prev Release Date', 'Next Release Date']
        hdr_str = '\t'.join(all_headers)
        fl_txt_dir =  '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/'%(project_id, deal_id)
         
        if not os.path.exists(fl_txt_dir):
            os.system('mkdir -p %s'%(fl_txt_dir))
        fl_path  = os.path.join(fl_txt_dir, 'doc_map.txt')  
        #print fl_path
        f = open(fl_path, 'w')
        f.write(hdr_str + '\n')
        for row in table_data:
            rw = map(str, row)
            rw_str = '\t'.join(rw)
            f.write(rw_str+'\n')
        f.close()
    
    def insert_company_info(self):
        db_path = '/mnt/eMB_db/company_info/4433_compnay_info.db'
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
   
        read_qry = 'SELECT *  FROM company_info where project_id="50"'
        cur.execute(read_qry)
        tb_data = cur.fetchall()
        conn.close()
        
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
        self.alter_table_coldef(conn, cur, 'company_info', ['d_studio_map', 'd_studio_path'])    
       
        for row in tb_data:
            row = map(str, row)
            company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path = row[1:] 
            insert_stmt = """INSERT INTO company_info('company_name', 'company_display_name', 'project_id', 'toc_company_id', 'type_of_company', 'model_number', 'industry_type', 'internal_status', 'external_status', 'reporting_year', 'filing_frequency', 'd_studio_map', 'd_studio_path') VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path) 
            print insert_stmt
            print 
            cur.execute(insert_stmt)
            conn.commit() 
        conn.close

    def company_meta_info_txt_data(self, txt_data):
        #f = open('bds_company_details_229_2.txt') 
        #txt_data = f.readlines()
        #file_path = '/var/www/html/avinash/bds_company_details_229_2.txt'
        #txt_data  = self.read_txt_from_server(file_path)
        comp_wise_data = {}
        for row in txt_data:
            row = eval(row)
            if row == None:continue
            cmp_name = ''.join(row['company_name'].split())
            dc_name = ''
            for s in cmp_name:
                if s.isalpha():
                    dc_name += s
            cmp_name = dc_name[:]
            comp_wise_data[cmp_name] = [row]
        return comp_wise_data
        
        
    def write_comp_md_txt(self, company_name, model_number, txt_data):
        ff_l = ['Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2', 'FY', 'M9']
        sh = ['ER DATE', 'Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2', 'FY', 'M9', 'Filing Frequency', 'Ticker', 'Accounting Standards','Industry','From Year','To Year']
        hdr_str = '\t'.join(sh)
        txt_path = '/mnt/eMB_db/%s/%s/company_meta_info.txt'%(company_name, model_number) 
        f1 = open(txt_path, 'w')
        f1.write(hdr_str + '\n')
        for row in txt_data:
            if row == None:continue
            get_ff_lst = []
            ff_pres = {}
            for fr in ff_l:
                dt = row.get(fr, '')
                if dt:
                    m_str = '#'.join(['true', fr])
                    get_ff_lst.append(m_str)
                    splt_dt = dt.split(' - ')
                    e1 = splt_dt[0][:3]
                    e2 = splt_dt[1][:3]
                    jn_splt = e1 + ' - ' + e2
                    ff_pres[fr] = jn_splt
            ff = '_'.join(get_ff_lst)
            dt_pr = ['', ff_pres.get('Q1', ''), ff_pres.get('Q2', ''), ff_pres.get('Q3', ''), ff_pres.get('Q4', ''), ff_pres.get('H1', ''), ff_pres.get('H2', ''), ff_pres.get('FY', ''), ff_pres.get('M9', ''), ff, row.get('Ticker', ''), row.get('Accounting Standards', ''), row.get('Industry', ''), '', '']
            val_str = '\t'.join(dt_pr)
            f1.write(val_str)  
        f1.close() 
        return 'done'
 
    def get_data_from_txt(self, txt_data, txt_data2):
        from_project_id = '21'
        comp_meta_txt_data = self.company_meta_info_txt_data(txt_data2)
        #f = open('bds_company_details_229_1.txt') 
        #txt_data = f.readlines()
        #file_path = '/var/www/html/avinash/bds_company_details_229_1.txt'
        #txt_data  = self.read_txt_from_server(file_path)
        comp_meta_data = {}
        for row in txt_data:
            row = eval(row)
            if row == None:continue
            old_doc_name, company_name, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date, review_flg, doc_id, d_studio_map, d_studio_path = '', row.get('CompanyName', ''), row.get('DocType', ''), row.get('FilingType', ''), row.get('Period', ''), row.get('Financial Year', ''), row.get('DocumentName', ''), row.get('Document Release Date', ''), row.get('Document From', ''), row.get('Document To', ''), row.get('Document Download Date', ''), row.get('PreviousReleaseDate', ''), row.get('NextReleaseDate', ''), row.get('review_flg', 1), row.get('DocID', ''), row.get('d_studio_map', ''), row.get('d_studio_path', '')
            company_name = ''.join(company_name.split())
            dc_name = ''
            for s in company_name:
                if s.isalpha():
                    dc_name += s
            company_name = dc_name[:]
            comp_meta_data.setdefault(company_name, []).append((old_doc_name, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date, review_flg, doc_id, d_studio_map, d_studio_path))
        
        for comp, comp_data_lst in comp_meta_data.items():
            db_path_dir = '/mnt/eMB_db/%s/%s/'%(comp, '50')
            if not os.path.exists(db_path_dir):
                os.system('mkdir -p %s'%(db_path_dir))
            txt_data_lst = comp_meta_txt_data[comp]
            self.wrt_comp_meta_db(comp, '50', comp_data_lst)
            self.write_comp_md_txt(comp, '50', txt_data_lst)
            company_id = self.get_company_id_passing_company_name()[comp]
            self.crt_doc_map(comp, '50', company_id)                                 # create doc_map.txt
            self.download_pdf_from_source(comp, '50', company_id, from_project_id)   # Download Documents
        return 'done' 

    def alter_table_coldef(self, conn, cur, table_name, coldef):
        col_info_stmt   = 'pragma table_info(%s);'%table_name
        cur.execute(col_info_stmt)
        all_cols        = cur.fetchall()
        cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
        exists_coldef   = set(coldef)
        new_cols        = list(exists_coldef.difference(cur_coldef))
        col_list = []
        for new_col in coldef:
            if new_col not in new_cols:continue
            col_list.append(' '.join([new_col, 'TEXT']))
        for col in col_list:
            alter_stmt = 'alter table %s add column %s;'%(table_name, col)
            #print alter_stmt
            try:
                cur.execute(alter_stmt)
            except:pass
        conn.commit()
        return 'done'
   
    def get_external_doc_info(self, company_id):
        to_db_name = 'tfms_urlid_%s'%(company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', to_db_name]
        m_conn, m_cur = self.mysql_connection(db_data_lst) 
        crt_qry = """CREATE TABLE IF NOT EXISTS old_new_doc_table_page_map(old_doc TEXT, new_doc TEXT, old_table TEXT, new_table TEXT, pageno TEXT)"""
        m_cur.execute(crt_qry)
        read_qry = """SELECT DISTINCT(new_doc) FROM old_new_doc_table_page_map """
        m_cur.execute(read_qry)
        table_data = m_cur.fetchall()
        m_conn.close()
        doc_str = ', '.join([str(r[0]) for r in table_data])
        return doc_str

    def wrt_comp_meta_db(self, company_name, model_number, data_rows, company_id, doc_ids):
        #d_str = self.get_external_doc_info(company_id)
        db_path_dir = '/mnt/eMB_db/%s/%s/'%(company_name, model_number)
        if not os.path.exists(db_path_dir):
            os.system('mkdir -p %s'%(db_path_dir))
        db_path = os.path.join(db_path_dir, 'tas_company.db')
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
       
        crt_stmt = """CREATE TABLE IF NOT EXISTS company_meta_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, old_doc_name VARCHAR(256), document_type VARCHAR(256), filing_type VARCHAR(256), period VARCHAR(256), reporting_year VARCHAR(256), doc_name VARCHAR(256), doc_release_date VARCHAR(256), doc_from VARCHAR(256), doc_to VARCHAR(256), doc_download_date VARCHAR(256), doc_prev_release_date VARCHAR(256), doc_next_release_date VARCHAR(256), review_flg INTEGER, doc_id INTEGER, d_studio_map TEXT, d_studio_path TEXT)"""
        cur.execute(crt_stmt)
        conn.commit()

        delete_stmt  =  'delete from company_meta_info where doc_id in (%s)'%(', '.join(doc_ids))
        cur.execute(delete_stmt)
        conn.commit()  
        alter_msg = self.alter_table_coldef(conn, cur, 'company_meta_info', ['d_studio_map', 'd_studio_path'])
        
        cur.executemany(""" INSERT INTO company_meta_info(old_doc_name, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date, review_flg, doc_id, d_studio_map, d_studio_path) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", data_rows)
        conn.commit()
        conn.close()
        return

    def alter_table_coldef(self, conn, cur, table_name, coldef):
        col_info_stmt   = 'pragma table_info(%s);'%table_name
        cur.execute(col_info_stmt)
        all_cols        = cur.fetchall()
        cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
        exists_coldef   = set(coldef)
        new_cols        = list(exists_coldef.difference(cur_coldef))
        col_list = []
        for new_col in coldef:
            if new_col not in new_cols:continue
            col_list.append(' '.join([new_col, 'TEXT']))
        for col in col_list:
            alter_stmt = 'alter table %s add column %s;'%(table_name, col)
            #print alter_stmt
            try:
                cur.execute(alter_stmt)
            except:pass
        conn.commit()
        return 'done'

    def read_txt_from_server(self, file_path):
        import bds_company_data_preparation as bds_data
        bds_Obj = bds_data.BDS_Data() 
        disableprint()
        data = bds_Obj.data_preparation_bds()[file_path]
        return data
    
    def check_db(self, company_name, model_number, company_id):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        read_qry = 'SELECT doc_id, page_no, grid_id, row_id from table_dict_phcsv_info;'
        cur.execute(read_qry)
        tb = cur.fetchall()
        
        t = {}
        for row in tb:
            doc_id, page_no, grid_id, table_id = map(str, row)
            t.setdefault((doc_id, page_no, grid_id), []).append(table_id) 
        print t 

    def download_pdf_from_source(self, company_name, model_number, company_id, from_project_id):
        print 'Downloading documents for %s from 172.16.20.52'%(company_name) 
        
        db_path_dir = '/mnt/eMB_db/%s/%s/'%(company_name, model_number)
        db_path = os.path.join(db_path_dir, 'tas_company.db')

        #print db_path
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        stmt = "select distinct(doc_id) from company_meta_info"
        cur.execute(stmt)
        res = cur.fetchall()
        dst_dir = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/input/' %(company_id.split('_')[0], company_id.split('_')[1])
        if not os.path.exists(dst_dir):
            cmd = 'mkdir -p  %s' %(dst_dir)
            os.system(cmd)
        opath = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/pdfpagewise/' %(company_id.split('_')[0], company_id.split('_')[1])
        if not os.path.exists(opath):
            cmd = 'mkdir -p %s' %(opath)
            os.system(cmd)
        for r in res:
            doc_id = str(r[0])
            #'http://172.16.20.229/TASFundamentalsV2/tasfms/data/output/1_common/data/28/input/1.pdf'    
            from_path = 'http://172.16.20.52/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/sieve_input/%s.pdf' %(from_project_id, doc_id, doc_id) 
            to_path = os.path.join(dst_dir, doc_id+'.pdf')
            cmd = 'wget -O %s %s' %(to_path, from_path)
            #print cmd
            os.system(cmd)
            cmd = 'mkdir -p %s' %(opath)
            #print cmd
            os.system(cmd)
    
    def add_table_dict_manual(self, company_name, model_number, company_id):
        fname = '/var/www/html/text_groups_results/%s.sh' %(company_id)
        import shelve
        sh = shelve.open(fname, 'r')
        data = sh.get('data', {})
        sh.close() 
        data_rows = []
        for (doc_id, page_no, grid_id), (table_dict, phcsv_dict) in data.items():
            data_rows.append(('', doc_id, page_no, grid_id, str(table_dict), str(phcsv_dict), 'Y', 'Sentence'))                


        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        crt_stmt = 'CREATE TABLE IF NOT EXISTS table_dict_phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER, page_no INTEGER, grid_id INTEGER, table_dict BLOB, phcsv_dict BLOB)'
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'table_dict_phcsv_info', ['status_flg', 'table_type'])
        stmt = "select doc_id, page_no, grid_id, row_id, table_dict, phcsv_dict from table_dict_phcsv_info where table_type='Sentence'"
        cur.execute(stmt)
        res = cur.fetchall()
        already_mapped_dict = {}
        for row in res:
            doc_id, page_no, grid_id, row_id, table_dict, phcsv_dict = map(str, row[:])
            already_mapped_dict[(doc_id, page_no, grid_id)] = [row_id, table_dict, phcsv_dict]
        ######################################################################
        insert_rows = []
        done_dict = {}
        for doc_tup in data_rows:
            comp_name, doc_id, page_no, grid_id, table_dict, phcsv_dict, status_flg, tt = doc_tup
            if (doc_id, page_no, grid_id) in already_mapped_dict:
                row_id = already_mapped_dict[(doc_id, page_no, grid_id)][0]
                #stmt = "update table_dict_phcsv_info set status_flg='Y', table_dict='%s', phcsv_dict='%s' where doc_id='%s' and page_no='%s' and grid_id='%s' and row_id='%s'" %(table_dict, phcsv_dict, doc_id, page_no, grid_id, row_id)
                cur.executemany("""UPDATE table_dict_phcsv_info SET table_dict=?, phcsv_dict=?, status_flg=?, table_type=? WHERE doc_id=? and page_no=? and grid_id=? and row_id=?""", [(table_dict, phcsv_dict, 'Y', tt, doc_id, page_no, grid_id, row_id)])
                #cur.execute(stmt)
                done_dict[(doc_id, page_no, grid_id)] = 1
            else:
                doc_tup = (doc_id, page_no, grid_id, table_dict, phcsv_dict, status_flg, tt)
                insert_rows.append(doc_tup)
        deleted = list(set(already_mapped_dict.keys()) - set(done_dict.keys()))
        for (doc_id, page_no, grid_id) in deleted:
            stmt = "delete from table_dict_phcsv_info where doc_id='%s' and page_no='%s' and grid_id='%s'" %(doc_id, page_no, grid_id)
            cur.execute(stmt)
        if insert_rows:
            cur.executemany("""INSERT INTO table_dict_phcsv_info(doc_id, page_no, grid_id, table_dict, phcsv_dict, status_flg, table_type) VALUES(?, ?, ?, ?, ?, ?, ?)""", insert_rows)
        conn.commit()
        conn.close()
        return 'done'
    
    def alt(self, company_name, model_number):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        crt_stmt = 'CREATE TABLE IF NOT EXISTS table_dict_phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER, page_no INTEGER, grid_id INTEGER, table_dict BLOB, phcsv_dict BLOB)'
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'table_dict_phcsv_info', ['status_flg', 'table_type'])
        conn.close() 
    
    def generate_txt_file(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT company_name, toc_company_id FROM company_info WHERE project_id="50"; '
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()

        err_comp = []
        for row in table_data[:]:
            cn, toc_company_id  = map(str, row)
            #if toc_company_id not in ('5001'):continue
            cmd = """cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py -8946 %s '{"project_id":"50","model_number":"50","project_name":"BDS  GAAP","norm_scale":"Y", "PRINT":"Y"}';cd -"""%(toc_company_id)
            try:
                os.system(cmd)
                #print cmd
            except:
                err_comp.append(toc_company_id)

        print err_comp
        return
    
    def get_company_id_passing_company_name(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        read_qry = """SELECT company_name, toc_company_id FROM company_info WHERE project_id='50'""" 
        cur.execute(read_qry)
        comp_data = {str(row[0]):'_'.join(['50', str(row[1])]) for row in cur.fetchall()}
        conn.close()
        return comp_data
    
    def read_data_once(self):
        import bds_company_data_preparation as bds_data
        bds_Obj = bds_data.BDS_Data() 
        txt_data_dct = bds_Obj.data_preparation_bds()
        #print txt_data_dct.keys()
        txt_comp_name       = txt_data_dct['/var/www/html/avinash/bds_company_details_229.txt'] 
        txt1_meta           = txt_data_dct['/var/www/html/avinash/bds_company_details_229_1.txt']
        txt2_tkr            = txt_data_dct['/var/www/html/avinash/bds_company_details_229_2.txt']        
        txt3_table_dct      = txt_data_dct['/var/www/html/avinash/bds_company_details_229_3.txt']        
        txt4_cell_dict      = txt_data_dct['/var/www/html/avinash/bds_company_details_229_4.txt']
        #print txt_comp_name 
        #print txt1_meta
        #print txt3_table_dct
        #print txt4_cell_dict
        self.write_4433_company_info(txt_comp_name)
        self.get_data_from_txt(txt1_meta, txt2_tkr)
        self.insert_table_dict_data_3(txt3_table_dct)
        self.insert_table_dict_data_4(txt4_cell_dict)

    def write_4433_company_info(self, company_names_dct, pid):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
   
        read_qry = 'SELECT type_of_company, company_name, toc_company_id FROM company_info where project_id="%s"'%(pid)
        cur.execute(read_qry)
        tb_data = cur.fetchall()
        check_data = {(i[0], i[1]): i[2] for i in tb_data}
        
        if check_data: 
            get_max_id = max(map(int, check_data.values()))
        if not check_data:
            get_max_id = 0
        
        res_comp_dct = {}
        idx = 1
        company_name_deal_map = {}
        for comp_name, d_comp_name in company_names_dct.iteritems():
            if ('TAS-MRD', comp_name) in check_data:
                company_name_deal_map[comp_name] =  check_data[('TAS-MRD', comp_name)]
                continue
            mx_id = get_max_id + idx
            company_name_deal_map[comp_name] =  mx_id
                
            company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path = comp_name, d_comp_name, pid, str(mx_id), 'TAS-MRD', pid, '', 'Y', 'N', 'Jan - Dec', 'FY', '', ''
            insert_stmt = """INSERT INTO company_info(company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(company_name, company_display_name, project_id, str(mx_id), type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path)
            print insert_stmt
            try:
                cur.execute(insert_stmt)
                res_comp_dct[company_name] = company_names_dct[company_name]
            except:continue
            comp_dir = '/mnt/eMB_db/%s/%s/'%(comp_name, pid)
            if not os.path.exists(comp_dir):
                print comp_dir
                os.system('mkdir -p %s'%(comp_dir))
            idx += 1
            print
        conn.commit()
        conn.close()
        if res_comp_dct:
            #pass
            #print res_comp_dct
            #sys.exit()
            self.write_fye_txt(res_comp_dct, pid)
            #print res_comp_dct
            #sys.exit()
            #self.write_fye_txt(res_comp_dct, pid)
        return company_name_deal_map

    def get_doc_ph_dct_hra(self, doc_data):
        doc_ph_dct = {}
        doc_d   = {}
        print 'doc_data ', doc_data
        for row in doc_data:
            if len(row) == 2:
                doc_id, ph = row
                docname = '%s.pdf'%(doc_id)
            else:
                doc_id, ph, docname = row
            doc_d[str(doc_id)]   = 1
            if isinstance(ph, int):
                ph = 'FY'+str(ph)
            period_type, period = ph[:2], ph[2:]
            doc_ph_dct[doc_id] = (period_type, period, docname)
        return doc_ph_dct, doc_d.keys()

    def write_comp_md_txt_hra(self, company_name, model_number):
        sh = ['ER DATE', 'Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2', 'FY', 'M9', 'Filing Frequency', 'Ticker', 'Accounting Standards','Industry','From Year','To Year']
        hdr_str = '\t'.join(sh)
        dt_pr = ['', '', '', '', '', '', '', 'Jan - Dec', '', 'true#FY', '', '', 'TEST', '', '']
        val_str = '\t'.join(dt_pr)
        txt_path = '/mnt/eMB_db/%s/%s/company_meta_info.txt'%(company_name, model_number) 
        f1 = open(txt_path, 'w')
        f1.write(hdr_str + '\n')
        f1.write(val_str)  
        f1.close() 
        return 'done'

    def prepare_metadata_rows(self, doc_ph, doc_wise_meta):
        data_rows = []
        for doc_id, dt_tup in doc_ph.iteritems():
            #doc_name = '%s.pdf'%(doc_id)
            meta_doc = doc_wise_meta.get(str(doc_id), {})
            print meta_doc, doc_id
            ddd, dpd, df, dt, drd, nrd, dc_typ, ft = meta_doc.get('ddd', ''), meta_doc.get('dpd', ''), meta_doc.get('df', ''), meta_doc.get('dt', ''), meta_doc.get('drd', ''), meta_doc.get('nrd', ''), meta_doc.get('dc_typ', ''), meta_doc.get('ft', '')
            data_tup = ('', dc_typ, ft, dt_tup[0], dt_tup[1], dt_tup[2], drd, df, dt, ddd, dpd, nrd, '0', doc_id, '', '')
            data_rows.append(data_tup)
        return data_rows

    def add_meta_data_hra(self, comp, doc_date_lst, cid, project_id, doc_wise_meta): 
        get_doc_ph_dct, docids  =  self.get_doc_ph_dct_hra(doc_date_lst)
        data_rows = self.prepare_metadata_rows(get_doc_ph_dct, doc_wise_meta)
        self.wrt_comp_meta_db(comp, project_id, data_rows, cid, docids)
        #self.write_comp_md_txt_hra(comp, project_id)
    
    def alter_mysql_tables(self, m_cur, m_conn, table_name, arr_columns):
        for col in arr_columns:
            stmt = """ALTER TABLE %s ADD COLUMN %s TEXT"""%(table_name, col)
            try:
                m_cur.execute(stmt)
                m_conn.commit()
            except:pass
        return 'done'
        
    def get_new_table_id(self, to_company_id, source_table_info, source_type, dcid):
        project_id, deal_id = to_company_id.split('_')
        eval_source_tup = eval(source_table_info)
        crt_db_name = 'tfms_urlid_%s'%(to_company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        crt2_stmt = """CREATE TABLE IF NOT EXISTS data_mgmt(resid BIGINT(20), training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), pageno INT(11), taxoid INT(11), taxoname TEXT, doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), data0 TEXT, data1 MEDIUMTEXT, process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, training_tableid INT(11), training_group_id INT(11), applicator_key TEXT, reviewed_user TEXT)"""
        try:
            m_cur.execute(crt2_stmt)
            m_conn.commit()
        except Exception as e:
            print e
        # select *from LastInsertedRow where Id=(SELECT LAST_INSERT_ID()); 
        rd_qry = """SELECT norm_resid, docid, pageno FROM norm_data_mgmt WHERE source_table_info=\"%s\" and source_type=\"%s\" """%(str(source_table_info), source_type)
        m_cur.execute(rd_qry)
        t_data = m_cur.fetchall()
        print 't_data', t_data
        mx_stmt = """SELECT max(norm_resid) FROM norm_data_mgmt"""
        m_cur.execute(mx_stmt)
        mx_t = m_cur.fetchone()[0]
        print mx_t
        if not mx_t:
            print 'here'
            t = 1
        else:
            print 'inc'
            t = copy.deepcopy(mx_t)
            t = t + 1 
        t = int(t)
        print '>>>', t
        if not t_data: 
            stmt = """insert into norm_data_mgmt(norm_resid, project_id, url_id, docid, ref_resid, pageno, active_status, process_status, review_flag, source_table_info, source_type) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \"%s\", '%s')"""%(t, project_id, deal_id, dcid, t, eval_source_tup[1],  'y', 'y', 'y', str(source_table_info), source_type) 
            print stmt
            m_cur.execute(stmt)
            stmt_2 = """insert into data_mgmt(resid, project_id, url_id, docid, pageno, active_status , process_status, taxoname) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(t, project_id, deal_id, dcid, eval_source_tup[1], 'y', 'y', 'grid')
            print stmt_2
            m_cur.execute(stmt_2)
            m_conn.commit()
            m_conn.close()
        return t 

    def get_new_doc_id_new(self, to_company_id, source_doc_info, source_type, document_name, m_conn, m_cur):
        project_id, deal_id = to_company_id.split('_')
        #crt_db_name = 'tfms_urlid_%s'%(to_company_id)
        #db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        #m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT document_id FROM ir_document_master WHERE source_doc_info='%s' and source_type='%s'"""%(source_doc_info, source_type) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        rd_qry = """SELECT max(document_id) FROM ir_document_master"""
        m_cur.execute(rd_qry)
        mx_d = m_cur.fetchone()[0]
        if not mx_d:
            d = 1
            #print 'here'
        else:
            d = copy.deepcopy(mx_d)
            d = d + 1 
            #print '>>>>'
        d = int(d)
        print 'd', d
        if not t_data:  
            ins_stmt = """INSERT INTO ir_document_master(document_id, project_id, url_id, document_name, active_status, source_doc_info, source_type) values('%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(d, project_id, deal_id, document_name, 'Y', source_doc_info, source_type) 
            m_cur.execute(ins_stmt)
            m_conn.commit()
        #sys.exit()
        #m_conn.close()
        return d
    
    def get_new_doc_id(self, to_company_id, source_doc_info, source_type, document_name):
        project_id, deal_id = to_company_id.split('_')
        crt_db_name = 'tfms_urlid_%s'%(to_company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT document_id FROM ir_document_master WHERE source_doc_info='%s' and source_type='%s'"""%(source_doc_info, source_type) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        rd_qry = """SELECT max(document_id) FROM ir_document_master"""
        m_cur.execute(rd_qry)
        mx_d = m_cur.fetchone()[0]
        if not mx_d:
            d = 1
            #print 'here'
        else:
            d = copy.deepcopy(mx_d)
            d = d + 1 
            #print '>>>>'
        d = int(d)
        print 'd', d
        if not t_data:  
            ins_stmt = """INSERT INTO ir_document_master(document_id, project_id, url_id, document_name, active_status, source_doc_info, source_type) values('%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(d, project_id, deal_id, document_name, 'Y', source_doc_info, source_type) 
            m_cur.execute(ins_stmt)
            m_conn.commit()
        #sys.exit()
        m_conn.close()
        return d

    def get_existing_doc_info(self, to_company_id):
        project_id, deal_id = to_company_id.split('_')
        crt_db_name = 'tfms_urlid_%s'%(to_company_id)
        try:
            self.create_databases_mysql(crt_db_name)
            print ['>>>>', crt_db_name]
        except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        crt3_stmt = """CREATE TABLE IF NOT EXISTS ir_document_master(document_id BIGINT(20), project_id INT(11), url_id INT(11), agent_id INT(11), mgmt_id INT(11), user_id INT(11), upload_id int(11), batch_id VARCHAR(64), document_name TEXT, src_type VARCHAR(16), doc_type VARCHAR(16), format_type VARCHAR(64), doc_status VARCHAR(1), total_pages INT(11), page_width INT(11), page_height INT(11), process_date_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, remote_ip VARCHAR(50), stage_id TEXT, reviewed_status VARCHAR(1), reviwed_by VARCHAR(100), active_status VARCHAR(1), display_status_internal VARCHAR(1), display_status_external VARCHAR(1))"""
        try:
            m_cur.execute(crt3_stmt)
            m_conn.commit()
        except Exception as e:
            print e
        self.alter_mysql_tables(m_cur, m_conn, 'ir_document_master', ['source_doc_info', 'source_type'])
        read_qry = """SELECT document_id, source_doc_info, source_type FROM ir_document_master"""
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        doc_info_map_docid = {}
        for row in t_data:
            doc, sdi, src_typ = row
            if sdi:
                sdi =sdi.split('^')[0]
            doc_info_map_docid[(sdi, src_typ)] = doc
        try:
            get_max_doc = max(map(int, doc_info_map_docid.values())) + 1
        except:get_max_doc = 1
        return doc_info_map_docid, get_max_doc
    
    def get_existing_table_info(self, to_company_id):
        crt_db_name = 'tfms_urlid_%s'%(to_company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        crt_stmt = """CREATE TABLE IF NOT EXISTS norm_data_mgmt(norm_resid BIGINT(20), norm_training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), ref_resid BIGINT(20), ref_training_id varchar(600), pageno INT(11), doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), review_flag VARCHAR(1), process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, table_type TEXT, user_info TEXT, mflag INT(2), system_flag INT(2))""" 
        try:
            m_cur.execute(crt_stmt) 
        except Exception as e:
            print e
        self.alter_mysql_tables(m_cur, m_conn, 'norm_data_mgmt', ['source_table_info', 'source_type'])
        read_qry = """SELECT norm_resid, source_table_info, source_type FROM norm_data_mgmt;"""
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        table_info_map_table = {}
        for row in t_data:
            norm_resid, source_table_info, source_type = row
            if source_type:
                source_type = source_type.split('^')[0]
            table_info_map_table[(source_table_info, source_type)] = norm_resid
        try:
            get_max_table = max(map(int, table_info_map_table.values())) + 1
        except:
            get_max_table = 1
        return table_info_map_table, get_max_table

    def alter_table(self, m_conn, m_cur, table_name, column_lst):
        for column_tup in column_lst:
            column, data_type = column_tup
            alter_stmt = """ ALTER TABLE %s ADD COLUMN %s (%s)  """%(column, data_type)
            print alter_stmt
            try:
                pass
                #m_cur.execute(alter_stmt)
                #m_conn.commit()
            except:continue
        return

    def working_with_d2_table_dict_data_doc_table_wise(self, company_name, model_number, company_id, html_cell_data_dict, dc_ph_dct, s_type, doc_wise_meta={}):
        project_id, deal_id = company_id.split('_')

        html_path = '/var/www/html/fundamentals_intf/output/%s/Table_Htmls'%(company_id)
        if not os.path.exists(html_path):
            os.system('mkdir -p %s'%(html_path))
        crt_db_name = 'tfms_urlid_%s'%(company_id)
        try:
            self.create_databases_mysql(crt_db_name)
            print ['>>>>', crt_db_name]
        except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)

        crt_stmt = """CREATE TABLE IF NOT EXISTS norm_data_mgmt(norm_resid BIGINT(20), norm_training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), ref_resid BIGINT(20), ref_training_id varchar(600), pageno INT(11), doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), review_flag VARCHAR(1), process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, table_type TEXT, user_info TEXT, mflag INT(2), system_flag INT(2))""" 
        m_cur.execute(crt_stmt) 
        m_conn.commit()
        

        crt2_stmt = """CREATE TABLE IF NOT EXISTS data_mgmt(resid BIGINT(20), training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), pageno INT(11), taxoid INT(11), taxoname TEXT, doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), data0 TEXT, data1 MEDIUMTEXT, process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, training_tableid INT(11), training_group_id INT(11), applicator_key TEXT, reviewed_user TEXT)"""
        m_cur.execute(crt2_stmt)
        m_conn.commit()

        crt3_stmt = """CREATE TABLE IF NOT EXISTS ir_document_master(document_id BIGINT(20), project_id INT(11), url_id INT(11), agent_id INT(11), mgmt_id INT(11), user_id INT(11), upload_id int(11), batch_id VARCHAR(64), document_name TEXT, src_type VARCHAR(16), doc_type VARCHAR(16), format_type VARCHAR(64), doc_status VARCHAR(1), total_pages INT(11), page_width INT(11), page_height INT(11), process_date_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, remote_ip VARCHAR(50), stage_id TEXT, reviewed_status VARCHAR(1), reviwed_by VARCHAR(100), active_status VARCHAR(1), display_status_internal VARCHAR(1), display_status_external VARCHAR(1))"""
        m_cur.execute(crt3_stmt)
        m_conn.commit()
        pt = '/mnt/eMB_db/%s/%s/table_tagging_dbs/'%(company_name, model_number)
        if not os.path.exists(pt):
            os.system('mkdir -p %s'%(pt))

        lmdbdict = {}
        #print '########################################################################'
        tt_info = {}
        doc_ids_for_meta = []
        from_doc_map, max_doc    = self.get_existing_doc_info(company_id) 
        from_table_map, max_table    = self.get_existing_table_info(company_id)
        #print max_doc
        #print max_table
        d_path = '/mnt/eMB_db/%s/%s/tas_tagging.db'%(company_name, model_number)
        cn = sqlite3.connect(d_path)
        cr = cn.cursor()
        crt_tbl = """CREATE TABLE IF NOT EXISTS UsrTableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(256), xml_id TEXT, period_type TEXT, period TEXT, currency TEXT, value_type TEXT, scale TEXT, month VARCHAR(20), review_flg INTEGER)"""
        cr.execute(crt_tbl)
        read_qry = """SELECT table_id, xml_id, period, period_type, currency, value_type, scale FROM UsrTableCsvPhInfo """
        cr.execute(read_qry)
        tdt = cr.fetchall()
        cn.close()
        chk_table_xml = {}
        for rw in tdt:
            tbid, xl_id, prd, prd_type, crncy, vl_type, scl = rw
            chk_table_xml[(tbid, xl_id)] = {'period':prd, 'period_type':prd_type, 'currency':crncy, 'value_type':vl_type, 'scale':scl}
        i_doc_ar    = []
        i_table_ar  = []
        i_table_ar2  = []
        html_array = [] 
        encryp_lmdb_array = []
        table_xml_dt_lst = []
        ph_info_array = []
        bbox_dct = {}
        project_id, deal_id = company_id.split('_')
        tmp_table_d = {}
        doc_lst = []
        #resultant_table_info_lst = []
        for doc_page_tup, html_cell_dct in html_cell_data_dict.iteritems():
            doc_id, page_no, table_id = map(str, doc_page_tup)
            source_table_info = str((doc_id, page_no, table_id))
            source_doc_info  = copy.deepcopy(doc_id)
            doc_lst.append(doc_id)
            table_type, table_html_str, table_cell_data, batch = html_cell_dct
            #print table_html_str
            #sys.exit()
            #print ['>>>>>>>>>>>>>>>>>', doc_id, page_no, table_id, table_type]
            source_type = copy.deepcopy(s_type)
            if project_id:
                source_type = s_type #+'^'+project_id
            d_n, ph = dc_ph_dct[doc_id]
            if (source_doc_info, source_type) not in from_doc_map:
                #new_docid  = create new doc docid/match exists
                #print source_doc_info, from_doc_map
                new_docid = copy.deepcopy(doc_id) #max_doc #self.get_new_doc_id(company_id, source_doc_info, source_type, d_n) 
                #ins_stmt = """INSERT INTO ir_document_master(document_id, project_id, url_id, document_name, active_status, source_doc_info, source_type) values('%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(d, project_id, deal_id, document_name, 'Y', source_doc_info, source_type) 
                i_doc_ar.append((new_docid, project_id, deal_id, d_n, 'Y', source_doc_info, source_type, deal_id, 21, ''))
                print (new_docid, project_id, deal_id, d_n, 'Y', source_doc_info, source_type)
                max_doc += 1
                from_doc_map[(source_doc_info, source_type)]   = new_docid
            doc_id  = from_doc_map[(source_doc_info, source_type)]
            #print 'HHHHHHHHHHHHHHHH', (doc_id, ph, d_n) 
            if (doc_id, ph, d_n) not in doc_ids_for_meta:
                doc_ids_for_meta.append((doc_id, ph, d_n))
            #doc_ids_for_meta.append((doc_id, ph, d_n))
            if (source_table_info, source_type)  not in from_table_map:
                #new_table  = create new table docid/match exists
                #print source_table_info, from_table_map
                new_table = max_table #self.get_new_table_id(company_id, source_table_info, source_type, doc_id)
                max_table   += 1
                i_table_ar.append((new_table, project_id, deal_id, doc_id, new_table, page_no,  'Y', 'Y', 'Y', source_table_info, source_type, new_table, new_table, deal_id, 21, 1, ''))
                i_table_ar2.append((new_table, project_id, deal_id, doc_id, page_no, 'Y', 'Y', 'Grid', deal_id, 21, 1, '', '0', new_table, new_table))
                #print (new_table, project_id, deal_id, doc_id, new_table, page_no,  'Y', 'Y', 'Y', source_table_info, source_type)
                #print (new_table, project_id, deal_id, doc_id, page_no, 'Y', 'Y', 'Grid')
                #print '>>>>>>>>>>>>>>>>>>>>>>>'
                from_table_map[(source_table_info, source_type)]   = new_table
            #print 
            table_id = from_table_map[(source_table_info, source_type)]
            
            tmp_table_d[str(table_id)]  = source_table_info
            full_table_cell_dict = {}
            full_table_cell_dict[table_id] = copy.deepcopy(table_cell_data) 
            if table_type:
                tt_info[(str(doc_id), str(table_id), table_type)] = 1
            if table_html_str:
                    html_path_str = os.path.join(html_path, '%s.html'%(str(table_id)))
                    html_array.append((html_path_str, table_html_str))
                    #f = open(html_path_str, 'w')
                    #f.write(table_html_str)
                    #f.close()
            cell_doc_path = '/var/www/html/fundamentals_intf/output/'+str(project_id)+'_'+deal_id+'/cell_data/'+str(doc_id)
            if not os.path.exists(cell_doc_path):
                os.system('mkdir -p %s'%(cell_doc_path))
            celldata_db_path = os.path.join(cell_doc_path, str(table_id))
            if os.path.exists(celldata_db_path):
                os.system('rm -rf %s'%(celldata_db_path))
            #lmdb_obj.write_to_lmdb(celldata_db_path, full_table_cell_dict, full_table_cell_dict.keys())   
            table_rc_lst = []
            db_path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/%s.db'%(company_name, model_number, table_id)   
            os.system("rm -rf "+db_path)
            #print db_path
            conn = sqlite3.connect(db_path)
            cur  = conn.cursor()
            crt_stmt = """CREATE TABLE IF NOT EXISTS TableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, org_row VARCHAR(20), org_col VARCHAR(20), nrow VARCHAR(20), ncol VARCHAR(20), cell_type VARCHAR(20), txt  TEXT, xml_id TEXT, period_type TEXT, usr_period_type TEXT, period TEXT, usr_period TEXT, currency TEXT, usr_currency TEXT, value_type TEXT, usr_value_type TEXT, scale TEXT, usr_scale VARCHAR(20))"""
            #print crt_stmt
            cur.execute(crt_stmt)
            #insert_stmt = """INSERT INTO(org_row, org_col, nrow, ncol, cell_type, txt, xml_id, period_type, usr_period_type, period, usr_period, currency, usr_currency, value_type, usr_value_type, scale, usr_scale) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""%(data_rows) 
            #cur.execute(insert_stmt)
            if 0:
                del_stmt = "DELETE FROM TableCsvPhInfo"
                cur.execute(del_stmt)
            for r_c, ddict in  table_cell_data.items():
                r, c = r_c
                dt = ()
                if ddict['section_type'] == 'GV':
                    xml_id =  '#'.join(ddict['text_ids'])
                    key = str(table_id)+'_'+self.get_quid(xml_id)
                    pkey = 'PH_MAP_'+str(key)
                    ph_date = ddict.get('ph', '')#dc_ph_dct[doc_id] #date_ar[doc_ar.index(doc_id)]  
                    if isinstance(ph_date, int):
                        ph_date = 'FY'+str(ph_date)
                    if ph_date:
                        period_type = ph_date[:-4].split('.')[-1] 
                    else:
                        period_type = ''
                 
                    print ph_date 
                    period = ph_date[-4:]#''.join(ph_date.split('.')[:2])
                    currency = ddict.get('currency', '')
                    value_type = ddict.get('value_type', '')
                    if currency:
                        value_type = 'MNUM'
                    scale    = ddict.get('scale', '')
                    print '\t', '\t','>>>>', [period_type, period, currency, scale, value_type, table_id, doc_id, source_table_info]
                    if type(scale) == type([]):
                        scale = str(scale[0])
                    gt_tbx_dct = chk_table_xml.get((str(table_id), str(xml_id)), {})
                    usr_prd  = gt_tbx_dct.get('period', '')
                    if gt_tbx_dct.get('period'):
                        period  = gt_tbx_dct['period']
                    usr_pt  = gt_tbx_dct.get('period_type', '')
                    if gt_tbx_dct.get('period_type'):
                        period_type  = gt_tbx_dct['period_type']
                    usr_crn  = gt_tbx_dct.get('currency', '')
                    if gt_tbx_dct:#.get('currency'):
                        currency  = gt_tbx_dct['currency']
                    usr_vlt  = gt_tbx_dct.get('value_type', '')
                    if gt_tbx_dct.get('value_type'):
                        value_type  = gt_tbx_dct['value_type']
                    usr_scl = gt_tbx_dct.get('scale', '')
                    if gt_tbx_dct.get('scale'):
                        scale = gt_tbx_dct['scale']
                    pval = '^'.join([period_type, period, currency, scale, value_type])
                    if usr_vlt:
                        print 'pval', pval
                    lmdbdict[pkey] = pval
                    txt = ' '.join(ddict['text_lst'])
                    txt = txt.replace('"', '').replace("'", '')
                    table_rc_lst.append((r, c, r, c, str(ddict['section_type']), txt, xml_id, period_type, usr_pt, period, usr_prd, currency, usr_crn, value_type, usr_vlt, scale, usr_scl))  
                xml_id =  '#'.join(ddict['text_ids'])  
                bbox_dct[(table_id, xml_id)]  = ddict['bbox_lst']
        #insert_stmt = """INSERT INTO TableCsvPhInfo(org_row, org_col, nrow, ncol, cell_type, txt, xml_id, period_type, usr_period_type, period, usr_period, currency, usr_currency, value_type, usr_value_type, scale, usr_scale) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            encryp_lmdb_array.append((celldata_db_path, full_table_cell_dict, full_table_cell_dict.keys(), table_rc_lst, company_name, model_number, table_id))  
            #ph_info_array.append((company_name, model_number, table_id, table_rc_lst))
    
        if i_doc_ar:
            ins_stmt = """INSERT INTO ir_document_master(document_id, project_id, url_id, document_name, active_status, source_doc_info, source_type, agent_id, user_id, doc_type ) values(%s, %s, %s, %s, %s, %s, %s,%s,%s, %s)""" 
            m_cur.executemany(ins_stmt, i_doc_ar)
        if i_table_ar:
            stmt_t1 = """insert into norm_data_mgmt(norm_resid, project_id, url_id, docid, ref_resid, pageno, active_status, process_status, review_flag, source_table_info, source_type, norm_training_id, ref_training_id, agent_id, user_id, mgmt_id, doc_type) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            m_cur.executemany(stmt_t1, i_table_ar)
    
        if i_table_ar2:
            stmt_2 = """insert into data_mgmt(resid, project_id, url_id, docid, pageno, active_status , process_status, taxoname, agent_id, user_id, mgmt_id, doc_type, taxoid, training_id, training_tableid) values(%s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s)"""
            m_cur.executemany(stmt_2, i_table_ar2)
        m_conn.commit()
        #self.write_gv_info_db_table(m_conn, m_cur, resultant_table_info_lst)
        m_conn.close() 
        lmdb_path = "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        os.system('mkdir -p '+lmdb_path)
        self.get_update_lmdb(lmdb_path, lmdbdict)
        if tt_info: 
            self.write_tt_info(company_name, project_id, tt_info, company_id)
        #print ph_info_array
        #sys.exit()
        #pool = Pool(10)
        #pool.map(run_html_process, html_array)  
        pool2 = Pool(10)
        print len(encryp_lmdb_array)
        pool2.map(run_lmdb_process, encryp_lmdb_array) 
        self.write_bbox(company_id, bbox_dct)
        #print tmp_table_d
        return doc_ids_for_meta, tmp_table_d, doc_lst
    
    def write_bbox(self, company_id, bbox_dct):
        lmdb_path    = os.path.join('/var/www/html/fill_table/', company_id, 'XML_BBOX')
        if not os.path.exists(lmdb_path):
            os.system("mkdir -p "+lmdb_path)
        env1 = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
        #print lmdb_path, len(bbox_dct.keys())
        with env1.begin(write=True) as txn1:
            for table_xml, info in bbox_dct.items():
                tablid = table_xml[0]
                xmlid  = table_xml[1]
                #print tablid, xmlid, 'Total - ', total, 'Remaining - ', total-ind
                #print [company_id, str(tablid)+':$$:'+str(xmlid)]
                xmlid = hashlib.md5(str(xmlid)).hexdigest()
                txn1.put(str(tablid)+':$$:'+str(xmlid), str(info))
        os.system("chmod -R 777 "+lmdb_path)
        return 
            

    def write_tt_info(self, company_name, model_number, tt_info, company_id):
        #d_str = self.get_external_doc_info(company_id)
        data_dct = self.get_sheet_id_map()
        #print data_dct
        #sys.exit()
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        crt_qry = """CREATE TABLE IF NOT EXISTS table_group_mapping(row_id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id INTEGER, doc_id VARCHAR(256), doc_name VARCHAR(250), table_id VARCHAR(250), parent_table_type TEXT)"""
        cur.execute(crt_qry)
        #try:
        #    del_stmt = 'delete from table_group_mapping where doc_id not in (%s);'%(d_str)
        #    cur.execute(del_stmt)
        #except:pass
            
        for rw, s in tt_info.iteritems():
            doc_id, table_id, table_type = rw 
            dc_name = '%s.pdf'%(doc_id)
            if table_type == 'RevenueBySegment':
                table_type = 'RBS'
            elif table_type == 'RevenueByGeography':
                table_type = 'RBG'
                #sid = data_dct['RBS'] 
            if table_type not in data_dct:continue
            sid = data_dct[table_type] 
            cur.execute("""INSERT INTO table_group_mapping(sheet_id, doc_id, doc_name, table_id, parent_table_type) VALUES('%s', '%s', '%s', '%s', '%s')"""%(sid, doc_id, dc_name, table_id, table_type))
            conn.commit()
        conn.close()
    
    def get_sheet_id_map(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn        = sqlite3.connect(db_file)
        cur      =   conn.cursor() 
        sql   = "select sheet_id, node_name from node_mapping where review_flg = 0"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        #print rr, len(tres)
        ddict = {}
        for tr in tres:
            sheet_id, node_name = map(str, tr)
            ddict[node_name] = sheet_id
        return ddict
    
    

    def insert_cell_data(self, company_name, model_number, company_id, d1):
        #d1, d2, oc = self.form_builder(doc_ar, date_ar, company_name, deal_id, project_id)
            
        #company_name = 'AFImmobilienGmbHampCoKG'
        #model_number = '1'
        db_path = '/mnt/eMB_db/%s/%s/taxo_data_builder.db'%(company_name, model_number) 
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()   
        if 1:
            del_qry = 'drop table mt_data_builder'
            try:
                cur.execute(del_qry)
            except:
                pass
        crt_stmt = 'CREATE TABLE IF NOT EXISTS mt_data_builder(row_id INTEGER PRIMARY KEY AUTOINCREMENT, taxo_id INTEGER, prev_id INTEGER, order_id INTEGER, table_type TEXT, taxonomy TEXT, user_taxonomy TEXT, missing_taxo TEXT, table_id TEXT, c_id TEXT, gcom TEXT, ngcom TEXT, ph TEXT, ph_label TEXT, user_name TEXT, datetime TEXT, isvisible TEXT, m_rows TEXT, vgh_text TEXT, vgh_group TEXT, doc_id INTEGER, xml_id TEXT, period VARCHAR(50), period_type VARCHAR(50), scale VARCHAR(50), currency VARCHAR(50), value_type VARCHAR(50), user_label TEXT)'
        cur.execute(crt_stmt)
        #conn.commit()
        #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'    
        data_rows = []
        with conn:
            sql         = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r           = cur.fetchone()
            if not r:
                g_id    = 1
            else:
                g_id    = int(r[0])+2
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            try:
                tg_id    = int(r[0])+1
            except:
                tg_id    = 1
            g_id    = max(g_id, tg_id)
            for tt, tt_lst in d1.items():
                #tt_lst = d1.get(tt, [])
                #if not tt_lst:continue
                
                #print tt_lst
                for s in tt_lst:
                    for rw in s:
                        #print 'rw: ', rw 
                        pass

                for s in tt_lst:
                    g_id    += 1
                    for rw in s:
                        #print rw 
                        taxo_id, order_id, taxonomy, table_id, doc_id, xml_id, ph = g_id, rw.get('order_id', 0), rw.get('taxonomy', 0), rw.get('tableid', ''), rw.get('doc_id', 0), rw.get('xml_id', 0), rw.get('ph', '')
                        if tt == 'RevenueBySegment':
                            tt = 'RBS'
                        elif tt == 'RevenueByGeography':
                            tt = 'RBG'
                        data = (taxo_id, order_id, taxonomy, table_id, doc_id, xml_id, 'Label', tt, 'Y')
                        data_rows.append(data)
        #sys.exit()
        #return
        if data_rows:
            #print '>>>>>>>>>>>>>>>>>>>>>>', data_rows
            cur.executemany('INSERT INTO mt_data_builder(taxo_id, order_id, taxonomy, table_id, doc_id, xml_id, ph, table_type, isvisible) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', data_rows) 
            conn.commit()
        conn.close()  
        return

    def get_ph_dc_dct(self, company_name, model_number):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """SELECT doc_id, period, reporting_year FROM company_meta_info"""
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        res_dct = {}
        for row in table_data:
            doc_id, period, reporting_year = map(str, row)
            res_dct[doc_id] = ''.join([period, reporting_year])
        return res_dct
   
    def doc_ph_lst_dct(self, doc_data):
        dc_ph_dct = {}
        for row in doc_data:
            if len(row)  > 2:
                doc, ph, dc_name = row
            elif len(row) == 2:
                doc, ph = row
                dc_name = '%s.pdf'%(doc)
            dc_ph_dct[doc] = (dc_name, ph)
        return dc_ph_dct

    
    def get_company_info(self, sh_data_dct, project_id, s_type):
        #sh_path = shelve.open('/root/prashant/data.sh')
        #sh_path = shelve.open('/root/prashant/data.sh')
        #sh_path = shelve.open('/root/prashant/rest_3_data.sh')
        #sh_data_dct = sh_path['data']
        #sh_path.close()    
        comp_keys = sh_data_dct.keys()
        comp_dict = {}
        for d_name in comp_keys:
            c_name = ''
            for elm in d_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            comp_dict[company_name]  = d_name
        #print comp_dict
        #self.write_4433_company_info(comp_dict, project_id) # no change
        #sys.exit()
        error_company_dct  = {}
        c_d = self.get_company_id_pass_company_name(project_id)
        table_d = {}
        for d_company_name, company_info_lst in  sh_data_dct.iteritems():
            c_name = '' 
            for elm in d_company_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            #if company_name != 'ASUSTEKCOMPUTERINCANDSUBSIDIARIES':continue
            data_builder_tt_wise, doc_table_html_cell_data, doc_ph_lst, order_class, doc_wise_meta = company_info_lst 
            get_cid = c_d[company_name]
            dc_ph_dct = self.doc_ph_lst_dct(doc_ph_lst)
        
            doc_ids_for_meta, tmp_table_d, doc_lst = self.working_with_d2_table_dict_data_doc_table_wise(company_name, project_id, get_cid, doc_table_html_cell_data, dc_ph_dct, s_type, doc_wise_meta)
            table_d.update(tmp_table_d)
            self.add_meta_data_hra(company_name, doc_ids_for_meta, get_cid, project_id, doc_wise_meta) # no change
            self.crt_doc_map(company_name, project_id, get_cid) # no change
            d_str = '#'.join(doc_lst)
            #os.system('python populate_projectid20_deals.py -c="%s" -d="%s" '%(get_cid, d_str))
            #self.insert_cell_data(company_name, project_id, get_cid, data_builder_tt_wise) # no change
            #os.system('python index_all_table.py %s ALL'%(get_cid))
            #print doc_ids_for_meta 
        return table_d
   

 
if __name__ == '__main__':
    obj = Company_populate_new()
    company_name = sys.argv[1]#'TataMotorsLimited'
    company_display_name = sys.argv[2]#'Tata Motors Limited'
    project_id   = sys.argv[3]#'20'
    #print company_name, company_display_name, project_id 
    ###################################################################################################################################################
    #sys.exit()
    # function to add new company for project_id 20
    obj.write_4433_company_info({company_name:company_display_name}, project_id)
    ###################################################################################################################################################
    #from_project_id = '21'
    #obj.write_4433_company_info({'HyundaiEngineeringandConstructionCoLtd':'Hyundai Engineering and Construction Co Ltd'}, '20')
    #print obj.get_company_info()
    #print obj.read_data_once()
    #print obj.read_txt_from_server('/var/www/html/avinash/bds_company_details_229.txt')
    #print obj.write_4433_company_info()                                                     #(1) add_company
    #print obj.get_data_from_txt()                                                            #(2) doc_meta_data, company_meta_data
    #print obj.insert_table_dict_data_3()                                                     #(3) insert table_dict
    #print obj.insert_table_dict_data_4()                                                     #(4) insert cell_dict
    #print obj.get_cell_dict_ds(company_name, model_number, company_id)                      #(5) xml_bbox
    #print obj.store_page_coordinates_lmdb(company_name, model_number, company_id)           #(6) store page coordinates
    #print obj.crt_doc_map(company_name, model_number, company_id)                           #(7) create doc_map.txt
    #obj.download_pdf_from_source(company_name, model_number, company_id, from_project_id)   #(8) Download Documents
    #obj.generate_txt_file()                                                                 #(9) Generate_txt_file 
