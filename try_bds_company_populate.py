#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime, sqlite3

class Company_populate_bds(object):
    def __init__(self):
        pass
        
    def working_info(self, arr):
        for j, i in enumerate(arr):
            while j > 0:
                if a[j-1] > a[j]:
                    a[j-1], a[j] = a[j], a[j-1]
                    j -= 1
                else:break

    def bl_st(self, arr):
        """
        [10, 16, 4, 1, 32, 21]
        [10, 4, 16, 1, 32, 21]
        [10, 4, 1, 16, 32, 21]
        [10, 4, 1, 16, 21, 32]
        [4, 10, 1, 16, 21, 32]
        [4, 1, 10, 16, 21, 32]
        [1, 4, 10, 16, 21, 32]
        """
        n = len(arr) 
        for i in range(n): 
            for j in range(n-i-1): 
                if arr[j] > arr[j+1]: 
                    arr[j], arr[j+1] = arr[j+1], arr[j]
    
    def ss(self, alist):
        for i in range(0, len(alist) - 1):
            smallest = i
            for j in range(i + 1, len(alist)):
                if alist[j] < alist[smallest]:
                    smallest = j
            alist[i], alist[smallest] = alist[smallest], alist[i]

    def in_so(self, arr):
        for i in range(1, len(arr)): 
            key = arr[i] 
            j = i-1
            while j >= 0 and key < arr[j] : 
                    arr[j + 1] = arr[j] 
                    j -= 1
            arr[j + 1] = key 
        return arr

    def write_to_4433_compnay(self):
        data_rows = [('', '', '1', '', '', '', '', '', '', '', '', '')]
        db_path = self.config['cinfo'] 
        conn, cur = sqlite3.connect(db_path)
        
        cur.executemany("INSERT INTO company_info('company_name', 'company_display_name', 'project_id', 'toc_company_id', 'type_of_company', 'model_number', 'industry_type', 'internal_status', 'external_status', 'reporting_year', 'filing_frequency', 'd_studio_map') VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data_rows)
        '''
        ['company_name TEXT', 'company_display_name TEXT', 'project_id varchar(1024)', 'toc_company_id varchar(1024)', 'type_of_company varchar(1024)', 'model_number varchar(1024)', 'industry_type varchar(1024)', 'internal_status varchar(1024)', 'external_status varchar(1024)', 'reporting_year varchar(1024)', 'filing_frequency varchar(1024)', 'd_studio_map TEXT', 'd_studio_path TEXT']
    
        ['AegonLifeInsuranceCompany', 'Aegon Life Insurance Company ', '1', '128', 'TAS-MRD', '1', 'Insuranc', 'Y', 'N', 'Apr - Mar', 'Q1, FY', '', '']
    
        {'company_name':'AegonLifeInsuranceCompany', 
            'company_display_name': 'Aegon Life Insurance Company ', 
            'project_id': '1', 
            'toc_company_id':'128',
            'type_of_company':'TAS-MRD', 
            'model_number':'1', 
            'industry_type':'Insurance', 
            'internal_status':'Y', 
            'external_status': 'N',
            'reporting_year':'Apr - Mar', 
            'filing_frequency':'Q1, FY', 
            'd_studio_map': '', 
            'd_studio_path':''}
        
        {'CompanyName':'AegonLifeInsuranceCompany', 
         'DocType':'PeriodicFinancialStatement', 
         'FilingType':'',
         'Period':'FY', 
         'Financial Year':'2018', 
         'DocumentName':'AegonLifeInsuranceCompany_PeriodicFinancialStatement__FY_2018.pdf',
         'Document Release Date':'', 
         'Document From':'01-01-2016',
         'Document To':'31-12-2016',
         'Document Download Date':'02-05-2019',
         'PreviousReleaseDate':'',
         'NextReleaseDate':''   }
         
        {'company_name':'AegonLifeInsuranceCompany',
            'deal_id':'128', 
            'Ticker':'AMS: AGN',
            'Industry':'Insurance'
            'Currency':'Eur', 
            'Accounting Standards':'', 
            'Reporting Date From':'January',
            'Reporting Date To':'December', 
            'Filing  Frequency':'FY,H1',
            'Q1':'',
            'Q2':'',
            'H1':'January - June',
            'Q3':'',
            'M9':'',
            'Q4':'',
            'H2':'',
            'FY':'January - December'
        }
        conn.commit()
        conn.close()
        def insertionSort(arr): 
  
        # Traverse through 1 to len(arr) 
        for i in range(1, len(arr)): 
      
            key = arr[i] 
      
            # Move elements of arr[0..i-1], that are 
            # greater than key, to one position ahead 
            # of their current position 
            j = i-1
            while j >= 0 and key < arr[j] : 
                    arr[j + 1] = arr[j] 
                    j -= 1
            arr[j + 1] = key 
        '''
    def adi(self, arr):
        for i in range(1, len(arr)):
            key = arr[i]
            j = i-1
            while j >= 0 and key < arr[j] : 
                    arr[j + 1] = arr[j] 
                    j -= 1
            arr[j + 1] = key 

    def write_company_company_data_txt(self, company_name, model_number, company_id, display_name):
        f_path = "/root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/data/company_info.txt" 
        project_id, deal_id = company_id.split('_')
        dt_str = '\t'.join([company_name, display_name, project_id, deal_id, 'BDS-MRD', model_number])
        f = open(f_path, 'a')
        f.write(dt_str+'\n')
        f.close()
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
            comp_name, display_name, proj_id, dl_id, template_name, model_no = rw.split('\t')
            cid_s = '_'.join([proj_id, dl_id])
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
                dt_str = '\t'.join([company_name, company_display_name, project_id, toc_company_id, 'BDS-MRD', model_number]) 
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

    
    def write_4433_company_info(self):
        #f = open('bds_company_details_229.txt') 
        #txt_data = f.readlines()
    
        file_path = '/var/www/html/avinash/bds_company_details_229.txt'
        txt_data  = self.read_txt_from_server(file_path)
    
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
   
        read_qry = 'SELECT type_of_company, company_name, toc_company_id FROM company_info where project_id="50"'
        cur.execute(read_qry)
        tb_data = cur.fetchall()
        check_data = {(i[0], i[1]): i[2] for i in tb_data}
        
        if check_data: 
            get_max_id = max(map(int, check_data.values()))
        if not check_data:
            get_max_id = 0
        
        res_comp_dct = {}
        idx = 1
        for row in txt_data:
            row = eval(row)
            if row == 'None':continue
            mx_id = get_max_id + idx
                
            company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path = row['company_name'], row['company_display_name'], row.get('project_id', ''), row.get('toc_company_id', ''), row.get('type_of_company', ''), row.get('model_number', '50'), row.get('industry_type', ''), row.get('internal_status', ''), row.get('external_status', ''), row.get('reporting_year', ''), row.get('filing_frequency', ''), row.get('d_studio_map', ''), row.get('d_studio_path', '')
            company_name = ''.join(company_name.split())
            dc_name = ''
            for s in company_name:
                if s.isalpha():
                    dc_name += s
            company_name = dc_name[:]
            if company_name in ('Bu00fchlerGmbH', 'Matthu00e4iBauunternehmenGmbH&Co.KG', 'PorrDeutschlandGmbH'):continue
            if (type_of_company, company_name) in check_data:continue
            
            insert_stmt = """INSERT INTO company_info(company_name, company_display_name, project_id, toc_company_id, type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""%(company_name, company_display_name, project_id, str(mx_id), type_of_company, model_number, industry_type, internal_status, external_status, reporting_year, filing_frequency, d_studio_map, d_studio_path)
            print insert_stmt
            try:
                cur.execute(insert_stmt)
                res_comp_dct[company_name] = 1
            except:continue
            idx += 1
        conn.commit()
        conn.close()
        self.write_fye_txt(res_comp_dct, '50')
        return 'done'

    def read_3_txt(self):
        res_dct = {}
        #f = open('bds_company_details_229_3.txt')
        #txt_data = f.readlines()
        file_path = '/var/www/html/avinash/bds_company_details_229_3.txt'
        txt_data  = self.read_txt_from_server(file_path)
        for row in txt_data:
            #if eval(row) == None:continue
            #row = eval(row)
            if row == None:continue
            comp_name, doc_id, page_no, grid_id, table_dict, phcsv_dict = row.split('\t')
            comp_name = ''.join(comp_name.split())
            dc_name = ''
            for s in comp_name:
                if s.isalpha():
                    dc_name += s
            comp_name = dc_name[:]
            res_dct.setdefault(comp_name, []).append((comp_name, doc_id, page_no, grid_id, str(table_dict), str(phcsv_dict)))
        return res_dct
        
    def crt_db_3_del(self, company_name, model_number, data_rows):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        crt_stmt = 'CREATE TABLE IF NOT EXISTS table_dict_phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER, page_no INTEGER, grid_id INTEGER, table_dict BLOB, phcsv_dict BLOB)'
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'table_dict_phcsv_info', ['status_flg', 'table_type'])
        stmt = "select doc_id, page_no, grid_id, row_id, table_dict, phcsv_dict from table_dict_phcsv_info"
        cur.execute(stmt)
        res = cur.fetchall()
        already_mapped_dict = {}
        for row in res:
            doc_id, page_no, grid_id, row_id, table_dict, phcsv_dict = map(str, row[:])
            already_mapped_dict[(doc_id, page_no, grid_id)] = [row_id, table_dict, phcsv_dict]
        ######################################################################
        insert_rows = []
        done_dict = {}
        for row in data_rows:
            doc_id, page_no, grid_id = map(str, [row[1], row[2], row[3]])
            table_dict, phcsv_dict = str(row[4]), str(row[5])
            if (doc_id, page_no, grid_id) in already_mapped_dict:
                row_id = already_mapped_dict[(doc_id, page_no, grid_id)][0]
                #stmt = "update table_dict_phcsv_info set status_flg='Y', table_dict='%s', phcsv_dict='%s' where doc_id='%s' and page_no='%s' and grid_id='%s' and row_id='%s'" %(table_dict, phcsv_dict, doc_id, page_no, grid_id, row_id)
                cur.executemany("""UPDATE table_dict_phcsv_info SET table_dict=?, phcsv_dict=?, status_flg=? WHERE doc_id=? and page_no=? and grid_id=? and row_id=?""", [(table_dict, phcsv_dict, 'Y', doc_id, page_no, grid_id, row_id)])
                #cur.execute(stmt)
                done_dict[(doc_id, page_no, grid_id)] = 1
            else:
                doc_tup = (doc_id, page_no, grid_id, table_dict, phcsv_dict, 'Y')
                insert_rows.append(doc_tup)
        deleted = list(set(already_mapped_dict.keys()) - set(done_dict.keys()))
        for (doc_id, page_no, grid_id) in deleted:
            stmt = "delete from table_dict_phcsv_info where doc_id='%s' and page_no='%s' and grid_id='%s'" %(doc_id, page_no, grid_id)
            cur.execute(stmt)
        if insert_rows:
            cur.executemany("""INSERT INTO table_dict_phcsv_info(doc_id, page_no, grid_id, table_dict, phcsv_dict, status_flg) VALUES(?, ?, ?, ?, ?, ?)""", insert_rows)
        conn.commit()
        conn.close()
        return 'done'
            
    def crt_db_3(self, company_name, model_number, data_rows, del_flg=0):
         #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()

        if del_flg:
            del_stmt = 'drop table table_dict_phcsv_info;'
            try:
                cur.execute(del_stmt)
                conn.commit()
            except:pass
        
        crt_stmt = 'CREATE TABLE IF NOT EXISTS table_dict_phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER, page_no INTEGER, grid_id INTEGER, table_dict BLOB, phcsv_dict BLOB)'
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'table_dict_phcsv_info', ['status_flg'])

        read_qry = 'SELECT doc_id, page_no, grid_id FROM table_dict_phcsv_info'
        cur.execute(read_qry)
        check_data = {tuple(map(str, rw)):1 for rw in cur.fetchall()}
       
        # update status_flg = 'N' for  for the check_data 
        #for (d, p, g) in check_data.keys():
        #    st = 'UPDATE table_dict_phcsv_info SET status_flg="%s" WHERE doc_id="%s" and page_no="%s" and grid_id="%s"' %('N', d, p, g)
        #conn.commit()
                    
        insert_rows = []
        update_rows = []
        for row in data_rows:
            if (row[1], row[2], row[3]) in check_data:
                update_rows.append((row[4], row[5], 'Y',  row[1], row[2], row[3]))
            else:
                insert_rows.append(tuple(list(row[1:])+['Y']))
        if insert_rows:
            cur.executemany("""INSERT INTO table_dict_phcsv_info(doc_id, page_no, grid_id, table_dict, phcsv_dict, status_flg) VALUES(?, ?, ?, ?, ?, ?)""", insert_rows)
            pass
        if update_rows:
            cur.executemany("""UPDATE table_dict_phcsv_info SET table_dict=?, phcsv_dict=?, status_flg=? WHERE doc_id=? and page_no=? and grid_id=?""", update_rows)

            pass
        conn.commit()
        conn.close()
        return 'done'
    
    def insert_table_dict_data_3(self):
        txt_data_dict = self.read_3_txt()
        for comp, data_list in txt_data_dict.iteritems():
            db_path_dir = '/mnt/eMB_db/%s/%s/'%(comp, '50')
            if not os.path.exists(db_path_dir):
                os.system('mkdir -p %s'%(db_path_dir))
            self.crt_db_3_del(comp, '50', data_list)
        return 'Inserted table dict'
    
    def read_4_txt(self):
        res_dct = {}
        file_path = '/var/www/html/avinash/bds_company_details_229_4.txt'
        txt_data  = self.read_txt_from_server(file_path)
        for row in txt_data:
            if row == None:continue
            comp_name, doc_id, page_no, cell_dict = row.split('\t')
            comp_name = ''.join(comp_name.split())
            dc_name = ''
            for s in comp_name:
                if s.isalpha():
                    dc_name += s
            comp_name = dc_name[:]
            res_dct.setdefault(comp_name, []).append((comp_name, doc_id, page_no, str(cell_dict)))
        return res_dct
        
    def crt_db_4(self, company_name, model_number, data_rows, del_flg=0):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        if del_flg:
            del_stmt = 'drop table cell_dict_info'
            try:
                cur.execute(del_stmt)
                conn.commit()    
            except:pass
        
        crt_stmt = 'CREATE TABLE IF NOT EXISTS cell_dict_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER, page_no INTEGER, cell_dict BLOB)'
        cur.execute(crt_stmt)
        conn.commit()
        
        read_qry = 'SELECT doc_id, page_no FROM cell_dict_info'
        cur.execute(read_qry)
        check_data = {tuple(map(str, rw)):1 for rw in cur.fetchall()}
        
        insert_rows = []
        update_rows = []
        for row in data_rows:
            if (row[1], row[2]) in check_data:
                update_rows.append((row[3], row[1], row[2]))
            else:
                insert_rows.append(tuple(list(row[1:])))
        cur.executemany("""INSERT INTO cell_dict_info(doc_id, page_no, cell_dict) VALUES(?, ?, ?)""", insert_rows)
        cur.executemany("""UPDATE cell_dict_info SET cell_dict=? WHERE doc_id=? and page_no=?""", update_rows)
        conn.commit()
        conn.close()
        return 'done'
    
    def insert_table_dict_data_4(self):
        from_project_id = '21'
        txt_data_dict = self.read_4_txt()
        for comp, data_list in txt_data_dict.iteritems():
            self.crt_db_4(comp, '50', data_list, 0)
            company_id = self.get_company_id_passing_company_name()[comp]
            #print comp, company_id 
            self.get_cell_dict_ds(comp, '50', company_id)                                           
            self.store_page_coordinates_lmdb(comp, '50', company_id)                 # page coordinates 
            self.crt_doc_map(comp, '50', company_id)                                 # create doc_map.txt
            self.download_pdf_from_source(comp, '50', company_id, from_project_id)   # Download Documents
        return 'Inserted cell dict and created xml-bbox'
    
    def get_table_dict_ds(self, company_name, model_number):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        read_qry = 'SELECT doc_id, page_no, row_id, table_dict FROM table_dict_phcsv_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        table_dict_ds = {}
        doc_page_distinct_xml = {}
        for row in table_data:
            doc_id, page_no, row_id   = map(str, row[:3]) #table_dict
            table_dict = eval(row[3])
            xml_dct = {}
            dp_xml_dct = {}
            for rc, data_dct in table_dict.iteritems():
                get_xml_lst = data_dct['text_ids'] 
                dct_x = {xm:1 for xm in get_xml_lst if xm}        
                dp_xm = {xm:row_id for xm in get_xml_lst if xm}
                xml_dct.update(dct_x)
                dp_xml_dct.update(dp_xm)
            table_dict_ds.setdefault((doc_id, page_no), {}).setdefault(row_id, {}).update(xml_dct)
            doc_page_distinct_xml.setdefault((doc_id, page_no), {}).update(dp_xml_dct)
        
        
        #for k, v in doc_page_distinct_xml.items():
        #    print k
        #    print v
        #    print
        return table_dict_ds, doc_page_distinct_xml 

    def get_cell_dict_ds(self, company_name, model_number, company_id):
        table_dict_ds, doc_page_distinct_xml = self.get_table_dict_ds(company_name, model_number)
        
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        read_qry = 'SELECT doc_id, page_no, cell_dict FROM cell_dict_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        res_dct = {}
        for row in table_data:
            doc_id, page_no = map(str, row[:2]) 
            cell_dict = eval(row[2])
            for rc, data_dct in cell_dict.iteritems():
                get_dp_data = doc_page_distinct_xml.get((doc_id, page_no), {})
                if not get_dp_data:continue
                get_cl_dct = data_dct['cell_dict']
                for chk_dct in get_cl_dct['chunks']:
                    get_xml = chk_dct['xmlID'] 
                    get_table_id = get_dp_data.get(get_xml, '')
                    if not get_table_id:continue
                    res_dct.setdefault(get_table_id, {})[get_xml] = [[list(get_cl_dct['cord'])], page_no]
        output_path = '/var/www/html/fundamentals_intf/output/'
        lmdb_folder = os.path.join(output_path, company_id)
        if not os.path.exists(lmdb_folder):
            os.system('mkdir -p %s'%(lmdb_folder))
        fname = os.path.join(lmdb_folder, 'xml_bbox_map')
        #if not tables_lddist: # Running for all tables, hence remove old data and create new
        if os.path.exists(fname):
            cmd = 'rm -rf %s'%(fname)
            os.system(cmd)
        env = lmdb.open(fname, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
            for k, v in res_dct.items():
                txn.put('RST:'+k, str(v))
        return "done"
        
    def store_page_coordinates_lmdb(self, company_name, model_number, company_id):
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        
        read_qry = 'SELECT doc_id, page_no, cell_dict FROM cell_dict_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        
        doc_page_pg_cord_dct = {}
        for row in table_data:
            doc_id, page_no = map(str, row[:2]) 
            cell_dict = eval(row[2])
            for rc, data_dct in cell_dict.iteritems():
                pg_cord = data_dct['page_rect'].split('_')
                doc_page_pg_cord_dct.setdefault(doc_id, {}).setdefault(page_no, []).extend(pg_cord)
                #print doc_id, page_no, data_dct['page_rect']
                #print
                break
        output_path = '/var/www/html/fundamentals_intf/output/'
        fname = os.path.join(output_path, company_id, 'doc_page_adj_cords')
        if os.path.exists(fname):
            cmd = 'rm -rf %s'%(fname)
            os.system(cmd)
        env = lmdb.open(fname, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
            for k, v in doc_page_pg_cord_dct.items():
                txn.put(k, str(v))
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
        return 
    
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

    def company_meta_info_txt_data(self):
        #f = open('bds_company_details_229_2.txt') 
        #txt_data = f.readlines()
        file_path = '/var/www/html/avinash/bds_company_details_229_2.txt'
        txt_data  = self.read_txt_from_server(file_path)
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
 
    def get_data_from_txt(self):
        comp_meta_txt_data = self.company_meta_info_txt_data()
        #f = open('bds_company_details_229_1.txt') 
        #txt_data = f.readlines()
        file_path = '/var/www/html/avinash/bds_company_details_229_1.txt'
        txt_data  = self.read_txt_from_server(file_path)
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

    def wrt_comp_meta_db(self, company_name, model_number, data_rows):
        db_path_dir = '/mnt/eMB_db/%s/%s/'%(company_name, model_number)
        if not os.path.exists(db_path_dir):
            os.system('mkdir -p %s'%(db_path_dir))
        db_path = os.path.join(db_path_dir, 'tas_company.db')
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
       
        crt_stmt = """CREATE TABLE IF NOT EXISTS company_meta_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, old_doc_name VARCHAR(256), document_type VARCHAR(256), filing_type VARCHAR(256), period VARCHAR(256), reporting_year VARCHAR(256), doc_name VARCHAR(256), doc_release_date VARCHAR(256), doc_from VARCHAR(256), doc_to VARCHAR(256), doc_download_date VARCHAR(256), doc_prev_release_date VARCHAR(256), doc_next_release_date VARCHAR(256), review_flg INTEGER, doc_id INTEGER, d_studio_map TEXT, d_studio_path TEXT)"""
        cur.execute(crt_stmt)
        conn.commit()

        delete_stmt  =  'delete from company_meta_info'
        cur.execute(delete_stmt)
        conn.commit()  
        
        cur.executemany(""" INSERT INTO company_meta_info(old_doc_name, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date, review_flg, doc_id, d_studio_map, d_studio_path) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", data_rows)
        conn.commit()
        conn.close()
        return

    def read_txt_from_server(self, file_path):
        import paramiko
        ssh_client_obj = paramiko.SSHClient()
        #ssh_client_obj.load_system_host_keys()
        ssh_client_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client_obj.connect('172.16.20.52', username='root', password='power@888')
        sftp_client = ssh_client_obj.open_sftp()
        #another_server_file = sftp_client.open('/var/www/html/avinash/bds_company_details_229_1.txt')
        another_server_file = sftp_client.open(file_path)
        txt_data = another_server_file.readlines() 
        another_server_file.close() 
        ssh_client_obj.close()
        return txt_data
    
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
            cmd = """cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py;cd - -8946 %s '{"project_id":"50","model_number":"50","project_name":"BDS  GAAP","norm_scale":"Y", "PRINT":"Y"}'"""%(toc_company_id)
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
    
       
        
    def get_ref_nms_ars(self, ar_el):   
        all_diff_arr = []
        m_lst = []
        for el_i in range(1, len(ar_el)):
            df = ar_el[el_i - 1] - ar_el[el_i]
            #print df
            if df <= 1:
                if not m_lst:
                    m_lst.extend([ar_el[el_i - 1], ar_el[el_i]])
                else:
                    m_lst.append(ar_el[el_i])
            elif df > 1:
                if m_lst:
                    all_diff_arr.append(m_lst) 
                    m_lst = [] 
        if m_lst:
            all_diff_arr.append(m_lst)
        #print m_lst
        print all_diff_arr

    def utop_tr(self, n):
        cnt = 0
        for s in range(n + 1):
            if s == 0:  
                cnt = 1
            elif s % 2 == 1:
                cnt += cnt
            elif s % 2 == 0:
                cnt += 1
        return cnt 
    
    def rt_arr(self, a, k, queries):
        from collections import deque
        sq = deque(a)
        sq.rotate(k)
        t = []
        for v in queries:
            t.append(sq[v])
        return t

    def sq_eqn(self, a_dta):
        s = []
        for l in range(1, len(a_dta)+ 1):
            f_idx = a_dta.index(l)
            s_ids = a_dta.index(f_idx + 1)
            s.append(s_ids + 1)
        return s

    def ss(self, alist):
        for i in range(0, len(alist) - 1):
            smallest = i
            for j in range(i + 1, len(alist)):
                if alist[j] < alist[smallest]:
                    smallest = j
            alist[i], alist[smallest] = alist[smallest], alist[i]
    
    def prepare_cld_op_info(self, al_dt, jp):   
        e = 100
        for rw_idx in range(0, len(al_dt), jp):
            if al_dt[rw_idx]  == 1:
                e = e - 3
            elif al_dt[rw_idx] == 0:
                e -= 1
        return e

    def fd_dg(self, n):
        s = str(n)
        cnt_div = 0 
        while s:
            try:
                ex = (n % int(s[-1]) == 0)
            except ZeroDivisionError:
                s = s[:-1]
            if ex:
                cnt_div += 1
            s = s[:-1]
        return cnt_div
        
    def get_rng_sq(self, a, b):
        import math
        cnt = 0
        for el in range(a, b+1):
            rt = math.sqrt(el)
            if int(rt + 0.5) ** 2 == el:
                cnt += 1 
        return cnt
    
    def get_rng_sq_bt(self, a, b):
        import math
        get_mn_sq = int(math.ceil(math.sqrt(a))) 
        get_mx_sq = int(math.floor(math.sqrt(b)))
        cnt = get_mx_sq - get_mn_sq + 1
        return cnt

    def nd_vis_gt(self, ar, k):
        from itertools import combinations
        res_dt_lst = []
        for r in range(2, len(ar)):
            dt = list(combinations(ar, r))
            for tp in dt:
                flg = 0
                for i in range(r-1):
                    ad = sum([tp[i], tp[i+1]])
                    if ad % k == 0:
                        flg = 1
                        break
                if not flg:
                    res_dt_lst.append(tp)
        mx_len = len(max(res_dt_lst, key=lambda x:len(x)))  
        act_lst = []
        for sr in res_dt_lst:
            if len(sr) == mx_len:
                act_lst.append(sr)
        print act_lst
        #return act_lst
    
    def get_cinfo(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'    
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """SELECT * FROM company_info""" 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        column_qry  =  """pragma table_info(company_info)"""
        cur.execute(column_qry)
        col_data = cur.fetchall()
        conn.close()
        return t_data, col_data
    
    def xl_w(self):
        c_info, col_data = self.get_cinfo()
        import xlsxwriter
        workbook = xlsxwriter.Workbook('/root/prashant/ctest.xlsx')
        ws1 = workbook.add_worksheet('company_info') 
        bold = workbook.add_format({'bold':2}) 
        itc = workbook.add_format({'italic':1}) 
        col = 0
        for idx, rw in enumerate(col_data):
            if rw[1] in ('company_name', 'company_display_name'):
                add_col_width  = ws1.set_column(0, col, 50)        
            ws1.write(0, col, rw[1], bold)
            col += 1

        row_inc = 1
        max_len_col = 0
        for row in c_info:
            col_inc = 0
            for cl_elm in row:
                ws1.write(row_inc, col_inc, cl_elm, itc) 
                col_inc += 1
            row_inc += 1 
        workbook.close()
        
    def flat_space_station(self, n, m, c_lst):
        if n == m:
            return 0
        mx = 0 
        for i in range(n):
            d_lst = []
            for k in c_lst:
                dff = abs(i-k)
                d_lst.append(dff)
            nrst = min(d_lst)
            mx = max(mx, nrst)
        return mx
    
    def tr_is(self, arr):
        for i in range(1, len(arr)):
            key = arr[i]
            j = i-1
            while j >=0 and key < arr[j]:
                arr[j+1] = arr[j]   
                j -= 1
            arr[j+1] = key
        return arr  
        
    def pt(self, number_of_rows):
        p_tri_lst = []
        for row_int in range(1, number_of_rows+1):  
            if row_int == 1:
                p_tri_lst.append([1]) 
            elif row_int == 2:
                p_tri_lst.append([1, 1])
            else:  
                last_a_lst = p_tri_lst[-1] 
                r_lst = []
                for el in range(len(last_a_lst)-1):
                    s_l = last_a_lst[el] + last_a_lst[el+1]
                    r_lst.append(s_l)
                r_lst = [last_a_lst[0]] + r_lst + [last_a_lst[-1]]
                p_tri_lst.append(r_lst)
        
        o_ln = number_of_rows + (number_of_rows-1)
        for e in p_tri_lst:
            ln = len(e)
        
if __name__ == '__main__':
    obj = Company_populate_bds()
         
    #print obj.slso([16, 10, 4, 1, 32, 21])
    #print obj.bl_st([16, 10, 4, 1, 32, 21]) 
    #print obj.tr_is([16, 10, 4, 1, 32, 21]) 
    

