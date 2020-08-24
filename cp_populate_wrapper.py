import os, sys, MySQLdb, copy, sqlite3, lmdb, sets
import mysql.connector
def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class Wrapper(object):
        
    def __init__(self):
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

    def doc_populate_auto_inc_1911(self, vijson):
        get_deep_cpy =  vijson.get('popul',{})
        ijson = copy.deepcopy(vijson)
        del ijson['popul']
        for doc_id,table_ids in  get_deep_cpy.items():
            ijson['doc_ids'] = [doc_id]
            ijson['table_ids']= table_ids
            company_display_name = ijson['company_name']
            project_id           = ijson['project_id']
            # BOC Hong Kong (Holdings) Limited
            #company_name_map = {"BOC Hong Kong (Holdings) Limited":"BankOfChina"}
            #if company_display_name in company_name_map:
            #    company_display_name = company_name_map[company_display_name]
            c_name = ''
            for elm in company_display_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            for docs in ijson["doc_ids"]:
                dcid = str(docs)
                import add_meta_to_txt as py
                obj = py.Company_populate_new()
                company_name_deal_map = obj.write_4433_company_info({company_name:company_display_name}, project_id, dcid) 
                deal_id = company_name_deal_map[company_name]
                ijson_c = copy.deepcopy(ijson)
                ijson_c['company_name'] = company_name
                ijson_c['deal_id'] = deal_id
                ijson_c['model_number'] = project_id
                #print ijson_c
                #sys.exit('Termination')
                self.insert_populate_info_cgi_lang(ijson_c)  
                # make directory 
                if 1:
                    path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/'%(company_name, project_id)
                    if not os.path.exists(path):
                        cmd_dir = os.system('mkdir -p %s'%(path))

                if 1:
                    path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, project_id)
                    conn = sqlite3.connect(path)
                    cur  = conn.cursor()
                    crt_qry = 'CREATE TABLE IF NOT EXISTS table_group_mapping(row_id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id INTEGER, doc_id VARCHAR(256), doc_name VARCHAR(250), table_id VARCHAR(250), parent_table_type TEXT)'
                    cur.execute(crt_qry)
                    conn.close()
                if 1:
                    pt = '/var/www/html/fill_table/%s_%s/table_info'%(project_id, deal_id)
                    if not os.path.exists(pt):
                        os.system('mkdir -p %s'%(pt))
        return [{'message':'done'}]

    def insert_populate_info_cgi_lang_table_doc(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        #doc_id          = '#'.join(map(str, ijson['doc_ids']))
        #table_id        = '#'.join(map(str, ijson['table_ids']))
        dc_info         = ijson['popul']
        from_auto_inc   = ijson.get('from_auto_inc', 'N')
        user_name       = ijson['user']
        lang_flg        = int(ijson.get('lang_flg', 0))
        if lang_flg:
            get_lang = ijson['lang']
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
    
        for doc_id, table_id in dc_info.iteritems():
            doc_id = str(doc_id)
            table_id = '#'.join(map(str, table_id))
            insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time, user_name, from_inc) VALUES('%s', '%s', '%s', 'N', Now(), '%s', '%s')"""%(company_id, doc_id, table_id, user_name, from_auto_inc)
            if lang_flg:
                insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time, user_name, language_info, from_inc) VALUES('%s', '%s', '%s', 'N', Now(), '%s', '%s', '%s')"""%(company_id, doc_id, table_id, user_name, get_lang, from_auto_inc)
            m_cur.execute(insert_stmt)
        m_conn.commit()

        read_qry = """SELECT status  FROM populate_status where company_id='%s' """%(company_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if not t_data:
            insert_stmt = """ INSERT INTO populate_status(company_id, status, queue_status, process_time, user_name) VALUES('%s', '%s', '%s', Now(), '%s')"""%(company_id, 'N', 'N', user_name)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data not in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET queue_status='Q', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET status='N', queue_status='N', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def get_company_name_MID(self, project_id, deal_id):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        qry = """SELECT company_name FROM company_info WHERE project_id='%s' AND toc_company_id='%s'  """%(project_id, deal_id)
        cur.execute(qry)
        company_name = str(cur.fetchone()[0])
        conn.close()
        return company_name

    
    def doc_table_populate_auto_inc(self, ijson):
        company_display_name = ijson['company_name']
        project_id           = ijson['project_id']
        doc_info             = ijson['popul']
        if not ijson.get('Old_deal_id'):
            c_name = ''
            for elm in company_display_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            dcid = str(next(iter(doc_info)))
            import chk_add_meta_to_txt as py
            obj = py.Company_populate_new()
            company_name_deal_map = obj.write_4433_company_info({company_name:company_display_name}, project_id, dcid) 
            deal_id = company_name_deal_map[company_name]
        else:
            cid         = ijson['Old_deal_id']
            project_id, deal_id = cid.split('_')
            ijson_c['project_id'] = project_id
            company_name    = self.get_company_name_MID(project_id, deal_id) 
        #print deal_id
        #sys.exit()
        ijson_c = copy.deepcopy(ijson)
        ijson_c['company_name'] = company_name
        ijson_c['deal_id'] = deal_id
        ijson_c['model_number'] = project_id

        self.insert_populate_info_cgi_lang_table_doc(ijson_c)

        if 1:
            path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/'%(company_name, project_id)
            if not os.path.exists(path):
                cmd_dir = os.system('mkdir -p %s'%(path))

        if 1:
            path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, project_id)
            conn = sqlite3.connect(path)
            cur  = conn.cursor()
            crt_qry = 'CREATE TABLE IF NOT EXISTS table_group_mapping(row_id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id INTEGER, doc_id VARCHAR(256), doc_name VARCHAR(250), table_id VARCHAR(250), parent_table_type TEXT)'
            cur.execute(crt_qry)
            conn.close()
        if 1:
            pt = '/var/www/html/fill_table/%s_%s/table_info'%(project_id, deal_id)
            if not os.path.exists(pt):
                os.system('mkdir -p %s'%(pt))
        if 1:
            path = '/mnt/eMB_db/%s/%s/table_phcsv_data'%(company_name, project_id)
            if not os.path.exists(path):
                cmd_dir = os.system('mkdir -p %s'%(path))
                env = lmdb.open(path)
                txn = env.begin(write=True)
        return [{'message':'done','sdvad':1}]
                

    def doc_populate_auto_inc(self, ijson):
        disableprint()
        company_display_name = ijson['company_name']
        project_id           = ijson['project_id']
        # BOC Hong Kong (Holdings) Limited
        #company_name_map = {"BOC Hong Kong (Holdings) Limited":"BankOfChina"}
        #if company_display_name in company_name_map:
        #    company_display_name = company_name_map[company_display_name]
        if not ijson.get('Old_deal_id'):
            c_name = ''
            for elm in company_display_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            dcid = str(ijson["doc_ids"][0])
            import chk_add_meta_to_txt as py
            obj = py.Company_populate_new()
            company_name_deal_map = obj.write_4433_company_info({company_name:company_display_name}, project_id, dcid) 
            deal_id = company_name_deal_map[company_name]
        else:
            cid         = ijson['Old_deal_id']
            project_id, deal_id = cid.split('_')
            company_name    = self.get_company_name_MID(project_id, deal_id) 
        ijson_c = copy.deepcopy(ijson)
        ijson_c['project_id'] = project_id
        ijson_c['company_name'] = company_name
        ijson_c['deal_id'] = deal_id
        ijson_c['model_number'] = project_id
        #print ijson_c
        #sys.exit('Termination')
        self.insert_populate_info_cgi_lang(ijson_c)  
        # make directory 
        if 1:
            path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/'%(company_name, project_id)
            if not os.path.exists(path):
                cmd_dir = os.system('mkdir -p %s'%(path))

        if 1:
            path = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, project_id)
            conn = sqlite3.connect(path)
            cur  = conn.cursor()
            crt_qry = 'CREATE TABLE IF NOT EXISTS table_group_mapping(row_id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id INTEGER, doc_id VARCHAR(256), doc_name VARCHAR(250), table_id VARCHAR(250), parent_table_type TEXT)'
            cur.execute(crt_qry)
            conn.close()
        if 1:
            pt = '/var/www/html/fill_table/%s_%s/table_info'%(project_id, deal_id)
            if not os.path.exists(pt):
                os.system('mkdir -p %s'%(pt))
        if 1:
            path = '/mnt/eMB_db/%s/%s/table_phcsv_data'%(company_name, project_id)
            if not os.path.exists(path):
                cmd_dir = os.system('mkdir -p %s'%(path))
                env = lmdb.open(path)
                txn = env.begin(write=True)
        enableprint()
        return [{'message':'done','sdvad':1}]
    
    def insert_populate_info(self, company_id, doc_id, table_id):
        #db_name = 'populate_info'
        #try:
        #    self.create_databases_mysql(db_name)
        #    print ['>>>>', db_name]
        #except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #crt_qry = """CREATE TABLE IF NOT EXISTS company_doc_table_info(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT, doc_id TEXT, table_id TEXT, language_info TEXT, PRIMARY KEY (row_id));"""
        #m_cur.execute(crt_qry)
            
        #crt_qry = """CREATE TABLE IF NOT EXISTS populate_status(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT,  status TEXT, source_row_id TEXT, PRIMARY KEY (row_id))""" 
        #m_cur.execute(crt_qry)

        insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time) VALUES('%s', '%s', '%s', 'N', Now())"""%(company_id, doc_id, table_id)
        print insert_stmt
        m_cur.execute(insert_stmt)
        
        read_qry = """SELECT status  FROM populate_status where company_id='%s' """%(company_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if not t_data:
            insert_stmt = """ INSERT INTO populate_status(company_id, status, queue_status, process_time, user_name) VALUES('%s', '%s', '%s', Now(), 'prashant')"""%(company_id, 'N', 'N')
            m_cur.execute(insert_stmt)
        elif t_data and (t_data not in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET queue_status='Q', process_time=Now(), user_name='prashant' WHERE company_id='%s' """%(company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET status='N', queue_status='N', process_time=Now(), user_name='prashant' WHERE company_id='%s' """%(company_id)
            m_cur.execute(insert_stmt)
        
        m_conn.commit()
        return 'done'

    def insert_populate_info_cgi(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        doc_id          = '#'.join(map(str, ijson['doc_ids']))
        table_id        = '#'.join(map(str, ijson['table_ids']))
        user_name       = ijson['user']
        #db_name = 'populate_info'
        #try:
        #    self.create_databases_mysql(db_name)
        #    print ['>>>>', db_name]
        #except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #crt_qry = """CREATE TABLE IF NOT EXISTS company_doc_table_info(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT, doc_id TEXT, table_id TEXT, language_info TEXT, PRIMARY KEY (row_id));"""
        #m_cur.execute(crt_qry)
            
        #crt_qry = """CREATE TABLE IF NOT EXISTS populate_status(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT,  status TEXT, source_row_id TEXT, PRIMARY KEY (row_id))""" 
        #m_cur.execute(crt_qry)

        insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time) VALUES('%s', '%s', '%s', 'N', Now())"""%(company_id, doc_id, table_id)
        #print insert_stmt
        m_cur.execute(insert_stmt)
        
        read_qry = """SELECT status  FROM populate_status where company_id='%s' """%(company_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if not t_data:
            insert_stmt = """ INSERT INTO populate_status(company_id, status, queue_status, process_time, user_name) VALUES('%s', '%s', '%s', Now(), '%s')"""%(company_id, 'N', 'N', user_name)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data not in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET queue_status='Q', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET status='N', queue_status='N', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        
        m_conn.commit()
        return [{'message':'done'}]

    def insert_populate_info_cgi_lang(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        doc_id          = '#'.join(map(str, ijson['doc_ids']))
        table_id        = '#'.join(map(str, ijson['table_ids']))
        from_auto_inc   = ijson.get('from_auto_inc', 'N')
        user_name       = ijson['user']
        lang_flg        = int(ijson.get('lang_flg', 0))
        if lang_flg:
            get_lang = ijson['lang']
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
    
        
        insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time, user_name, from_inc) VALUES('%s', '%s', '%s', 'N', Now(), '%s', '%s')"""%(company_id, doc_id, table_id, user_name, from_auto_inc)
        if lang_flg:
            insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time, user_name, language_info, from_inc) VALUES('%s', '%s', '%s', 'N', Now(), '%s', '%s', '%s')"""%(company_id, doc_id, table_id, user_name, get_lang, from_auto_inc)
        m_cur.execute(insert_stmt)
        
        read_qry = """SELECT status  FROM populate_status where company_id='%s' """%(company_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if not t_data:
            insert_stmt = """ INSERT INTO populate_status(company_id, status, queue_status, process_time, user_name) VALUES('%s', '%s', '%s', Now(), '%s')"""%(company_id, 'N', 'N', user_name)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data not in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET queue_status='Q', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET status='N', queue_status='N', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        
        m_conn.commit()
        return [{'message':'done'}]

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
    
    def prepare_doc_page_tup(self, doc_info):
        inc_doc_dct = {}
        doc_lst = []
        for doc, pg_grd_lst in doc_info.iteritems():
            doc = str(doc)
            doc_lst.append(doc)
            for pg_grd in pg_grd_lst:
                pg, grd = pg_grd.split('-')
                inc_doc_dct.setdefault(doc, {}).setdefault(pg, {})[grd] = 1
        return inc_doc_dct, doc_lst 
    
    def grid_wise_data_preparation(self, company_name, doc_ids, c_id, i_doc_d={}):
        dbname = 'AECN_INC'
        import view.TestRead.chunk_error_cls as chunk_error_cls 
        chkobj = chunk_error_cls.chunk_error_cls('/root/tas_processing_code/WorkSpaceBuilder_DB/pysrc/dbConfig.ini')
        import view.data_builder as data_builder
        obj = data_builder.view('/root/tas_processing_code/WorkSpaceBuilder_DB/pysrc/dbConfig.ini') 
        data  = obj.get_doc_id_meta_data_doc_wise_dct_20(dbname, doc_ids) 
        
        project_id = '34'
        doc_page_grid_dict = {}
        doc_phlst   = []
        company_meta_data_dct = 1
        doc_id_info = {}
        for data_elm in  data:
            if i_doc_d and str(data_elm[0]) not in i_doc_d:continue
            doc_phlst.append((data_elm[0], '%s%s'%(data_elm[1]['periodtype'], data_elm[1]['Year']), data_elm[5]+'.pdf'))
            if not company_meta_data_dct:
                pass 
                #meta_info = data_elm[1]
                #self.write_company_meta_info_txt(company_name, '20', meta_info)  
                #company_meta_data_dct = 1
                #sys.exit()
            meta_info = data_elm[1]
            doc_typ = meta_info.get("DocType", "")
            dc_frm = meta_info.get("Document From", "")
            if not dc_frm:
                dc_frm = meta_info.get("Document From (mm//dd/yyyy)", "") 
            if dc_frm:
                try:
                    mnth, dt, yr = dc_frm.split('/')
                    dc_frm = '-'.join([dt, mnth, yr]) 
                except: dc_frm = dc_frm[:]
            ft = meta_info.get("FilingType", "")
            #if dc_frm:
            #    mnth, dt, yr = dc_frm.split('/')
            #    dc_frm = '-'.join([dt, mnth, yr])
            dc_to = meta_info.get("Document To", "")
            if not dc_to:
                dc_to = meta_info.get("Document To (mm//dd/yyyy)", "")
            if dc_to:
                try:
                    mnth, dt, yr = dc_to.split('/')
                    dc_to = '-'.join([dt, mnth, yr])
                except:dc_to =  dc_to[:] 
                    
            ddd = meta_info.get("Document Download Date", '')
            if not ddd:
                ddd = meta_info.get("Document Download Date  (mm//dd/yyyy)", '') 
            if ddd:
                try:
                    mnth, dt, yr = ddd.split('/')
                    ddd = '-'.join([dt, mnth, yr])
                except:ddd = ddd[:]
            dpd = meta_info.get("PreviousReleaseDate", '') 
            if dpd:
                try:
                    mnth, dt, yr = dpd.split('/')
                    dpd = '-'.join([dt, mnth, yr])
                except:dpd = dpd[:]
            drd = meta_info.get("Document Release Date", '')
            if drd:
                try:
                    mnth, dt, yr = drd.split('/')
                    drd = '-'.join([dt, mnth, yr])
                except:drd = drd[:]
            nrd = meta_info.get("NextReleaseDate", '')
            if nrd:
                try:
                    mnth, dt, yr = nrd.split('/')
                    nrd = '-'.join([dt, mnth, yr])
                except:nrd = nrd[:]
            dc_meta_dt = {
                            "ddd":ddd,
                            "dpd" :dpd,
                            "df":dc_frm,
                            "dt":dc_to,
                            "drd":drd,
                            "nrd":nrd,
                            "dc_typ":doc_typ,
                            "ft":ft
                        }
            doc_id_info[str(data_elm[0])] = dc_meta_dt
            phcsv_data = data_elm[3]
            #print phcsv_data
            kdata = data_elm[2]
            ws_id = '1'
            doc_ppath = chkobj.base_path + str(project_id) + '/' + str(ws_id) + '/pdata/docs/'+str(data_elm[0])
            if 1: 
             for page_no in kdata.keys():
                if i_doc_d.get(str(data_elm[0])) and str(page_no) not in i_doc_d[str(data_elm[0])]:continue
                #if str(page_no) not in ['25']: continue
                #print ' data_elm: ', data_elm[3][page_no][1], page_no 
                #print type(page_no)  
                #if str(page_no) != '31': continue
                #if str(page_no) != '26': continue
                cell_info = [str(page_no), doc_ppath+'/CID/',    1, 1] # page_no, cid_path, isdb, isenc
                cell_dict = chkobj.get_cell_dict(cell_info[0], cell_info[1], cell_info[2], cell_info[3])
    

                gids = kdata[page_no].keys()
                for gid in gids:
                    #print '>>>>>>>>>>>>>>>>>', (data_elm[0], page_no, gid) 
                     
                    #if str(gid) != '2': continue
                    #if str(gid) != '5': continue
                    if i_doc_d.get(str(data_elm[0]), {}).get(str(page_no), {}) and str(gid) not in i_doc_d[str(data_elm[0])][str(page_no)]:continue
                    if gid > 1000: continue
                    rcount = 10000
                    xml_d   = phcsv_data.get(int(page_no), {}).get(int(gid), {})
                    if 'data' not in kdata[page_no][gid]:
                        #print '==================================='
                        #print data_elm[0], page_no, gid
                        #print 
                        continue
                    ddict =  kdata[page_no][gid]['data']
                    #print ddict
                    #sys.exit()
                    #print 'ddict: ', ddict 
                    mykeys_dict = { 'mdata':[], 'text_lst':[], 'rowspan':1, 'font_prop_lst':[], 'attr':[], 'colspan':1, 'section_type':'', 'text_ids':[], 'isbold':[], 'bbox_lst':[], 'level_info':-1 }
                    # SUNIL PHCSV INFO  { valref: ( ph, csv) }
                    gid_dict = {}
                    r_cs = ddict.keys()
                    r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
                    for r_c in r_cs:
                        #print ddict[r_c]
                        row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
                        gid_dict[(row, col)] = copy.deepcopy(mykeys_dict)
                        gid_dict[(row, col)]['text_lst'] =  [ ddict[r_c]['data'] ]
                        #print ((row, col), ddict[r_c]['data'], ddict[r_c]['ldr'], ddict[r_c].get('colspan', ''), ddict[r_c].get('rowspan', ''))
                        try: 
                            gid_dict[(row, col)]['rowspan'] =  ddict[r_c]['rowspan'] 
                        except:
                            gid_dict[(row, col)]['rowspan'] =  '1'
                        try:
                             gid_dict[(row, col)]['colspan'] =  ddict[r_c]['colspan'] 
                        except:
                             gid_dict[(row, col)]['colspan'] =  '1'
                        bl = []
                        bbox_lst = ddict[r_c]['bbox'].split('$$') 
                        for bx in bbox_lst:#xmin_ymin_xmax_ymax
                            if bx:
                                b = map(lambda x:int(x), bx.split('_'))
                                x   = b[0]
                                y   = b[1]
                                w   = b[2] - b[0]
                                h   = b[3] - b[1]
                                bl.append([x, y, w, h])
                         
                        bbx = [bl, page_no]
                        if not bl: 
                            bbx = []
                        gid_dict[(row, col)]['bbox_lst']    = bbx
                        chref   = ddict[r_c].get('chref', '')
                        if chref:
                            gid_dict[(row, col)]['text_ids'] =  map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), ddict[r_c]['xml_ids'].split('$$')[:]))
                        else:
                            gid_dict[(row, col)]['text_ids'] =  filter(lambda x:x.strip(), ddict[r_c]['xml_ids'].split('$$')[:])
                        gid_dict[(row, col)]['ph']    = ''
                        gid_dict[(row, col)]['scale']    = ''
                        gid_dict[(row, col)]['currency']    = ''
                        gid_dict[(row, col)]['value_type']    = ''

                        phcsv_d = {}
                        #print r_c, gid_dict[(row, col)]['text_ids'], gid_dict[(row, col)]['text_lst'], ddict[r_c]['ldr']
                        for xid in [ddict[r_c]['xml_ids']]:
                            if xid in xml_d:
                                phinfo, csvinfo = xml_d[xid][0], xml_d[xid][1]
                                #print '\t', xid, phinfo, csvinfo
                                if phinfo.get('Year') and phinfo.get('Period_Type'):
                                    if type(phinfo['Period_Type']) != type(''):
                                        sd = ''
                                        #print phinfo['Period_Type'], xid,  data_elm[0], page_no, gid
                                    gid_dict[(row, col)]['ph']   = str(phinfo['Period_Type'])+str(phinfo['Year'])
                                if csvinfo.get('Currency'):
                                    gid_dict[(row, col)]['currency']    = csvinfo['Currency']
                                if csvinfo.get('Scale'):
                                    gid_dict[(row, col)]['scale']    = csvinfo['Scale']
                                if csvinfo.get('ValueType'):
                                    gid_dict[(row, col)]['value_type']    = csvinfo['ValueType']
                        stype = ddict[r_c]['ldr']
                        #print stype
                        #if stype == []:continue
                        if stype == 'value': 
                           gid_dict[(row, col)]['section_type'] =  'GV'
                        elif stype == 'hch': 
                           gid_dict[(row, col)]['section_type'] =  'HGH'
                        elif stype == 'vch': 
                           gid_dict[(row, col)]['section_type'] =  'VGH'
                        elif stype in ['gh', 'g_header']: 
                           gid_dict[(row, col)]['section_type'] =  'GH'
                        elif stype in ['undefined']:
                           gid_dict[(row, col)]['section_type'] =  ''
                        else:
                           gid_dict[(row, col)]['section_type'] =  ''
                           if 0:#stype.strip():
                              print ' -- ', stype
                              sys.exit()  
                              #gid_dict[(row, col)]['section_type'] =  'GV'
                    if phcsv_data.get(int(page_no), {}).get(int(gid), {}):
                       pass
                       #print (batch, data_elm[0], page_no, gid)
                       #print ' PH Present'
                       #print phcsv_data.get(int(page_no), {}).get(int(gid), {})['x56_1']
                       #sys.exit()     
                    #print "HHHH"
                    #print '*'*100
                    #print gid_dict   
                    #sys.exit()
                    if 0:
                        r_cs = gid_dict.keys()
                        for r_c in r_cs[:0]:
                            r_c_dict = gid_dict[r_c]
                            ref_ars =  r_c_dict.get('text_ids', []) 
                            mj = ''.join(ref_ars).strip()
                            if not mj:
                               new_red = 'x'+str(rcount)+'_'+str(page_no)
                               gid_dict[r_c]['text_ids'] = [ new_red ]
                               rcount = rcount + 1
                    doc_page_grid_dict[(data_elm[0], page_no, gid)] = ('', '', gid_dict, data_elm[4]) 
        #print doc_phlst;sys.exit()
        return doc_page_grid_dict, dbname, doc_phlst 

    def alter_mysql_tables(self, m_cur, m_conn, table_name, arr_columns):
        for col in arr_columns:
            stmt = """ALTER TABLE %s ADD COLUMN %s TEXT"""%(table_name, col)
            try:
                m_cur.execute(stmt)
                m_conn.commit()
            except:pass
        return 'done'

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
            table_info_map_table[(source_table_info, source_type)] = norm_resid
        try:
            get_max_table = max(map(int, table_info_map_table.values())) + 1
        except:
            get_max_table = 1
        return table_info_map_table, get_max_table

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
            doc_info_map_docid[(sdi, src_typ)] = doc
        try:
            get_max_doc = max(map(int, doc_info_map_docid.values())) + 1
        except:get_max_doc = 1
        return doc_info_map_docid, get_max_doc
    
    def table_doc_info_insert(self, company_name, model_number, project_id, deal_id, company_id, html_cell_data_dict, s_type, dc_ph_dct):
        crt_db_name = 'tfms_urlid_%s'%(company_id)
        try:
            self.create_databases_mysql(crt_db_name)
            print ['>>>>', crt_db_name]
        except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', crt_db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)

        crt2_stmt = """CREATE TABLE IF NOT EXISTS data_mgmt(resid BIGINT(20), training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), pageno INT(11), taxoid INT(11), taxoname TEXT, doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), data0 TEXT, data1 MEDIUMTEXT, process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, training_tableid INT(11), training_group_id INT(11), applicator_key TEXT, reviewed_user TEXT)"""
        m_cur.execute(crt2_stmt)
        m_conn.commit()

        pt = '/mnt/eMB_db/%s/%s/table_computation_dbs/'%(company_name, model_number)
        if not os.path.exists(pt):
            os.system('mkdir -p %s'%(pt))

        from_doc_map, max_doc    = self.get_existing_doc_info(company_id) 
        from_table_map, max_table    = self.get_existing_table_info(company_id)
        #print max_doc, from_doc_map, '\n'
        #print max_table, from_table_map, '\n';sys.exit()
        i_doc_ar    = []
        i_table_ar  = []
        i_table_ar2  = []
        doc_ids_for_meta = []

        populate_doc_table_info_dct = {}    
        batch = ''
        for doc_page_tup, html_cell_dct in html_cell_data_dict.iteritems():
            doc_id, page_no, table_id = map(str, doc_page_tup)
            grd_id = copy.deepcopy(table_id)
            source_table_info = str((doc_id, page_no, table_id))
            source_doc_info  = copy.deepcopy(doc_id)
            table_type, table_html_str, table_cell_data, batch = html_cell_dct
            source_type = copy.deepcopy(s_type)
            if batch:
                source_type = s_type +'^'+batch
            d_n, ph = dc_ph_dct[doc_id]
            if (source_doc_info, source_type) not in from_doc_map:
                #new_docid  = create new doc docid/match exists
                new_docid = copy.deepcopy(doc_id) #max_doc #self.get_new_doc_id(company_id, source_doc_info, source_type, d_n) 
                i_doc_ar.append((new_docid, project_id, deal_id, d_n, 'Y', source_doc_info, source_type, deal_id, 21, ''))
                print (new_docid, project_id, deal_id, d_n, 'Y', source_doc_info, source_type)
                max_doc += 1
                from_doc_map[(source_doc_info, source_type)]   = new_docid
            doc_id  = from_doc_map[(source_doc_info, source_type)]
            if (doc_id, ph, d_n) not in doc_ids_for_meta:
                doc_ids_for_meta.append((doc_id, ph, d_n))
            if (source_table_info, source_type)  not in from_table_map:
                new_table = max_table #self.get_new_table_id(company_id, source_table_info, source_type, doc_id)
                max_table   += 1
                i_table_ar.append((new_table, project_id, deal_id, doc_id, new_table, page_no,  'Y', 'Y', 'Y', source_table_info, source_type, new_table, new_table, deal_id, 21, 1, ''))
                i_table_ar2.append((new_table, project_id, deal_id, doc_id, page_no, 'Y', 'Y', 'Grid', deal_id, 21, 1, '', '0', new_table, new_table))
                from_table_map[(source_table_info, source_type)]   = new_table
            table_id = from_table_map[(source_table_info, source_type)]
            populate_doc_table_info_dct[str(table_id)] = (str(doc_id), table_cell_data) #[(doc_id, page_no, grd_id)] = (doc_id, table_id, table_cell_data)
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
        m_conn.close() 
        return populate_doc_table_info_dct
        
    def make_rc_wise_data(self, table_id, doc_id, cell_data, project_id, deal_id):
        doc_path = ''
        docph   = ''
        if doc_id:
            #path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
            path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/doc_map.txt'%(project_id, deal_id)
            if os.path.exists(path):
                fin = open(path, 'r')
                lines   = fin.readlines()
                fin.close()
            else:
                lines   = []
            doc_d       = {}
            dphs        = {}

            c_year      = self.get_cyear(lines)
            start_year  = c_year - int(ijson.get('year', 5))
            for line in lines[1:]:
                line    = line.split('\t')
                if len(line) < 8:continue
                tdoc_id  = line[0]
                if tdoc_id   != str(doc_id):continue
                line    = map(lambda x:x.strip(), line)
                ph      = line[3]+line[7]
                try:
                    year    = int(ph[-4:])
                except:continue
                docph   = doc_id+'-'+table_id+' ( '+'_'.join([ph, line[2]])+' )'
        rc_d = {}
        hrc_d = {}
        col_d = {}
        for r_c, ddict in  cell_data.items():  
            r, c = r_c
            xml_id =  '#'.join(ddict['text_ids'])
            txt = ' '.join(ddict['text_lst'])
            bbox = ddict['bbox_lst']
            if ddict['section_type'] == 'GV':
                ph_date = ddict.get('ph', '')
                if ph_date:
                        period_type = ph_date[:-4].split('.')[-1]
                else:
                    period_type = ''
                period = ph_date[-4:]
                currency = ddict.get('currency', '')
                if currency:
                    value_type = 'MNUM'
                value_type = ddict.get('value_type', '')
                scale    = ddict.get('scale', '')
                col_d[c]   = '%s%s'%(period_type, period)
                rc_d.setdefault(r, {})[table_id+'-'+str(c)+'-']   = {'v':txt, 'x':xml_id, 'bbox':bbox, 'phcsv':{'p':period, 'pt':period_type, 's':scale, 'c':currency, 'vt':value_type}, 't':table_id, 'd':doc_id}
                rc_d[r]['da']  = 'Y'
            elif ddict['section_type'] == 'HGH':
                hrc_d.setdefault(r, []).append((c, txt, xml_id, bbox))     
        rc  = list(sets.Set(rc_d.keys()+hrc_d.keys()))
        rc.sort()
        data    = []
        for r in rc:
            cols    = hrc_d.get(r, {})
            cols.sort()
            txt = []
            xml = []
            bbox    = []
            for tr in cols:
                txt.append(tr[1])
                xml.append(tr[2])
                bbox    += tr[3]
            dd  = {'t_id':r, 't_l':' '.join(txt), 'x':':@:'.join(xml), 'bbox':bbox, 't':table_id,'d':doc_id}
            dd.update(rc_d.get(r, {}))
            data.append(dd)
        cols    = col_d.keys()
        cols.sort()
        phs = []
        for c in cols:
            dd  = {'n':col_d[c], 'k':'%s-%s-'%(table_id, c), 'g':docph}
            phs.append(dd)
        res = [{'message':'done', 'phs':phs, 'data':data}]
        return res
            
    def get_validate_info(self, ijson, given_table_dct):
        import chk_review_ph_tally_table as pyf
        v_Obj = pyf.Validate() 
        v_Obj.validate(ijson, given_table_dct)
        return 
    
    def formula_populate(self, ijson):
        #sys.exit('MT')
        if ijson.get("PRINT", "N") != "Y":
            disableprint()
        company_display_name = ijson['company_name']
        project_id           = ijson['project_id']
        doc_info             = ijson['doc_info']
        c_name = ''
        for elm in company_display_name:
            if elm.isalnum():
                c_name += elm
        company_name = c_name[:]
        
        company_id = self.get_company_id_pass_company_name(project_id)[company_name]
        #print company_id, company_name, company_display_name;sys.exit()
        deal_id = company_id.split('_')[1]
        ijson_c  = copy.deepcopy(ijson)            
        ijson_c['company_name'] = company_name
        ijson_c['model_number'] = project_id
        ijson_c['project_id']   = project_id
        ijson_c['deal_id']      = deal_id 
        #print ijson_c;sys.exit()
        formula_path = '/mnt/eMB_db/%s/%s/table_computation_dbs/'%(company_name, project_id)
        if not os.path.exists(formula_path):
            os.system('mkdir -p %s'%(formula_path))
        doc_table_dct, doc_lst = self.prepare_doc_page_tup(doc_info)
        grid_wise_cell_data, s_type, doc_ph_lst = self.grid_wise_data_preparation(company_name, doc_lst, company_id, doc_table_dct)
        #print grid_wise_cell_data, s_type;sys.exit()
        dc_ph_dct = self.doc_ph_lst_dct(doc_ph_lst)
        #print dc_ph_dct;sys.exit() 
        model_number = project_id
        populate_doc_table_info_dct  = self.table_doc_info_insert(company_name, model_number, project_id, deal_id, company_id, grid_wise_cell_data, s_type, dc_ph_dct) 
        #print populate_doc_table_info_dct;sys.exit() 
        self.get_validate_info(ijson_c, populate_doc_table_info_dct)  
        res = [{"message":'done'}]
        return res

if __name__ == '__main__':
        
    obj = Wrapper() 
    company_id = sys.argv[1]
    doc_ids    = sys.argv[2]
    table_ids  = sys.argv[3]
    #obj.insert_populate_info(company_id, doc_ids, table_ids)
