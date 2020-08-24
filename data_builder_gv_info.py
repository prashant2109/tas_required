import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime,sqlite3, MySQLdb
import utils.meta_info as meta_info
import data_builder.db_data as db_data
db_dataobj  = db_data.PYAPI()

class DataBuilder(object):
    
    def __init__(self):
        self.m_obj   = meta_info.MetaInfo()
        

    def parse_ijson(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        return company_name, model_number, deal_id, project_id, company_id
     
    def read_data_builder_data(self, ijson):
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        projects, p_d    = [], {}
        ijson['project_name']   = ''
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.m_obj.get_main_table_info(company_name, model_number)

        #############################################
        """ table_type wise  collect ijson """
        j_ar    = []
        for ii, table_type in enumerate(rev_m_tables.keys()):
            if not table_type:continue
            #print 'Running ', ii, ' / ', len(rev_m_tables.keys()), [table_type, ijson['company_name']]
            ijson_c = copy.deepcopy(ijson)
            ijson_c['table_type'] = table_type
            #g_ar    = db_dataobj.read_all_vgh_groups(table_type, company_name, model_number, doc_m_d, {})
            j_ar.append(copy.deepcopy(ijson_c))

        """ table_type data builder info  loop  and collect table_xml wise data to update databuilder_gv_info"""
        
        table_info_dct = {}
        for ii, ijson_c in enumerate(j_ar):
            print 'Running ', ii, ' / ', len(j_ar), [ijson_c['table_type'], ijson.get('data')]

            res         = db_dataobj.read_db_data(ijson_c)
            if not res:continue
            #print res;sys.exit()
            if res[0]['message'] != 'done':continue
            #keytup          = (ijson_c['table_type'], ijson_c.get('grpid', ''))
            tt, grpid = ijson_c['table_type'], ijson_c.get('grpid', '')
            data    = res[0]['data']
            phs     = res[0]['phs']
            
            idx_info, widx = [], []
            for i2, data_elm in enumerate(data):
                print data_elm;sys.exit() # t_l, 't', 'x', bbox, t_id 
                for ph in phs:
                    pkey = ph['k']
                    if pkey in data_elm:
                       tx_id = data_elm['t_id']
                       val    = data_elm[pkey]['v']                   
                       xml    = data_elm[pkey]['x']
                       doc    = data_elm[pkey]['d']
                       tab    = data_elm[pkey]['t']
                       bbox   = data_elm[pkey]['bbox']
                       phcsv  = data_elm[pkey]['phcsv']
                       ph = ''.join([phcsv['pt'], phcsv['p']])
                       currency, scl, vt = phcsv['c'], phcsv['s'], phcsv['vt']
                       #print phcsv
                       #sys.exit()
                       #print (doc, tab, r, c, xml, val, ph, currency, scl, vt, str(bbx), sti, tm, un), '\n'
                       xml_hashed = hashlib.md5(xml).hexdigest()
                       widx = (tx_id, tt, i2, ph, currency, scl, vt, xml_hashed, tab, xml)   
                       widx = map(str, widx)
                       table_info_dct.setdefault(tab, []).append(widx)
        
        #print table_info_dct;sys.exit()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #for tb, info_lst in table_info_dct.iteritems():
        tids    = table_info_dct.keys()
        for i, tb in enumerate(tids):
            print 'Running ', i, '/', len(tids), [tb]
            info_lst    = table_info_dct[tb]
            update_stmt = """ UPDATE data_builder_gv_info SET taxo_id=%s, table_type=%s, order_id=%s, ph=%s, currency=%s, scale=%s, value_type=%s, xml_hashed=%s WHERE table_id=%s AND xml_id=%s   """
            #m_cur.executemany(update_stmt, info_lst)
        m_conn.commit()
        m_conn.close()
        sys.exit() #### Exit

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def read_data_builder_data_hgh(self, ijson):
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        projects, p_d    = [], {}
        ijson['project_name']   = ''
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.m_obj.get_main_table_info(company_name, model_number)

        #############################################
        """ table_type wise  collect ijson """
        j_ar    = []
        for ii, table_type in enumerate(rev_m_tables.keys()):
            if not table_type:continue
            #print 'Running ', ii, ' / ', len(rev_m_tables.keys()), [table_type, ijson['company_name']]
            ijson_c = copy.deepcopy(ijson)
            ijson_c['table_type'] = table_type
            #ijson_c['taxo_flg'] = 1
            #g_ar    = db_dataobj.read_all_vgh_groups(table_type, company_name, model_number, doc_m_d, {})
            j_ar.append(copy.deepcopy(ijson_c))

        """ table_type data builder info  loop  and collect table_xml wise data to update databuilder_gv_info"""
        
        table_info_dct = {}
        hgh_info_dct = {}
        for ii, ijson_c in enumerate(j_ar):
            print 'Running ', ii, ' / ', len(j_ar), [ijson_c['table_type'], ijson.get('data')]

            res         = db_dataobj.read_db_data(ijson_c)
            if not res:continue
            #print res;sys.exit()
            if res[0]['message'] != 'done':continue
            #keytup          = (ijson_c['table_type'], ijson_c.get('grpid', ''))
            tt, grpid = ijson_c['table_type'], ijson_c.get('grpid', '')
            data    = res[0]['data']
            phs     = res[0]['phs']
            
            idx_info, widx = [], []
            for i2, data_elm in enumerate(data):
                print data_elm #;sys.exit() # t_l, 't', 'x', bbox, t_id 
                #t_l = data_elm['t_l']
                #ht = data_elm['t']
                #hx = data_elm['x']
                #tx_id = data_elm['t_id']
                #widx = (tx_id, tt, i2, ph, currency, scl, vt, xml_hashed, tab, xml)   
                #hgh_info_dct.setdefault(ht, []).append((tx_id, tt, i2, 'Label', ))
                for ph in phs:
                    pkey = ph['k']
                    if pkey in data_elm:
                       tx_id = data_elm['t_id']
                       val    = data_elm[pkey]['v']                   
                       xml    = data_elm[pkey]['x']
                       doc    = data_elm[pkey]['d']
                       tab    = data_elm[pkey]['t']
                       bbox   = data_elm[pkey]['bbox']
                       phcsv  = data_elm[pkey]['phcsv']
                       ph = 'Label' #''.join([phcsv['pt'], phcsv['p']])
                       currency, scl, vt = phcsv['c'], phcsv['s'], phcsv['vt']
                       #print phcsv
                       #sys.exit()
                       #print (doc, tab, r, c, xml, val, ph, currency, scl, vt, str(bbx), sti, tm, un), '\n'
                       xml_hashed = hashlib.md5(xml).hexdigest()
                       widx = (tx_id, tt, i2, ph, currency, scl, vt, xml_hashed, tab, xml)   
                       widx = map(str, widx)
                       table_info_dct.setdefault(tab, []).append(widx)
        
        #print table_info_dct;sys.exit()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #for tb, info_lst in table_info_dct.iteritems():
        tids    = table_info_dct.keys()
        for i, tb in enumerate(tids):
            print 'Running ', i, '/', len(tids), [tb]
            info_lst    = table_info_dct[tb]
            update_stmt = """ UPDATE hgh_data_builder_info SET taxo_id=%s, table_type=%s, order_id=%s, ph='%s', currency=%s, scale=%s, value_type=%s, xml_hashed=%s WHERE table_id=%s AND xml_id=%s   """
            #m_cur.executemany(update_stmt, info_lst)
        m_conn.commit()
        m_conn.close()
        sys.exit() #### Exit
       
    def construct_db(self, ijson):
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        table_type = ijson['table_type']
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry   =  """  SELECT row_id, taxo_id, order_id, value, ph, table_id, col, doc_id FROM data_builder_gv_info WHERE table_type='%s' """%(table_type) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        data_dct = {}
        phs_dct = {}
        tx_order = {}
        for row in t_data:
            row_id, taxo_id, order_id, value, ph, table_id, col, doc_id = row
            pk = '%s-%s-'%(table_id, col)
            data_dct.setdefault(taxo_id, {})[pk] = {'v':value, 'rid':row_id}
            phs_dct.setdefault((doc_id, table_id), {}).setdefault(pk, {})[ph] = phd_dct.get(pk, {}).get(ph, 0)+1
            tx_order[taxo_id] = order_id
        tids = tx_order.keys()
        tids.sort(key=lambda x:tx_order[x])
        res_lst = []
        for tid in tids:
            dt = data_dct[tid]
            dt['t_id'] = tid
            res_lst.append(dt)
            
        phs_arr = []
        for doc_tab, info in phs_dct.iteritems():
            for k, vl in info.iteritems():
                phs = vl.keys()
                phs.sort(key=lambda x:vl[x])
                dst = {'k':k, 'n':phs[-1], 'g':'%s-%s'%(doc_tab)}
                phs_arr.append(dst)
        res = [{'message':'done', 'data':res_lst, 'phs':phs_arr}] 
        return res
    
    def read_hgh_from_taxo_builder(self, company_name, model_number):
        db_path = '/mnt/eMB_db/%s/%s/taxo_data_builder.db'%(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, user_label FROM mt_data_builder  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        res_rows = []
        for row in t_data:  
            tup_lst = []
            for el in row:
                if not el:
                    el = ''
                if isinstance(el, unicode):
                    el = el.encode('utf-8')
                tup_lst.append(el)
            res_rows.append(tuple(tup_lst))
        #sys.exit()
        return res_rows 
    
    def update_hgh_server_info(self, ijson):   
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        insert_rows = self.read_hgh_from_taxo_builder(company_name, model_number)
        #print insert_rows; sys.exit()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        crt_qry = """ CREATE TABLE IF NOT EXISTS hgh_data_builder_info(row_id INTEGER NOT NULL AUTO_INCREMENT, taxo_id VARCHAR(50) DEFAULT NULL, prev_id VARCHAR(50) DEFAULT NULL, order_id VARCHAR(50) DEFAULT NULL, table_type VARCHAR(255) DEFAULT NULL, taxonomy TEXT DEFAULT NULL, user_taxonomy TEXT DEFAULT NULL, missing_taxo varchar(1) DEFAULT NULL, table_id VARCHAR(50) DEFAULT NULL, c_id VARCHAR(100) DEFAULT NULL, gcom VARCHAR(1024) DEFAULT NULL, ngcom VARCHAR(1024) DEFAULT NULL, ph VARCHAR(20) DEFAULT NULL, ph_label TEXT DEFAULT NULL, user_name VARCHAR(50) DEFAULT NULL, datetime VARCHAR(256) DEFAULT NULL, isvisible varchar(1) DEFAULT NULL, m_rows VARCHAR(50) DEFAULT NULL, vgh_text TEXT DEFAULT NULL, vgh_group TEXT DEFAULT NULL, doc_id VARCHAR(50) DEFAULT NULL, xml_id TEXT DEFAULT NULL, period VARCHAR(50) DEFAULT NULL, period_type VARCHAR(50) DEFAULT NULL, scale VARCHAR(50) DEFAULT NULL, currency VARCHAR(50) DEFAULT NULL, value_type VARCHAR(50) DEFAULT NULL, user_label TEXT DEFAULT NULL, PRIMARY KEY (row_id)) """  
        m_cur.execute(crt_qry)
        
        insert_stmt = """ INSERT INTO hgh_data_builder_info(taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, user_label) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        m_cur.executemany(insert_stmt, insert_rows)
        m_conn.commit()
        m_conn.close()
        return



if __name__ == '__main__':
    dObj =  DataBuilder()
    
