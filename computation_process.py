import os, sys
import httplib


class Computation_Process(object):
            
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def computation_call(self, ijson):
        import httplib
        project_id   = ijson.get('project_id' ,'A')
        ws_id        = ijson.get('ws_id' ,'A')
        db_name        = ijson.get('db_name' ,'A')
        doc_id        = ijson.get('doc_id' ,'')
        page_no        = ijson.get('page_no' ,'')
        grid_id        = ijson.get('grid_id' ,'')    
        data = {"oper_flag":518, "project_id":project_id, "ws_id":ws_id, "db_name":db_name, "doc_id":doc_id, "page_no":page_no, "grid_id":grid_id}

        extention = "/tree_data"
        params = json.dumps({'input':data})
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn  = httplib.HTTPConnection('172.16.20.212:9008', timeout=120)
        conn.request("POST",  extention, params, headers)
        #response = conn.getresponse()
        #d_info = response.read()
        conn.close()
        return [{'message':'done'}]

    def get_display_name(self, doc_info_dct, project_id):
        project_id = str(project_id)
        if project_id in ('20', ):
            db_name = 'AECN_INC'
        elif project_id in ('80', ):
            db_name = 'AECN_CBDS'
        doc_str = ', '.join({"'"+str(e)+"'"for e in doc_info_dct.keys()})

        db_data_lst = ['172.16.20.52', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        r_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s) """%(doc_str)
        m_cur.execute(r_qry)
        mt_data = m_cur.fetchall()
        m_conn.close()
        disp_name = ''
        for row in mt_data:
            doc_id, meta_data = str(row[0]), eval(row[1])
            gdn = meta_data.get("Display_Name", "")
            if not gdn:continue
            if gdn:    
                disp_name = gdn
                break
        return disp_name

    def alter_mysql_tables(self, m_cur, m_conn, table_name, arr_columns):
        for col in arr_columns:
            stmt = """ALTER TABLE %s ADD COLUMN %s TEXT"""%(table_name, col)
            try:
                m_cur.execute(stmt)
                m_conn.commit()
            except:pass
        return 'done'

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
            if src_typ:
                src_typ =src_typ.split('^')[0]
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
        
        crt2_stmt = """CREATE TABLE IF NOT EXISTS data_mgmt(resid BIGINT(20), training_id BIGINT(20), project_id INT(11), url_id INT(11), user_id INT(11), agent_id INT(11), mgmt_id INT(11), parent_docid INT(11), main_docid INT(11), docid INT(11), pageno INT(11), taxoid INT(11), taxoname TEXT, doc_type varchar(32), active_status VARCHAR(1), process_status VARCHAR(1), istraining VARCHAR(1), data0 TEXT, data1 MEDIUMTEXT, process_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, training_tableid INT(11), training_group_id INT(11), applicator_key TEXT, reviewed_user TEXT)"""
        try:
            m_cur.execute(crt2_stmt)
            m_conn.commit()
        except Exception as e:
            print e

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

    def create_tableid_docid(self, ijson):
        '''
        project_id, inc_prokect_id, inc_workspace_id, company_name, doc_id, page_no, grid_id, populate_status, queue_status
        '''
        project_id              = ijson['project_id']
        inc_project_id          = ijson['inc_project_id']
        workspace_id            = ijson['workspace_id']
        company_display_name    = ijson['company_name']
        user_name               = ijson['user']
        doc_info                = ijson['doc_info']
        dcid                  = next(iter(doc_info))
        page_no, grid_id        = doc_info[dcid][0].split('-')
        if project_id in ('20', ):
            db_name = 'AECN_INC'
        elif project_id in ('80', ):
            db_name = 'AECN_CBDS'

        if not ijson.get('Old_deal_id'):
            company_display_name = en_obj.convert(company_display_name)
            if isinstance(company_display_name, unicode):
                company_display_name = company_display_name.encode('utf-8')
            c_name = ''
            for elm in company_display_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            dsp_name = self.get_display_name({dcid:{}}, project_id)
            dsp_name = en_obj.convert(dsp_name)
            if isinstance(dsp_name, unicode):
                dsp_name = dsp_name.encode('utf-8')
            import chk_add_meta_to_txt as py
            obj = py.Company_populate_new()
            print {company_name:company_display_name}, project_id, dcid
            company_name_deal_map = obj.write_4433_company_info({company_name:company_display_name}, project_id, dcid, dsp_name) 
            deal_id = company_name_deal_map[company_name]
            print ['deal', deal_id]
            company_id = '%s_%s'%(project_id, deal_id)

            from_doc_map, max_doc    = self.get_existing_doc_info(company_id) 
            from_table_map, max_table    = self.get_existing_table_info(company_id)

            i_doc_ar    = []
            i_table_ar  = []
            i_table_ar2  = []
            
            if (dcid, db_name) not in from_doc_map:
                dc_tup = (dcid, project_id, deal_id, d_n, 'Y', dcid, db_name, deal_id, 21, '')
        
            sti = (str(dcid), str(page_no), str(grid_id)) 
            if (sti, db_name) not in from_table_map:
                n_table = max_table
                i_table_ar.append((n_table, project_id, deal_id, dcid, n_table, page_no,  'Y', 'Y', 'Y', str(sti), db_name, n_table, n_table, deal_id, 21, 1, ''))
                i_table_ar2.append((n_table, project_id, deal_id, dcid, page_no, 'Y', 'Y', 'Grid', deal_id, 21, 1, '', '0', n_table, n_table)) 
            else: 
                n_table = from_table_map[(sti, db_name)]
        
            db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
            m_conn, m_cur = self.mysql_connection(db_data_lst)

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
        # computation_service_call
        return
            
    def computation_service_call(self, ijson, table_id, deal_id):
        import httplib
        project_id   = ijson.get('project_id' ,'A')
        ws_id        = ijson.get('ws_id' ,'A')
        db_name        = ijson.get('db_name' ,'A')
        doc_id        = ijson.get('doc_id' ,'')
        page_no        = ijson.get('page_no' ,'')
        grid_id        = ijson.get('grid_id' ,'')    
        data = {"oper_flag":518, "project_id":project_id, "ws_id":ws_id, "db_name":db_name, "doc_id":doc_id, "page_no":page_no, "grid_id":grid_id}

        extention = "/tree_data"
        params = json.dumps({'input':data})
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn  = httplib.HTTPConnection('172.16.20.212:9008', timeout=120)
        conn.request("POST",  extention, params, headers)
        #response = conn.getresponse()
        #d_info = response.read()
        conn.close()
        return [{'message':'done'}]
            

