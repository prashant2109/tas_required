import os, sys, json, lmdb, hashlib, ast, httplib
import MySQLdb, sqlite3
from db.webdatastore import webdatastore
from url_execution import Request
import shelve, copy
import common.dbcrypt as dbcrypt
from read_shelve_file import read_enc_shelve_dict

def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class UpdateINC(object):
    def __init__(self, company_id, company_name, inc_company_name):
        self.company_id        = company_id
        self.company_name      = company_name
        self.inc_company_name  = inc_company_name
        self.project_id, self.deal_id = company_id.split('_')    
        self.lmdb_obj    = webdatastore()
        self.output_path = '/var/www/html/fundamentals_intf/output/'

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def get_bbox_frm_xml(self, txn1, table_id, t_x, ret_f=None):
        valbox      = []
        x_c         = ''
        done_d      = {}
        for x in [t_x]:
           bbox = txn1.get(str(table_id)+':$$:'+str(x))
           if not bbox:
               bbox = txn1.get(str(table_id)+':$$:'+self.get_quid(str(x)))
           #print str(table_id)+':$$:'+str(x), txn1.get(str(table_id)+':$$:'+str(x))
           if bbox:
                bbox1 = ast.literal_eval(bbox)
                if not bbox1:continue
                    #print bbox1
                cnt_flg = 0
                for tmpbbox in bbox1[0]:
                    if not tmpbbox or tuple(tmpbbox) in done_d:continue
                    done_d[tuple(tmpbbox)]  = 1
                    valbox.append(tmpbbox)
                    cnt_flg = 1

        if not valbox:
            for  cxml in t_x.split(':@:'):
                for x in cxml.split('#'):
                   if x and x_c =='':
                        x_c= x 
                   
                   bbox = txn1.get(str(table_id)+':$$:'+str(x))
                   if not bbox:
                       bbox = txn1.get(str(table_id)+':$$:'+self.get_quid(str(x)))
                   #print str(table_id)+':$$:'+str(x), txn1.get(str(table_id)+':$$:'+str(x))
                   if bbox:
                        bbox1 = ast.literal_eval(bbox)
                        #print bbox1
                        if not bbox1 or tuple(bbox1[0][0]) in done_d:continue
                        done_d[tuple(bbox1[0][0])]  = 1
                        valbox.append(bbox1[0][0])
                        continue
                   #print str(table_id)+':$$:'+str(x), txn1.get(str(table_id)+':$$:'+str(x.split('@')[0]))
                   bbox = txn1.get(str(table_id)+':$$:'+str(x.split('@')[0]))
                   if not bbox:
                       bbox = txn1.get(str(table_id)+':$$:'+self.get_quid(str(x.split('@')[0])))
                   if bbox:
                        bbox1 = ast.literal_eval(bbox)
                        #print bbox1
                        if tuple(bbox1[0][0]) in done_d:continue
                        done_d[tuple(bbox1[0][0])]  = 1
                        valbox.append(bbox1[0][0])
        if ret_f == 'Y':
            if x_c == '':
                x_c = 'x'+table_id+'_'+table_id
            return valbox, x_c
        return valbox
        
    def read_txt_and_tar_it(self):
        f = open('/var/www/html/misc/JPMorgan.txt')
        r_d = f.readlines()
        f.close()
        nto_doc_map = {}
        for line in r_d[3:]:
            line = line.strip()
            n_did, n_dn, o_did = line.split('\t') 
            if o_did == '#N/A':continue      
            #if o_did == '14':continue
            nto_doc_map[o_did] = n_did
        return nto_doc_map
        
    def write_encrypt_shelve(self, fname, data):
        tabname = 'data'
        obj = dbcrypt.DBCrypt('1')
        obj.write_to_dbcrypt(fname, data, tabname)
        return 

    def call_reprocess(self, doc_id, get_e_pages={}):
        print [get_e_pages]
        if not get_e_pages:
            return
        pgs = get_e_pages.keys()
        pgs.sort(key=lambda x:int(x))
        selected_pages = ','.join(pgs)
        s_json = {"oper_flag":97026,"doc_lst":"%s"%(doc_id),"stage_lst":"12~2~3~4~7","project_id":34,"db_name":"AECN_INC","ws_id":1,"doc_type":"PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":selected_pages,"ocr_chk":"N","lc":"1","pd":"1","user_id":"sunil"} 
        print s_json
        as_json = json.dumps(s_json)
        conn  = httplib.HTTPConnection('172.16.20.52:5010', timeout=120)
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn.request("POST","/tree_data", as_json, headers)
        response = conn.getresponse()
        data = response.read()
        print '<<<<<<<<<<<<<<<<<<', data
        conn.close()
        return #{'message':'done','data':data}

    def cal_bobox_data_new(self):
        #self.output_path = '/var/www/html/fundamentals_intf/output/'
        lmdb_folder      = os.path.join('/var/www/html/fundamentals_intf/output/', self.company_id, 'doc_page_adj_cords')
        doc_page_dict    = {}
        if os.path.exists(lmdb_folder):
            env = lmdb.open(lmdb_folder, readonly=True)
            txn = env.begin()
            if 1:
                cursor = txn.cursor()
                for doc_id, res_str in cursor:
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
                        doc_page_dict[doc_id] = page_dict
        return doc_page_dict

    def get_image_height_width(self, filename):
        from PIL import Image
        width, height = '', ''
        with Image.open(filename) as image:
            width, height = image.size
        return ['0', '0', width, height]
        
    def create_coordinates_page_wise(self, doc_id, page):
        image_path  = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/image/%s-page-%s.png'%(self.project_id, self.deal_id, doc_id, doc_id, page)
        if os.path.exists(image_path):
            return self.get_image_height_width(image_path)
        else:
            return []
        
    def update_non_existing_pages(self, doc_id, existing_pages):
        doc_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/'%(self.project_id, self.deal_id, doc_id)
        no_pages_path = os.path.join(doc_path, 'pdf_total_pages') 
        f = open(no_pages_path)
        nop = int(f.read())
        f.close()
        page_coord_dct = {}
        for page in range(1, nop):
            page = str(page)
            if page in existing_pages:continue
            chk_path = os.path.join(doc_path, 'CID', '%s.sh'%(page))
            if not os.path.exists(chk_path):continue
            page_coordinates = self.create_coordinates_page_wise(doc_id, page)
            if not page_coordinates:continue
            page_coord_dct[page] = page_coordinates
        return page_coord_dct

    def update_page_rect(self, doc_ids, doc_wise_page_coord, doc_pages):
        doc_ne_processed_pages = {}
        for doc in doc_ids:
            get_preferred_pages = []
            if doc_pages:
                get_preferred_pages = doc_pages.get(doc, [])
            get_doc_info_dct = doc_wise_page_coord[doc]
            check_exists = copy.deepcopy(get_doc_info_dct)
            non_existing_page_rect_dct = self.update_non_existing_pages(doc, get_doc_info_dct)
            get_doc_info_dct.update(non_existing_page_rect_dct)
            for page, coord_lst in get_doc_info_dct.iteritems():
                if get_preferred_pages and (page not in get_preferred_pages):continue
                sh_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/CID/%s.sh'%(self.project_id, self.deal_id, doc, page)
                print sh_path
                celldict_result1    = read_enc_shelve_dict(sh_path)
                celldict_result1['page_rect']  = coord_lst
                try:
                    cell_info_dct_data = celldict_result1['cell_info_dict']
                except KeyError:
                    continue
                for rc, val_i in cell_info_dct_data.iteritems():
                    celldict_result1['cell_info_dict'][rc]['page_rect'] = '_'.join(map(str, coord_lst)) 
                    get_chunk_lst = celldict_result1['cell_info_dict'][rc]['cell_dict']['chunks']
                    modified_chnk_lst = []
                    for chnk_dct in get_chunk_lst:
                        raw_chunk_ref = chnk_dct.get('raw_chunk_ref', '')
                        if raw_chunk_ref:
                            chnk_dct['SEQNO']  = raw_chunk_ref
                        modified_chnk_lst.append(chnk_dct)
                    celldict_result1['cell_info_dict'][rc]['cell_dict']['chunks'] = modified_chnk_lst
                print ['DDDDDDDD',doc, page]#, celldict_result1, '\n'
                if page not in check_exists:
                    doc_ne_processed_pages.setdefault(doc, {})[page] = 1
                self.write_encrypt_shelve(sh_path, celldict_result1)
        return doc_ne_processed_pages 
        
    def pre_and_re_docs(self, doc_ids, doc_pages):
        doc_str = ', '.join(["'" +str(e)+ "'" for e in doc_ids])
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(self.company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """  select n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid') and n.docid in (%s) """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()        
        m_conn.close()
        doc_wise_existing_pages = {}
        for row in t_data:
            tb, doc, pg = map(str, row) 
            get_dc_wise_pages = []
            if doc_pages:
                get_dc_wise_pages = doc_pages.get(doc, [])
                if pg not in get_dc_wise_pages:continue
            doc_wise_existing_pages.setdefault(doc, {})[pg] = 1
        return doc_wise_existing_pages
        
    def recheck_function(self, doc_ids, nto_doc_map, txn, doc_wise_page_coord):
         



    def all_doc_wise_update(self, doc_ids, flg='2', doc_pages={}):
        nto_doc_map   = self.read_txt_and_tar_it()
        doc_wise_page_coord   = self.cal_bobox_data_new()
        if flg == '2':
            doc_wise_existing_pages   = self.pre_and_re_docs(doc_ids, doc_pages)
            doc_processed_ne_pages = self.update_page_rect(doc_ids, doc_wise_page_coord, doc_pages)
            #print doc_wise_existing_pages
            #print doc_processed_ne_pages
        lmdb_path    = os.path.join('/var/www/html/fill_table/', self.company_id, 'XML_BBOX') 
        print lmdb_path
        env = lmdb.open(lmdb_path)
        txn = env.begin()
        for doc in doc_ids:
            n_did = nto_doc_map[doc] 
            #################### copy tar #####################
            if flg == '2':
                get_e_pages  = doc_wise_existing_pages.get(doc, {})
                get_ne_pages = doc_processed_ne_pages.get(doc, {})
                get_e_pages.update(get_ne_pages)
                #print get_e_pages;continue
                #sys.exit()
                os.system('cd /var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s; tar -cvf /tmp/%s.tar  * --exclude "__temp"; cd -'%(self.project_id, self.deal_id, doc, n_did))
                dest_path = '/var/www/html/WorkSpaceBuilder_DB/34/1/pdata/docs/%s/'%(n_did)
                if not os.path.exists(dest_path):
                    os.system('mkdir -p %s'%(dest_path))
                os.system(' tar -xvf /tmp/%s.tar -C %s'%(n_did, dest_path))
                ################# grid insertion ##################
                self.call_reprocess(n_did, get_e_pages)
            ##################################################
        # recheck function
            if flg == '1':
                self.update_new_inc_cell_data([doc], nto_doc_map, txn, doc_wise_page_coord[doc]) 
            ################# reprocesss ####################

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
            ddict[sheet_id] = node_name
        return ddict
        
    def read_classified_tables(self):
        sheet_node_map = self.get_sheet_id_map()
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%(self.company_name, self.project_id)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT table_id, sheet_id FROM table_group_mapping """
        cur.execute(read_qry) 
        t_data = cur.fetchall()
        conn.close()
        classified_tables = {}
        for row in t_data:
            rts, sheet_id = map(str, row)
            for t_s in rts.split('^!!^'):   
                if not t_s:continue
                node_name = sheet_node_map[sheet_id]
                classified_tables[t_s] = node_name
        return classified_tables 
    
    def update_new_inc_cell_data(self, doc_ids, nto_doc_map, txn, page_dct):
        
        doc_str = ', '.join(["'" +str(e)+ "'" for e in doc_ids])        
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(self.company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        read_qry = """  select n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid') """
        if doc_ids:
            read_qry = """  select n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid') and n.docid in (%s) """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()        
        m_conn.close()

        dpg_wise_cell_dct = {}
        del_rows = []
        to_be_scoped_lst = {}
        for row in t_data:
            table_id, doc_id, page_no  = map(str, row)
            get_page_rect = page_dct[page_no]
            celldata_db_path = os.path.join(self.output_path, company_id, 'cell_data', doc_id, table_id)
            celldata = self.lmdb_obj.read_all_from_lmdb(celldata_db_path)
            if not celldata:continue
            #print 'MMMMMMMMMMMMMMMMM', (table_id, doc_id, page_no)
            get_tb = next(iter(celldata))
            all_rcs = celldata[get_tb].keys()
            all_rcs.sort(key=lambda x:(int(x[0]), int(x[1])))
            inc_rc_dct = {} 
            for rc in all_rcs:
                grid_dct = celldata[get_tb][rc] 
                #print grid_dct
                rc_str = '%s_%s'%(rc)
                if grid_dct['section_type']  == 'GV':   
                    ldr = 'value'
                elif grid_dct['section_type'] == 'HGH':
                    ldr = 'hch'
                elif grid_dct['section_type'] == 'VGH':
                    ldr = 'vch'
                elif grid_dct['section_type'] == 'GH':
                    ldr = 'gh'
                else:
                    ldr = ''
                rs           = grid_dct['rowspan'] 
                cs           = grid_dct['colspan']
                bref_ar      = grid_dct['text_ids']
                ref_ar       = map(lambda x:x.strip(), bref_ar)
                text_str     = grid_dct['text_lst']
                text_data     = ' '.join(grid_dct['text_lst'])
        
                #xml_id       = '#'.join(ref_ar)
                #hashed_xml   = hashlib.md5(str(xml_id)).hexdigest() 
                # call 
                #try:
                #    bbox_info    = eval(txn.get(str(table_id)+':$$:'+str(xml_id), '[]')) #grid_dct['bbox_lst']
                #except:bbox_info    = eval(txn.get(str(table_id)+':$$:'+str(hashed_xml), '[]'))
                ##s = []
                bbox_dt_lst = [] 
                for xml_id in ref_ar:
                    bbox    =   self.get_bbox_frm_xml(txn, table_id, xml_id, ret_f=None)   #eval(txn.get(str(table_id)+':$$:'+str(xml_id), '[]'))
                    bbox_dt_lst.append(bbox)
                ##print s
                ##sys.exit('MT')
                '''
                bbox_dt_lst = [] 
                for xml_id in ref_ar:
                    bbox    = eval(txn.get(str(table_id)+':$$:'+str(xml_id), '[]'))
                    if bbox:
                        bbox_dt_lst.append(bbox[0][0])
                '''
                #print bbox_dt_lst, text_data, ref_ar
                px1, py1, px2, py2 = 0, 0, 0, 0
                bbox_str_lst = []
                if bbox_dt_lst:
                    for bbx in bbox_dt_lst:
                        for bx in bbx:
                            if not bx:continue
                            x1, y1, w, h = map(int, bx)
                            x2 = x1 + w
                            y2 = y1 + h
                            if x1 > px1:px1 = x1
                            if y1 > py1:py1 = y1
                            if x2 > px2:px2 = x2
                            if y2 > py2:py2 = y2
                            bx_str = '_'.join(map(str, [x1, y1, x2, y2]))
                            if bx_str not in bbox_str_lst:
                                bbox_str_lst.append(bx_str)
                bbox_str = '$$'.join(bbox_str_lst)
                #print [(doc_id, table_id, page_no), rs, cs, ref_ar, text_str,  bbox_dt_lst, bbox_str], '\n'
                #sys.exit()
                sph_index = ''
                #if rc == (12, 0):
                #    print text_data;sys.exit()
                #print text_data 
                #text_data = text_data.replace("'", "")
                #print text_data, '\n'
                dt_data = {"value_taxo_str": "", "md_key": "", "colspan": cs, "topic_name": "", "ldr": ldr, "md_taxo": "", "dparent_ids": "", "rowspan": rs, "slevel": "", "parent_id": "", "value_filter_str": "", "md_txph": "", "txph": "", "xml_ids": "$$".join(ref_ar[:]), "chref": "%s_%s" %(0, len(text_str)), "cell_id": "%s_%s" %rc, "value_txph": "", "md_s_range": "", "data": text_data, "rects": "", "custids": "0", "md_val": "", "md_pos": "", "pdf_xmlids": "$$".join(ref_ar[:]), "sph_index": sph_index, "bbox" : bbox_str}
                inc_rc_dct[rc_str]  = dt_data  
            new_doc = nto_doc_map[doc_id]
            dpg_wise_cell_dct.setdefault(new_doc, {}).setdefault(page_no, {})[table_id] = inc_rc_dct
            if (str(new_doc), page_no) not in del_rows:
                del_rows.append((str(new_doc), page_no))
            #print '\n'
        '''
        for k, v in dpg_wise_cell_dct.iteritems():
            print k
            for vk, vv in v.iteritems():
                print vk, vv
            print '**' * 50, '\n'
        '''
        #sys.exit()
        self.update_grid_info_new_inc(dpg_wise_cell_dct, del_rows)
        return 
        
    def update_grid_info_new_inc(self, dpg_wise_cell_dct, del_rows):
        classified_tables =  self.read_classified_tables()

        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'" +str(e)+ "'" for e in dpg_wise_cell_dct.keys()]) 

        if 1:
            del_stmt = """ DELETE FROM db_data_mgmt_grid_slt WHERE docid=%s  AND pageno=%s """
            m_cur.executemany(del_stmt, del_rows)
            #m_conn.commit()
        
        read_qry = """ select docid, pageno, max(groupid) from db_data_mgmt_grid_slt where docid in (%s)  and groupid < 1000 group by docid, pageno;  """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
            
        doc_wise_max_grid_info = {}
        for row in t_data:
            docid, pageno = map(str, row[:-1])
            max_grid = int(row[-1])
            doc_wise_max_grid_info[(docid, pageno)] = max_grid
        #print doc_wise_max_grid_info#;sys.exit() 
            
        r_qry = """  SELECT doc_id, page_no, grid_id FROM Focus_Data_mgmt   """
        m_cur.execute(r_qry)
        tr_data = m_cur.fetchall()

        chk_focus_mgmt_data_dct = {}
        for rsw in  tr_data:    
            dcid, pgno, gdid = map(int, rsw)
            chk_focus_mgmt_data_dct[(dcid, pgno, gdid)] = 1     
            
        insert_rows = []
        to_be_scoped_lst = []
        del_already_scoped_lst = []
        for doc, page_dct in dpg_wise_cell_dct.iteritems():
            for page, table_dct in page_dct.iteritems():
                get_max_grid = doc_wise_max_grid_info.get((doc, page), 0)
                for incre_idx, (table, cell_dict) in  enumerate(table_dct.iteritems(), 1):
                    cell_data_user = {'data':cell_dict, 'user_id':'FROM_OLD_INC^^%s'%(table), 'old_inc_table':table}
                    json_cell_data = json.dumps(cell_data_user)
                    #m_conn.escape_string(insert_stmt)
                    json_cell_data =  m_conn.escape_string(json_cell_data)
                    des_grid = get_max_grid + incre_idx
                    if (table in classified_tables) and ((int(doc), int(page), des_grid) not in chk_focus_mgmt_data_dct):
                        t_name = classified_tables[table]
                        to_be_scoped_lst.append((int(doc), self.inc_company_name, t_name, int(page), des_grid, ''))
                        del_already_scoped_lst.append((int(doc), int(page), des_grid))
                    data_tup = (int(doc), int(page), des_grid, 'Y', json_cell_data, 'FROM_OLD_INC^^%s'%(table), json_cell_data)
                    insert_rows.append(data_tup)
            
        for r_tup in insert_rows:
            print 'Here'
            insert_stmt = """ INSERT INTO db_data_mgmt_grid_slt(docid, pageno, groupid, active_status, udata, userid, sdata) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s') """%(r_tup)
            try:
                m_cur.execute(insert_stmt)
            except Exception as e:
                print e
                print r_tup;sys.exit('INSERT ERROR')
            
        if del_already_scoped_lst: 
            del_stmt = """  DELETE FROM Focus_Data_mgmt WHERE doc_id=%s AND page_no=%s AND grid_id=%s  """
            #m_cur.executemany(del_stmt, del_already_scoped_lst)
        
        if to_be_scoped_lst: 
            ins_stmt = """ INSERT INTO Focus_Data_mgmt(doc_id, company, table_name, page_no, grid_id, tableid) VALUES(%s, %s, %s, %s, %s, %s) """
            m_cur.executemany(ins_stmt, to_be_scoped_lst)

        m_conn.commit()
        m_conn.close()
        return 
    
    def read_encrypted_shelve_file(self):
        #sh_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/116/output/7/CID/53.sh' 
        sh_path = '/var/www/html/WorkSpaceBuilder_DB/34/1/pdata/docs/3087/CID/53.sh' 
        sh_info = read_enc_shelve_dict(sh_path)
        print sh_info
        return  
    
if __name__ == '__main__':
    company_id       = sys.argv[1]
    company_name     = sys.argv[2]
    inc_company_name = sys.argv[3]

    p_Obj = UpdateINC(company_id, company_name, inc_company_name) 
    doc_ids = sys.argv[4] 
    if doc_ids == 'ALL':
        doc_ids = []
    else:
        doc_ids = doc_ids.split('#')
    flg = str(sys.argv[5])
    doc_pages = {}
    if len(sys.argv) > 6:
        doc_pages = sys.argv[6]
        #print type(doc_pages);sys.exit()
        #doc_pages = eval(doc_pages) 
        #print doc_pages
        doc_pages = json.loads(doc_pages) 
        dc_pgs = {}
        for d, p_lst in doc_pages.iteritems():
            dc_pgs[str(d)] = map(str, p_lst)
        doc_pages = copy.deepcopy(dc_pgs)
    p_Obj.all_doc_wise_update(doc_ids, flg, doc_pages)
    ##p_Obj.read_encrypted_shelve_file() 


