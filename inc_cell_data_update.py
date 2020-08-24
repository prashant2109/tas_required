import os, sys, json, lmdb, hashlib, ast, httplib
import MySQLdb
from db.webdatastore import webdatastore
from url_execution import Request

def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class UpdateINC(object):
    def __init__(self, company_id):
        self.company_id = company_id
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
        for line in r_d[4:]:
            line = line.strip()
            print line
            n_did, n_dn, o_did = line.split('\t') 
            if o_did == '#N/A':continue      
            if o_did == '14':continue
            nto_doc_map[o_did] = n_did
        return nto_doc_map
        
    def cp_call_reprocess(self, doc_id):
        # {"oper_flag":97026,"doc_lst":"3076","stage_lst":"12~2~3~4~7","project_id":34,"db_name":"AECN_INC","ws_id":1,"doc_type":"PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"1","user_id":"sunil"}
        ru_Obj = Request()
        s_json = {"oper_flag":97026,"doc_lst":"%s"%(doc_id),"stage_lst":"12~2~3~4~7","project_id":34,"db_name":"AECN_INC","ws_id":1,"doc_type":"PDF","p_type":"r","meta_data":"{}","lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"1","user_id":"sunil"} 
        as_json = json.dumps(s_json)
        url_info = """ http://172.16.20.52:5010/tree_data?input=[%s] """%(as_json)
        print url_info
        txt, txt1   = ru_Obj.load_url(url_info, 120)
        print '>>>>>>>>', [txt, txt1]
        #data = json.loads(txt1)
        return 

    def call_reprocess(self, doc_id):
        s_json = {"oper_flag":97026,"doc_lst":"%s"%(doc_id),"stage_lst":"1~12~2~3~4~7","project_id":34,"db_name":"AECN_INC","ws_id":1,"doc_type":"PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"1","user_id":"sunil"} 
        as_json = json.dumps(s_json)
        conn  = httplib.HTTPConnection('172.16.20.52:5010', timeout=120)
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn.request("POST","/tree_data", as_json, headers)
        response = conn.getresponse()
        data = response.read()
        print '<<<<<<<<<<<<<<<<<<', data
        conn.close()
        return #{'message':'done','data':data}
    
    def all_doc_wise_update(self, doc_ids, flg='2'):
        nto_doc_map   = self.read_txt_and_tar_it()
        lmdb_path    = os.path.join('/var/www/html/fill_table/', self.company_id, 'XML_BBOX') 
        print lmdb_path
        env = lmdb.open(lmdb_path)
        txn = env.begin()
        for doc in doc_ids:
            n_did = nto_doc_map[doc] 
            #################### copy tar #####################
            if flg == '2':
                os.system('cd /var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s; tar -cvf /tmp/%s.tar  *; cd -'%(self.project_id, self.deal_id, doc, n_did))
                dest_path = '/var/www/html/WorkSpaceBuilder_DB/34/1/pdata/docs/%s/'%(n_did)
                if not os.path.exists(dest_path):
                    os.system('mkdir -p %s'%(dest_path))
                os.system(' tar -xvf /tmp/%s.tar -C %s'%(n_did, dest_path))
                ################# grid insertion ##################
                self.call_reprocess(n_did)
            ##################################################
            if flg == '1':
                self.update_new_inc_cell_data([doc], nto_doc_map, txn)         
            ################# reprocesss ####################
    
    def update_new_inc_cell_data(self, doc_ids, nto_doc_map, txn):
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
        for row in t_data:
            table_id, doc_id, page_no  = map(str, row)
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
                dt_data = {"value_taxo_str": "", "md_key": "", "colspan": cs, "topic_name": "", "ldr": ldr, "md_taxo": "", "dparent_ids": "", "rowspan": rs, "slevel": "", "parent_id": "", "value_filter_str": "", "md_txph": "", "txph": "", "xml_ids": "$$".join(ref_ar[:]), "chref": "%s_%s" %(0, len(text_str)), "cell_id": "%s_%s" %rc, "value_txph": "", "md_s_range": "", "data": text_data, "rects": "", "custids": "0", "md_val": "", "md_pos": "", "pdf_xmlids": "$$".join(ref_ar[:]), "sph_index": sph_index, 'bbox' : bbox_str}
                inc_rc_dct[rc_str]  = dt_data  
            new_doc = nto_doc_map[doc_id]
            dpg_wise_cell_dct.setdefault(new_doc, {}).setdefault(page_no, {})[table_id] = inc_rc_dct
            if (str(new_doc), page_no) not in del_rows:
                del_rows.append((str(new_doc), page_no))
            print '\n'
        sys.exit('MT')
        '''
        for k, v in dpg_wise_cell_dct.iteritems():
            print k
            for vk, vv in v.iteritems():
                print vk, vv
            print '**' * 50, '\n'
        sys.exit()
        '''
        self.update_grid_info_new_inc(dpg_wise_cell_dct, del_rows)
        return 
        
    def update_grid_info_new_inc(self, dpg_wise_cell_dct, del_rows):
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
            
        insert_rows = []
        for doc, page_dct in dpg_wise_cell_dct.iteritems():
            for page, table_dct in page_dct.iteritems():
                get_max_grid = doc_wise_max_grid_info.get((doc, page), 0)
                for incre_idx, (table, cell_dict) in  enumerate(table_dct.iteritems(), 1):
                    cell_data_user = {'data':cell_dict, 'user_id':'FROM_OLD_INC'}
                    json_cell_data = json.dumps(cell_data_user)
                    des_grid = get_max_grid + incre_idx
                    data_tup = (int(doc), int(page), des_grid, 'Y', json_cell_data, 'FROM_OLD_INC', json_cell_data)
                    insert_rows.append(data_tup)
            
        #sys.exit()
        #print insert_rows;sys.exit()
        print 'length-: ', len(insert_rows)
        for k in insert_rows:
            print '>>>>>>>>>>>>>', k, '\n'
        #sys.exit('MT')
            #m_conn.commit()
        '''
        if insert_rows:
            insert_stmt = """ INSERT INTO db_data_mgmt_grid_slt(docid, pageno, groupid, active_status, udata, userid, sdata) VALUES(%s, %s, %s, %s, %s, %s, %s) """  
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        '''
        for r_tup in insert_rows:
            print 'Here'
            insert_stmt = """ INSERT INTO db_data_mgmt_grid_slt(docid, pageno, groupid, active_status, udata, userid, sdata) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s') """%r_tup
            m_cur.execute(insert_stmt)
        m_conn.commit()
        m_conn.close()
        return 

if __name__ == '__main__':
    company_id = sys.argv[1]
    p_Obj = UpdateINC(company_id) 
    doc_ids = sys.argv[2] 
    if doc_ids == 'ALL':
        doc_ids = []
    else:
        doc_ids = doc_ids.split('#')
    flg = str(sys.argv[3])
    #p_Obj.update_new_inc_cell_data(doc_ids)
    p_Obj.all_doc_wise_update(doc_ids, flg)
    #p_Obj.all_doc_wise_update(doc_ids)
    #doc_id = '3076' 
    #p_Obj.call_reprocess(doc_id)


