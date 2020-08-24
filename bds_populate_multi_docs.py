import os, sys, copy
import MySQLdb
import pprocess
import shelve, time
import sets, commands, lmdb
import read_norm_cell_data as Slt_normdata
sObj = Slt_normdata.Slt_normdata()
from db.webdatastore import webdatastore
lmdb_obj = webdatastore()
import slt_map_sectiontype as slt_map_sectiontype
Mobj = slt_map_sectiontype.Map_sectiontype()
import font_id_for_text_final as Font_id_text
StlyObj = Font_id_text.Font_id_text() 
import xml_to_bbox_normid as cell_id_bbox
Xml_Cell_Obj = cell_id_bbox.cell_id_bbox()
output_path = '/var/www/html/fundamentals_intf/output/'
import sqLite.sqLiteApi as sqLiteApi
qObj    = sqLiteApi.sqLiteApi()
from googletrans import Translator
translator = Translator()
sys.path.append('/root/muthu_translate/')
import socket_client_utils2 as socket_client_utils2
#import html_entity_to_single_char
import common.convert as convert
convob = convert.convert()

def index_all_table(company_id):
    from getCompanyName_machineId import getCN_MID
    getCompanyName_machineId = getCN_MID()
    company_name, machine_id = getCompanyName_machineId[company_id]
    model_number = '1'
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)
    db_file     = os.path.join('/mnt/eMB_db/', company_name, model_number, 'company_report.db')   
    conn = qObj.create_connection(db_file)
    cur  = conn.cursor()
    table_name = 'Table_Report'
    column_list = [('row_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('table_id', 'VARCHAR(20)'), ('doc_id', 'VARCHAR(20)'), ('classification', 'VARCHAR(256)'), ('normalization', 'VARCHAR(1)'), ('error_accepted', 'VARCHAR(1)'), 
                   ('db_status', 'VARCHAR(1)')]
    column_tup = tuple(map(lambda x:x[0], column_list[1:]))
    qObj.createLiteTable(conn, cur, '', table_name, column_list)
    data = []
    for doc_tup in norm_res_list:
        doc_id, page_number, norm_table_id = map(lambda x:str(x), doc_tup)
        db_tup = (norm_table_id, doc_id, '', 'Y', 'N', 'N')  
        data.append(db_tup)
    stmt = 'delete from %s' %(table_name)
    cur.execute(stmt)
    qObj.insertIntoLite(conn, cur, '', table_name, column_tup, data)
    conn.commit()
    conn.close()
    return 'done'

def insert_update_table_report(company_id, table_ids, company_name, model_number):
    #from getCompanyName_machineId import getCN_MID
    #getCompanyName_machineId = getCN_MID()
    #company_name, machine_id = getCompanyName_machineId[company_id]
    #model_number = '1'
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    #norm_res_list = sObj.slt_normresids(project_id, url_id)
    db_file     = os.path.join('/mnt/eMB_db/', company_name, model_number, 'company_report.db')   
    print 'db_file',db_file
    conn = qObj.create_connection(db_file)
    cur  = conn.cursor()
    table_name = 'Table_Report'
    column_list = [('row_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('table_id', 'VARCHAR(20)'), ('doc_id', 'VARCHAR(20)'), ('classification', 'VARCHAR(256)'), ('normalization', 'VARCHAR(1)'), ('error_accepted', 'VARCHAR(1)'), 
                   ('db_status', 'VARCHAR(1)')]
    column_tup = tuple(map(lambda x:x[0], column_list[1:]))
    qObj.createLiteTable(conn, cur, '', table_name, column_list)
    stmt = "select table_id, doc_id, classification, normalization, error_accepted, db_status from Table_Report"
    cur.execute(stmt)
    res = cur.fetchall()
    selected_dict = {}
    for r in res:
        table_id, doc_id, classification, normalization, error_accepted, db_status = map(str, r)
        selected_dict[table_id] = (classification, normalization, error_accepted, db_status)
    tids = [] 
    data = []
    for (doc_id, table_id, page_number, table_type) in table_ids:
        classification, normalization, error_accepted, db_status = selected_dict.get(table_id, ('', 'Y', 'N', 'N'))
        normalization = 'Y'
        error_accepted = 'N'
        db_status = 'N'
        data.append((table_id, doc_id, classification, normalization, error_accepted, db_status))
        tids.append('"'+table_id+'"')
    tstr = ', '.join(tids)
    stmt = 'delete from %s where table_id in (%s)' %(table_name, tstr)
    cur.execute(stmt)
    qObj.insertIntoLite(conn, cur, '', table_name, column_tup, data)
    conn.commit()
    conn.close()
    return 'done'

def get_norm_table_ids(company_name, model_number):
    db_file     = os.path.join('/mnt/eMB_db/', company_name, model_number, 'tas_company.db')  
    conn = qObj.create_connection(db_file)
    cur  = conn.cursor()
    stmt = "select doc_id, page_no, row_id, table_dict, table_type from table_dict_phcsv_info"
    #stmt = "select doc_id, page_no, row_id, table_dict, table_type from table_dict_phcsv_info where table_type='Sentence'"
    #stmt = "select doc_id, page_no, row_id, table_dict, table_type from table_dict_phcsv_info where table_type='Sentence'"
    cur.execute(stmt)
    res = cur.fetchall()
    norm_res_list = []
    for row in res:
        doc_id, page_no, row_id, cell_dict, table_type = map(str, row[:])
        cell_dict = eval(cell_dict)
        norm_res_list.append((doc_id, page_no, row_id, cell_dict, table_type))
    return norm_res_list

def get_comp_model(company_id):
    from getCompanyName_machineId import getCN_MID
    getCompanyName_machineId = getCN_MID()
    company_name, machine_id = getCompanyName_machineId[company_id]
    return company_name

def get_cur_prev_value_from_hgh(txt):
    values = []
    words = txt.split()
    n = len(words)
    for pos, x in enumerate(words):
        if x.strip().lower() == 'eur' and pos+1<=(n-1):
            next_word = words[pos+1].strip()
            st = ''.join([xc for xc in next_word if xc in '0123456789,.'])
            if st.replace(',', '').replace('.', '').isdigit():
                values.append(st)
    return values

def generate(company_id, given_doc_ids=None, src_lan=None):
    company_name = get_comp_model(company_id)
    model_number = '50'
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    norm_res_list = get_norm_table_ids(company_name, model_number)
    print len(norm_res_list)
    #sys.exit()
    table_cell_dict = {}
    for doc_tup in norm_res_list:
        doc_id, page_number, norm_table_id, cell_dict, table_type = doc_tup
        #if norm_table_id != '100':continue
        #if int(norm_table_id) < 360:continue
        if table_type != 'Sentence':
            table_type = 'Table'
        ktup = (doc_id, norm_table_id, page_number, table_type)
        table_cell_dict[ktup] = {0:cell_dict}
        if given_doc_ids:
            if doc_id in given_doc_ids:
                all_doc_table_to_process.append(ktup)
            continue
        else:
            all_doc_table_to_process.append(ktup)
    lmdb_folder = os.path.join(output_path, company_id) 
    if not os.path.exists(lmdb_folder):
        os.mkdir(lmdb_folder)
    doc_html_path = os.path.join(lmdb_folder, 'Doc_Htmls')
    if not os.path.exists(doc_html_path):
        os.mkdir(doc_html_path)
    table_html_path = os.path.join(lmdb_folder, 'Table_Htmls')
    if not os.path.exists(table_html_path):
        os.mkdir(table_html_path)
    ######################################################
    #all_doc_table_to_process = [('164', '70286')]
    total = len(all_doc_table_to_process)
    translated_dict = {}
    res = []
    '''
    for i, x in enumerate(all_doc_table_to_process):
        print [x, i+1, '/', total]
        #if (x[1] != '8896'): continue
        f = generate_map_ds(company_id, x, table_cell_dict[x], src_lan, translated_dict)
        #print table_cell_dict[x]
        #sys.exit()
        each_html_str = f[2]
        table_id = x[1]
        tab_path = os.path.join(table_html_path, str(table_id)+'.html')
        ftab = open(tab_path, 'w')
        ftab.write('<html><body>'+str(each_html_str)+'</body></html>')
        ftab.close()
        res.append(f)
    #sys.exit()
    '''
    print len(all_doc_table_to_process)
    doc_table_html_dict = {}
    res = pprocess.pmap(lambda x:generate_map_ds(company_id, all_doc_table_to_process[x], table_cell_dict[all_doc_table_to_process[x]], src_lan, translated_dict), range(0, len(all_doc_table_to_process)), 6)
    project_id, url_id = company_id.split('_')
    table_id_xml_bbox_dict = {}
    #######################################
    #print res
    error_list = []
    for rf in res:
        if len(rf) != 6:continue
        doc_id, table_id, each_html_str, flg, sflg, er_str = rf
        print doc_id, table_id
        if not flg:
            st = '\t'.join([doc_id, table_id, sflg, er_str])
            error_list.append(st)
            continue
        #print [each_html_str]
        #print '*'*100
        tab_path = os.path.join(table_html_path, str(table_id)+'.html')
        ftab = open(tab_path, 'w')
        ftab.write('<html><body>'+each_html_str+'</body></html>')
        ftab.close()
        if doc_id not in doc_table_html_dict:
            doc_table_html_dict[doc_id] = []
        #try:
        #    Xml_bb_dict = Xml_Cell_Obj.get_cell_bbox_data(project_id, url_id, doc_id, table_id)
        #    table_id_xml_bbox_dict[table_id] = str(Xml_bb_dict)
        #except Exception as e:
        #    st = '\t'.join([doc_id, table_id, 'BBOX', str(e)])
        #    error_list.append(st)
        doc_table_html_dict[doc_id].append((table_id, each_html_str))
    
    #####################################
    for doc_id, table_html_str_list in doc_table_html_dict.items():
        doc_path    =  os.path.join(doc_html_path, str(doc_id))
        if not os.path.exists(doc_path):
            os.mkdir(doc_path)
        html_str = '<html><body>'
        t_html_str = '<html><body>'
        for (table_id, each_html_str) in table_html_str_list:
            print '   +++', table_id
            html_str += '<hr>' + str(each_html_str) + '<hr>'
            t_html_str += '<hr><div id="table-'+str(table_id)+'" class="table-container">' + str(each_html_str) + '</div><hr>'
            tab_path = os.path.join(table_html_path, str(table_id)+'.html')
            ftab = open(tab_path, 'w')
            ftab.write('<html><body>'+str(each_html_str)+'</body></html>')
            ftab.close()
        html_str += '</body></html>'
        t_html_str += '</body></html>'
        html_fname = os.path.join(doc_path, '1.html')    
        fout = open(html_fname, 'w')
        fout.write(html_str)
        fout.close()

        html_fname = os.path.join(doc_path, '2.html')    
        fout = open(html_fname, 'w')
        fout.write(t_html_str)
        fout.close()
    msg = insert_update_table_report(company_id, all_doc_table_to_process, company_name, model_number)
    fname = os.path.join(lmdb_folder, 'errors.txt')
    fout = open(fname, 'w')
    for st in error_list:
        st = st + '\n'
        fout.write(st)
    fout.close()
    if error_list:
        print 'please look this error log', fname
    print "done"


def gen_bboxs(company_id):
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)
    table_id_xml_bbox_dict = {}
    for doc_tup in norm_res_list:
        doc_id, page_number, table_id = map(lambda x:str(x), doc_tup)
        Xml_bb_dict = Xml_Cell_Obj.get_cell_bbox_data(project_id, url_id, doc_id, table_id)
        table_id_xml_bbox_dict[table_id] = str(Xml_bb_dict)
    

def db_connection_batch_name(project_id,url_id):
    db=MySQLdb.connect("172.16.20.229","root","tas123","tfms_urlid_%s_%s" %(str(project_id),str(url_id)))
    cursor=db.cursor()
    return db,cursor

def get_analyst_name(dealid, tableid):
    project_id, url_id = dealid.split('_')
    db, cursor = db_connection_batch_name(project_id, url_id)        
    identification = "select t.user_log_id,  d.reviewed_user from training_mgmt t join data_mgmt d where t.training_id = d.training_id and d.resid='%s'"%(tableid)
    cursor.execute(identification)
    idenresult = cursor.fetchone()
    normalization =  "select user_info from norm_data_mgmt where norm_resid='%s'"%(tableid)
    cursor.execute(normalization)
    normresult = cursor.fetchone()
    cursor.close()
    db.close()
    iusr = ''
    if idenresult :
        if idenresult[1] :
            iusr = idenresult[1] 
        else:
            iusr = idenresult[0]
    else:
      iusr = ''
    nusr = normresult[0] 
    return iusr, nusr
 


def generate_map_ds(company_id, doc_tup, celldata, src_lan, translated_dict):
    sobj = socket_client_utils2.socket_client_utils2('172.16.20.229', '1100')
    project_id, url_id = company_id.split('_')
    doc_id, norm_table_id, page_number, table_type = doc_tup
    deal_id = '_'.join([project_id, url_id])
    if 1:#try:
        ######################################################
        celldata_db_path = os.path.join(output_path, company_id, 'cell_data', doc_id, norm_table_id)
        cmd = 'rm -rf %s' %(celldata_db_path)
        os.system(cmd)
        ############################################ 
        cmd = "mkdir -p %s" %(celldata_db_path)
        os.system(cmd)
        ############################################
        if 1:
         row_cmdict = {}
         xml_cdict = {}
         xml_dict = {}
         for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                section_type = cell_info.get('section_type', '')
                row, col = map(int, cell_id)
                #print cell_id, cell_info['text_lst']
                if section_type in ['HGH', 'GV']:
                    if section_type == 'HGH':
                        if row not in xml_cdict:
                            xml_cdict[row] = [cell_id]
                    else:
                        if row in xml_cdict:
                            xml_cdict[row].append(cell_id)
                    #print [cell_id, section_type, cell_info['text_lst']]
                    #print cell_info['text_ids']
                    #print '*'*100
                    if company_id=='50_6' and norm_table_id=='159' and cell_id == (26, 1):
                        cell_info['text_lst'] = [u'-davon aus Leistungen EUR 523.729,64 (i. Vj. EUR 594.172,22)']
                        cell_info['text_ids'] = cell_info['text_ids'][::-1]
                txml_ids = cell_info.get('text_ids', [])
                for ii, txid in enumerate(txml_ids):
                    txml_ids[ii] = txid.replace('$', '#')
                xml_id = '#'.join(txml_ids)
                if xml_id not in xml_dict:
                    xml_dict[xml_id] = []
                xml_dict[xml_id].append(cell_id)
         new_uniq_xml_ids = {}       
         for xml_id, cell_ids in xml_dict.items():
             if len(cell_ids) > 1:
                for cell_id in cell_ids:
                    new_ids = []
                    for each in xml_id.split('#'):
                        #print 'before', each
                        iks = each.split('_')[1]
                        idk = int(iks.split('@')[-1]) + 1
                        iks = iks + '@' + str(idk)
                        new_id = '_'.join([each.split('_')[0], iks])
                        #print 'after', new_id
                        new_ids.append(new_id)
                    xml_id = '#'.join(new_ids)
                    new_uniq_xml_ids[cell_id] = new_ids
            
         already_done_cells = {}
        
         #print celldata
         
         for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                all_txts = []
                all_txts = cell_info.get('text_lst', [])
                if type(all_txts) != type([]):
                    all_txts = [all_txts]

                txml_ids = cell_info.get('text_ids', [])
                for ii, txid in enumerate(txml_ids):
                    txml_ids[ii] = txid.replace('$', '#')
                if cell_id in new_uniq_xml_ids:
                    txml_ids = new_uniq_xml_ids[cell_id]
                cell_info['text_ids'] = txml_ids
                colspan = int(cell_info.get('colspan', 1))
                rowspan = int(cell_info.get('rowspan', 1))
                cell_info['colspan'] = colspan
                cell_info['rowspan'] = rowspan
                section_type = cell_info.get('section_type', '')
                #print 'orginal', section_type, all_txts
                for p, txt in enumerate(all_txts):
                    if isinstance(txt, unicode):
                        continue 
                        #mtxt = convob.convertNonAsciiToHTMLEntities(txt)
                        #mtxt = txt.encode('utf-8')
                        #all_txts[p] = mtxt
                        #print section_type, cell_info['text_ids'], [ p, txt, mtxt ], isinstance(txt, unicode) 
                    else:
                        try: 
                           mtxt = txt.decode('utf-8')
                        except:
                           mtxt = txt.decode('utf-8', 'ignore')
                        if mtxt:
                           all_txts[p] = mtxt
                 
                cell_info['text_lst'] = all_txts[:] 
                tm_ar = []
                if section_type != 'GV' and src_lan and table_type != 'Sentence':
                    #print 'INPUT', all_txts
                    myop  = []
                    for all_txt in all_txts:
                        if all_txt in translated_dict:  
                           myop.append(translated_dict[all_txt])
                        else:
                           myop = []
                           break
                    #print all_txts
                    if not myop:
                       #print 'all', all_txts  
                       ojson = sobj.send2(all_txts, 'TEXT', src_lan, 5)
                       #print 'ojson', ojson
                       op = ojson['o']
                       for i, all_txt in enumerate(all_txts):
                           translated_dict[all_txt] = op[i]

                       all_txts = ojson['o']
                    else:
                       #print ' From memory: ', myop  
                       all_txts  = myop[:]

                    #print ' POST TRANSLATION: ', all_txts 
                    #for txt in all_txts:
                    #    if isinstance(txt, unicode):
                    #        txt = txt.encode('utf-8')
                    #    tm_ar.append(txt)
                    #cell_info['text_lst'] = tm_ar[:]
                    cell_info['text_lst'] = all_txts[:]
                else:
                    #print 'ELSE: ',all_txts,  cell_info['text_ids']
                    pass
                    '''   
                    for p, txt in enumerate(all_txts):
                        if isinstance(txt, unicode):
                            txt = txt.encode('utf-8')
                        all_txts[p] = txt
                    
                    cell_info['text_lst'] = all_txts[:] 
                    '''   
             

                all_txts = cell_info['text_lst']
                for p, txt in enumerate(all_txts):
                    if isinstance(txt, unicode):
                        mtxt = convob.convertNonAsciiToHTMLEntities(txt)
                        all_txts[p] = mtxt
                        #print 'AFTER TRANS: ', section_type, cell_info['text_ids'], [ p, txt, mtxt ], isinstance(txt, unicode) 
                row, col = map(int, cell_id)
                if section_type == 'HGH' and xml_cdict.get(row, []):
                    values_ar = xml_cdict[row][1:]
                    txt = ' '.join(all_txts)
                    matches_values = get_cur_prev_value_from_hgh(txt) 
                    if matches_values and len(matches_values) == 2 and len(values_ar) in [2, 4, 3]:
                        it_flg = 0
                        for pmid, values_id in enumerate(values_ar):
                            if values_id not in already_done_cells:
                                if len(values_ar) == 4:
                                    if pmid%2 != 0:
                                        if it_flg:
                                            vv = matches_values[-1]
                                        else:
                                            vv = matches_values[0]
                                        ccc = ''.join(value_dict[values_id].get('text_lst', []))
                                        if not ccc.strip():
                                            value_dict[values_id]['text_lst'] = [vv]
                                            already_done_cells[values_id] = 1
                                            it_flg = 1
                                elif len(values_ar) == 3:
                                    if pmid == 1:
                                        vv = matches_values[0]
                                    elif pmid == 2:
                                        vv = matches_values[-1]
                                    else:
                                        continue
                                    ccc = ''.join(value_dict[values_id].get('text_lst', []))
                                    if not ccc.strip():
                                        value_dict[values_id]['text_lst'] = [vv]
                                        already_done_cells[values_id] = 1
                                else:
                                    if pmid == 0:
                                        vv = matches_values[0]
                                    else:
                                        vv = matches_values[-1]
                                    ccc = ''.join(value_dict[values_id].get('text_lst', []))
                                    if not ccc.strip():
                                        value_dict[values_id]['text_lst'] = [vv]
                                        already_done_cells[values_id] = 1
                                    
                cell_info['text_lst'] = all_txts[:] 
                #print '*'*100
                value_dict[cell_id] = cell_info
            celldata[key] = value_dict
        #celldata.keys() 
        #sys.exit()
        ############################################
        lmdb_obj.write_to_lmdb(celldata_db_path, celldata, celldata.keys()) ##### Store Cell Data
        ################################################################
        row_col_dict2 = {}
        value_table_id_map = {}
        value_ref_map_all = {} 
        for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                section_type = cell_info.get('section_type', '')
                if not section_type:
                    section_type = ''
                text_lst = cell_info.get('text_lst', [])
                txt = ' '.join(text_lst)
                
                xml_ids = cell_info.get('text_ids', [])
                row, col = cell_id
                #print '<hr>', cell_info
                #print [row, col, section_type, txt, xml_ids]
                colspan = int(cell_info.get('colspan', 1))
                rowspan = int(cell_info.get('rowspan', 1))
                if row not in row_col_dict2:
                    row_col_dict2[row] = {}
                #txobj = translator.translate(txt, src="ja", dest="en") 
                #txt = txobj.text
                xml_id = '#'.join(filter(lambda x:x.strip(), xml_ids[:]))
                #print [xml_id, section_type, txt]
                row_col_dict2[row][col] = (row, col, txt, section_type, colspan, rowspan, section_type, xml_id)
                value_table_id_map[xml_id] = norm_table_id
                for xxid in xml_ids:
                    value_ref_map_all[xxid] = xml_id
        data_ar = []
        all_rows = row_col_dict2.keys()
        all_rows.sort()
        for row in all_rows:
            all_cols = row_col_dict2[row].keys()
            all_cols.sort()
            tmp_data_ar = []
            for col in all_cols:
                tmp_data_ar.append(row_col_dict2[row][col])
            data_ar.append(tmp_data_ar[:])
        #iusr, nusr = get_analyst_name(company_id, norm_table_id)
        iusr, nusr = '', ''
        html_str = display_table(data_ar, norm_table_id, doc_id, iusr, nusr, page_number) 
        #############################################################################
        new_data_ar = normalise_colspan(data_ar)
        normalised_hyp_data = normalise_rowspan(new_data_ar)   ## normalized data in form of triplet
        row_col_lmdb_path = os.path.join(output_path, company_id, 'triplet_init')
        if not os.path.exists(row_col_lmdb_path):
            cmd = "mkdir -p %s" %(row_col_lmdb_path)
            os.system(cmd)
        trip_lmdb_path = os.path.join(row_col_lmdb_path, norm_table_id)
        env = lmdb.open(trip_lmdb_path, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
            txn.put('T', str(normalised_hyp_data))
        #######################################################
        return [doc_id, norm_table_id, html_str, 1, '', '']
    else:
        return [doc_id, norm_table_id, '', 0, 'ME', str(e)]

def get_clean_value(svalue):
    svalue = ''.join(svalue.strip().split())
    svalue = svalue.replace(';', '').replace('$ ', '').replace('$', '').replace('%', '').replace('&nbsp', '').replace('..', '.')
    if ('(' in svalue and ')' in svalue):
        ast = 'abcdefghijklmnopqrstuvwxyz'
        for e in ast:
            sb = '('+e+')'
            ss = '('+e.upper()+')'
            svalue = svalue.replace(ss, '')
            svalue = svalue.replace(sb, '')
    flip_sign = ''
    if ('(' in svalue and ')' in svalue) or (svalue.startswith('-') > 0):
        flip_sign = '-1'
    svalue = svalue.replace('&#8211', '').replace('\xe2\x80\x93', '').replace('&#8364', '').replace('&#8208', '').replace('&#8722', '').replace('&#8212', '').replace('&#160', '').replace('&#8213', '').replace('&#402', '')
    dot_count = 0
    new_string = ''
    for e in svalue:
        e = e.strip()
        if e in '0123456789':
           new_string += e
        elif e in '.':
           new_string += e
           dot_count = dot_count + 1
        else:
           continue
    if (dot_count > 1):
       return 0
    mul_factor = 1
    if flip_sign:
       mul_factor = -1
    try:
         if (dot_count == 1):
            new_num = mul_factor*float(new_string)
         else:
            new_num = mul_factor*int(new_string)
    except:
         new_num = 0
    return new_num


def form_search_data_structure(data_ar, table_id, doc_id):
    line_item_map = {} 
    line_item_map_ref = {} 
    value_to_line_item_map = {}
    row_id = -1
    row_col_map_dict = {}
    for row_idx, row_data_elm in enumerate(data_ar):
        cons_val = {} 
        for col_idx, row_col_elm in enumerate(row_data_elm):     
            if row_col_elm[7] in cons_val: continue 
            if (row_col_elm[3] == 'GV'):
               if table_id not in row_col_map_dict:
                    row_col_map_dict[table_id] =[]
               clean_value = get_clean_value(row_col_elm[2])
               row_col_map_dict[table_id].append({ (col_idx, 'COL_'+str(col_idx)): (clean_value, row_col_elm[7]) , (row_idx, 'ROW_'+str(row_idx)): (clean_value, row_col_elm[7]) } )
               #print '======================='
               #print row_data_elm
               #print row_col_elm    
               #print 'COL_'+str(col_idx), 'ROW_'+str(row_idx), [ row_col_elm ] 
    #sys.exit()
    for row_idx, row_data_elm in enumerate(data_ar):
        row_id = row_id + 1
        col_id = -1
        all_hghs_txt = []
        all_hghs_xmls_ids = []
        for row_col_elm in row_data_elm:     
            if row_col_elm[7] in all_hghs_xmls_ids: continue
            if (row_col_elm[3] == 'HGH'):
               all_hghs_txt.append(row_col_elm[2])
               all_hghs_xmls_ids.append(row_col_elm[7])

        #print 'all_hghs_txt: ', all_hghs_txt 
        hgh_text = ' '.join(all_hghs_txt)
        hgh_text = ' '.join(hgh_text.strip().split())
        hgh_text_xmlids = '#'.join(all_hghs_xmls_ids) 
        if (hgh_text, hgh_text_xmlids) not in line_item_map:
           line_item_map[(hgh_text, hgh_text_xmlids)] = []
        if hgh_text_xmlids not in line_item_map_ref:
           line_item_map_ref[hgh_text_xmlids] = []

        value_count = 0     
        cons_val = {} 
        for col_idx, row_col_elm in enumerate(row_data_elm):     
            if row_col_elm[7] in cons_val: continue 
            col_id = col_id + 1
            #row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = row_col_elm 
            cons_val[row_col_elm[7]] = 1
            if (row_col_elm[3] == 'GV'):
               line_item_map[(hgh_text, hgh_text_xmlids)].append([value_count, row_col_elm[2], row_col_elm[7]])
               line_item_map_ref[hgh_text_xmlids].append([value_count, row_col_elm[2], row_col_elm[7]])
               clean_value = get_clean_value(row_col_elm[2])
               dtup = (table_id, row_col_elm[7])  
               value_to_line_item_map[row_col_elm[7]] = ( hgh_text, hgh_text_xmlids, value_count)
               value_count = value_count + 1
               #if table_id not in row_col_map_dict:
               #     row_col_map_dict[table_id] =[]
               #row_col_map_dict[table_id].append({ (col_idx, 'COL_'+str(col_idx)): (clean_value, row_col_elm[7]) , (row_idx, 'ROW_'+str(row_idx)): (clean_value, row_col_elm[7]) } )
                
    norm_row_col_map_dict = {}

    
    for k, vdicts in row_col_map_dict.items():
      for vdict in vdicts:
        row_cols = vdict.keys() 
        row_cols.sort()
        for row_col in row_cols:
            val_info = vdict[row_col]
            if k not in norm_row_col_map_dict:
               norm_row_col_map_dict[k] = {}
            if row_col[1] not in  norm_row_col_map_dict[k]:
                norm_row_col_map_dict[k][row_col[1]] = []              
            norm_row_col_map_dict[k][row_col[1]].append(val_info)
            #print 'NORM: ', row_col , val_info 

    #print doc_id  
    #print norm_row_col_map_dict

    all_keys = line_item_map.keys()
    for all_key in all_keys:
        if not line_item_map[all_key]:
           del line_item_map[all_key]
        else:
           val_flg = 0
           for val in line_item_map[all_key][:]:
               if val[1].strip():
                  val_flg = 1
                  break 
           if val_flg == 0:
              del line_item_map[all_key] 
    return line_item_map, value_to_line_item_map, norm_row_col_map_dict, line_item_map_ref 


def normalise_rowspan(data_ar):
    flg = 1
    while flg:
          flg = 0
          row_ind = -1  
          mod_row_col = ()
          for tmp_data in data_ar:
              row_ind = row_ind + 1
              new_tmp_data = [] 
              col_ind = -1
              for data in tmp_data:
                  col_ind = col_ind + 1
                  row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data  
                  if (rowspan > 1):
                     # go to the below cells and in the position of row_ind, add the cells with rowspan = 1
                     #current_cell_data = (row, col, txt, section_type, colspan, 1, section_type)  
                     normal_data = (row, col, txt, section_type, colspan, 1, section_type, xml_ids) 
                     mod_row_col = (row_ind, col_ind, rowspan, normal_data)
                     break
              if mod_row_col: break  
           
          if mod_row_col:
             flg  = 1
             row_ind = mod_row_col[0]
             col_ind = mod_row_col[1]
             rowspan = mod_row_col[2]
             normal_data = mod_row_col[3]
            
             first_row_flg = 0 
             #print  len(data_ar)
             for i in range(row_ind, row_ind+rowspan):
                
                 #print '<hr>row: ', i, col_ind 
                 try:
                     row_data = data_ar[i]
                     if first_row_flg == 0:
                        new_row_data = data_ar[i][0:col_ind]  + [ normal_data ] + data_ar[i][col_ind+1:] 
                     else:
                        new_row_data = data_ar[i][0:col_ind]  + [ normal_data ] + data_ar[i][col_ind:] 
                     #print '<br> Before: ', row_data
                     #print '<br> After: ', new_row_data
                     data_ar[i] = new_row_data[:]
                     first_row_flg = 1
                 except:
                    pass
    return data_ar                     

def normalise_colspan(data_ar):
    new_data_ar = []
    for tmp_data in data_ar:
        new_tmp_data = [] 
        for data in tmp_data:
            row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data   
            if colspan > 1:
               for i in range(0, colspan):
                   row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data   
                   new_data = (row, col, txt, section_type, 1, rowspan, section_type, xml_ids)
                   new_tmp_data.append(new_data)   
            else:
               new_tmp_data.append(data)   
        new_data_ar.append(new_tmp_data)
    return new_data_ar      

def display_table(data_ar, norm_table_id, docid, iusr, nusr, page_no):
    #print 'Display table: ', [ norm_table_id, docid ]  
    html_str = '<div style="width: 100%;height: 4%;text-indent: 8px;background:#f3eeee;">'
    html_str += 'TABLE ID: '+ str(norm_table_id) + '   Document ID: '+ str(docid) +'  PAGE NO: ' + str(page_no) + '  IAnalyst Name: '+iusr+'  NAnalyst Name: '+nusr
    html_str += '</div>'
    html_str += '<table border=1 style="width:100%" cellspacing="1" cellpadding="1">'
    #HGH VGH GH GV
    for tmp_data in data_ar[:]:
        html_str += '<tr>' 
        for data in tmp_data:
            row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data 
            #print 'Data inside display html: ', data 
            background_colr = '#fff'
            if section_type == 'HGH':   
                background_colr = '#ffe2c9;'
            elif section_type == 'VGH':
                background_colr = '#c9efd9;'
            elif section_type == 'GH':
                background_colr = '#e6e696;'
            elif section_type == 'GV':
                background_colr = '#9ed8f3;'
            elif section_type == 'PGH':
                background_colr = '#eab800f0;'
            elif section_type == 'PVGH':
                background_colr = '#89deabe0;'
            elif section_type == 'GFT':
                background_colr = '#decece;'
            #html_str += '<td colspan='+str(colspan)+' rowspan='+str(rowspan)+'>'+txt+'<sub><font color=blue>'+str(section_type)+'</font></sub>'+'<sup><font color=red>'+str(rowspan)+'_'+str(colspan)+'</font><sup>'+'</td>' 
         
            if not txt.strip():
                section_type    = str(section_type)
            #print 'html: ', [ section_type, txt ] 
            html_str += '<td section_type='+str(section_type)+' style="background:'+background_colr+'" colspan='+str(colspan)+' rowspan='+str(rowspan)+'><span id="'+xml_ids+'" table_id="'+str(norm_table_id)+'" id="'+xml_ids+'">'+txt+'</span></td>'
        html_str += '</tr>' 
    html_str += '</table>'
    return html_str 

if __name__ == "__main__":
    doc_ids = []
    company_id = sys.argv[1]
    #doc_lst = []
    #f = open('doc_lst.txt', 'r')
    #all_doc = f.readlines()
    #doc_ids = '#'.join(map(lambda x:x.split('\n')[0], all_doc))
    if len(sys.argv) == 4:
        doc_str = sys.argv[3]
        src_lang = sys.argv[2]
        doc_ids = [e for e in doc_str.split('#') if e.strip()]
        generate(company_id, doc_ids, src_lang)
    else:
        src_lang = sys.argv[2]
        generate(company_id, [], src_lang)
