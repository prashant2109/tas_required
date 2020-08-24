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

def insert_update_table_report(company_id, table_ids):
    from getCompanyName_machineId import getCN_MID
    getCompanyName_machineId = getCN_MID()
    company_name, machine_id = getCompanyName_machineId[company_id]
    model_number = '1'
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)
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
    for (doc_id, table_id) in table_ids:
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

def generate(company_id, given_doc_ids=None):
    project_id, url_id = company_id.split('_')
    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)
    for doc_tup in norm_res_list:
        doc_id, page_number, norm_table_id = map(lambda x:str(x), doc_tup)
        #if norm_table_id not in ['806', '801']:continue
        ktup = (doc_id, norm_table_id)
        #hf = os.path.join(output_path, company_id, 'Table_Htmls', '%s.html'%(norm_table_id)) 
        #if os.path.exists(hf):continue
        if given_doc_ids:
            if doc_id in given_doc_ids:
                #if norm_table_id != '2904':continue
                all_doc_table_to_process.append(ktup)
            continue
        else:
            all_doc_table_to_process.append(ktup)
    #print all_doc_table_to_process;sys.exit()
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
    doc_table_html_dict = {}
    
    #for x in all_doc_table_to_process:
    #    print x
    #    f = generate_map_ds(company_id, x)
    #sys.exit()
    res = pprocess.pmap(lambda x:generate_map_ds(company_id, all_doc_table_to_process[x]), range(0, len(all_doc_table_to_process)), 4)
    project_id, url_id = company_id.split('_')
    table_id_xml_bbox_dict = {}
    #######################################
    error_list = []
    #for [doc_id, table_id, each_html_str, flg] in res:
    for rf in res:
        if len(rf) != 6:continue
        doc_id, table_id, each_html_str, flg, sflg, er_str = rf
        if not flg:
            st = '\t'.join([doc_id, table_id, sflg, er_str])
            error_list.append(st)
            continue
        if doc_id not in doc_table_html_dict:
            doc_table_html_dict[doc_id] = []
        try:
            Xml_bb_dict = Xml_Cell_Obj.get_cell_bbox_data(project_id, url_id, doc_id, table_id)
            table_id_xml_bbox_dict[table_id] = str(Xml_bb_dict)
        except Exception as e:
            st = '\t'.join([doc_id, table_id, 'BBOX', str(e)])
            error_list.append(st)
        doc_table_html_dict[doc_id].append((table_id, each_html_str))
    
    #####################################
    for doc_id, table_html_str_list in doc_table_html_dict.items():
        doc_path    =  os.path.join(doc_html_path, str(doc_id))
        if not os.path.exists(doc_path):
            os.mkdir(doc_path)
        html_str = '<html><body>'
        t_html_str = '<html><body>'
        for (table_id, each_html_str) in table_html_str_list:
            html_str += '<hr>' + each_html_str + '<hr>'
            t_html_str += '<hr><div id="table-'+str(table_id)+'" class="table-container">' + each_html_str + '</div><hr>'
            tab_path = os.path.join(table_html_path, str(table_id)+'.html')
            ftab = open(tab_path, 'w')
            ftab.write('<html><body>'+each_html_str+'</body></html>')
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
        
    msg = insert_update_table_report(company_id, all_doc_table_to_process)
    #print table_id_xml_bbox_dict     
    if not given_doc_ids:
        fname = os.path.join(lmdb_folder, 'xml_bbox_map')
        cmd = 'rm -rf %s' %(fname)
        os.system(cmd)
        env = lmdb.open(fname, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
            for k, v in table_id_xml_bbox_dict.items():
                txn.put('RST:'+k, v)
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
 


def generate_map_ds(company_id, doc_tup):
    project_id, url_id = company_id.split('_')
    doc_id, norm_table_id = doc_tup
    deal_id = '_'.join([project_id, url_id])
    try:
        celldata = Mobj.run_process(norm_table_id, deal_id)
    except Exception as e:
        return [doc_id, norm_table_id, '', 0, 'CELL DICT', str(e)]
    #celldata = StlyObj.get_font_bold_dict(celldata, project_id, url_id, doc_id)
    try:
        celldata = StlyObj.get_font_bold_dict(celldata, project_id, url_id, doc_id)
    except Exception as e:
        return [doc_id, norm_table_id, '', 0, 'FONT', str(e)]
    ##########################################
    if 1:#try:
        ######################################################
        celldata_db_path = os.path.join(output_path, company_id, 'cell_data', doc_id, norm_table_id)
        cmd = 'rm -rf %s' %(celldata_db_path)
        os.system(cmd)
        ############################################ 
        cmd = "mkdir -p %s" %(celldata_db_path)
        os.system(cmd)
        ############################################
        if 0:
         for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                all_txts = cell_info.get('text_lst', [])
                for kl, txt in enumerate(all_txts):
                    print [txt]
                    txobj = translator.translate(txt, src="ja", dest="en") 
                    all_txts[kl] = txobj.text
                cell_info['text_lst'] = all_txts[:]
                value_dict[cell_id] = cell_info
            celldata[key] = value_dict
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
                txt = ' '.join(cell_info.get('text_lst', []))
                xml_ids = cell_info.get('text_ids', [])
                row, col = cell_id
                #print '<hr>', cell_info
                #print [row, col, section_type, txt, xml_ids]
                colspan = cell_info.get('colspan', 1)
                rowspan = cell_info.get('rowspan', 1)
                if row not in row_col_dict2:
                    row_col_dict2[row] = {}
                #txobj = translator.translate(txt, src="ja", dest="en") 
                #txt = txobj.text
                row_col_dict2[row][col] = (row, col, txt, section_type, colspan, rowspan, section_type, '#'.join(xml_ids))
                value_table_id_map['#'.join(xml_ids)] = norm_table_id
                for xml_id in xml_ids:
                    value_ref_map_all[xml_id] = '#'.join(xml_ids) 
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
        iusr, nusr = get_analyst_name(company_id, norm_table_id)
        html_str = display_table(data_ar, norm_table_id, doc_id, iusr, nusr) 
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
    #except Exception as e:
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

def display_table(data_ar, norm_table_id, docid, iusr, nusr):
    page_no = ''
    if data_ar:
        #page_no = data_ar[0][0][-1].split('#')[0].split('_')[-1]
        for tmp_data in data_ar:
            for data in tmp_data:
                row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data  
                page_no = str(xml_ids.split('#')[0].split('_')[-1]).strip()
                if page_no:break
            if page_no:break
    html_str = '<div style="width: 100%;height: 4%;text-indent: 8px;background:#f3eeee;">'
    html_str += 'TABLE ID: '+ str(norm_table_id) + '   Document ID: '+ str(docid) +'  PAGE NO: ' + str(page_no) + '  IAnalyst Name: '+iusr+'  NAnalyst Name: '+nusr
    html_str += '</div>'
    html_str += '<table border=1 style="width:100%" cellspacing="1" cellpadding="1">'
    #HGH VGH GH GV
    for tmp_data in data_ar[:]:
        html_str += '<tr>' 
        for data in tmp_data:
            row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data  
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
            #print [norm_table_id, txt, section_type] 
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
    if len(sys.argv) == 3:
        doc_str = sys.argv[2]
        doc_ids = doc_str.split('#')
        print doc_ids
        generate(company_id, doc_ids)
    else:
        generate(company_id)
