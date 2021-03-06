import os, sys, copy
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
import  traceback

def generate(company_id, tables_list=None):

    project_id, url_id = company_id.split('_')

    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)

    for doc_tup in norm_res_list:

        doc_id, page_number, norm_table_id = map(lambda x:str(x), doc_tup)

        #if tables_list and norm_table_id not in tables_list: continue # Only selected tables

        ktup = (doc_id, norm_table_id)

        all_doc_table_to_process.append(ktup)
    lmdb_folder = os.path.join(output_path, company_id) 
    if not os.path.exists(lmdb_folder):
        os.mkdir(lmdb_folder)
    doc_table_html_dict = {}
    #print company_id, "TOTAL TABLES", len(norm_res_list)
    #print company_id, "SELECTED TABLES", len(all_doc_table_to_process)
    #for x in all_doc_table_to_process:
    #    generate_map_ds(company_id, x)
    #sys.exit()
    res = pprocess.pmap(lambda x:generate_map_ds(company_id, all_doc_table_to_process[x]), range(0, len(all_doc_table_to_process)), 4)
    table_id_xml_bbox_dict = {}
    #######################################
    for (ktup, rdict) in res:
        doc_id, table_id = ktup
        if rdict:
            table_id_xml_bbox_dict[table_id] = str(rdict)
    if table_id_xml_bbox_dict:
        fname = os.path.join(lmdb_folder, 'xml_bbox_map')
        if not tables_list: # Running for all tables, hence remove old data and create new
            cmd = 'rm -rf %s' %(fname)
            os.system(cmd)
        env = lmdb.open(fname, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
            for k, v in table_id_xml_bbox_dict.items():
                txn.put('RST:'+k, v)
        print "done"


def generate_ret(company_id, tables_list=None):

    project_id, url_id = company_id.split('_')

    all_doc_table_to_process = []
    norm_res_list = sObj.slt_normresids(project_id, url_id)

    for doc_tup in norm_res_list:

        doc_id, page_number, norm_table_id = map(lambda x:str(x), doc_tup)

        if tables_list and norm_table_id not in tables_list: continue # Only selected tables

        ktup = (doc_id, norm_table_id)

        all_doc_table_to_process.append(ktup)
    lmdb_folder = os.path.join(output_path, company_id) 
    if not os.path.exists(lmdb_folder):
        os.mkdir(lmdb_folder)
    doc_table_html_dict = {}
    #print company_id, "TOTAL TABLES", len(norm_res_list)
    #print company_id, "SELECTED TABLES", len(all_doc_table_to_process)
    #for x in all_doc_table_to_process:
    #    generate_map_ds(company_id, x)
    #sys.exit()
    res = pprocess.pmap(lambda x:generate_map_ds(company_id, all_doc_table_to_process[x]), range(0, len(all_doc_table_to_process)), 4)
    table_id_xml_bbox_dict = {}
    #######################################
    for (ktup, rdict) in res:
        doc_id, table_id = ktup
        if rdict:
            table_id_xml_bbox_dict[table_id] = rdict
    return table_id_xml_bbox_dict


def print_exception():
    formatted_lines = traceback.format_exc().splitlines()
    for line in formatted_lines:
        print line


def generate_map_ds(company_id, doc_tup):
    project_id, url_id = company_id.split('_')
    doc_id, table_id = doc_tup
    Xml_bb_dict = {}
    try:
        Xml_bb_dict = Xml_Cell_Obj.get_cell_bbox_data(project_id, url_id, doc_id, table_id)
    except:
        Xml_bb_dict = {}
    return (doc_tup, Xml_bb_dict)

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

def display_table(data_ar, norm_table_id):
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
    html_str += 'TABLE ID: '+ str(norm_table_id) + '  PAGE NO: ' + str(page_no)
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
            #html_str += '<td colspan='+str(colspan)+' rowspan='+str(rowspan)+'>'+txt+'<sub><font color=blue>'+str(section_type)+'</font></sub>'+'<sup><font color=red>'+str(rowspan)+'_'+str(colspan)+'</font><sup>'+'</td>' 
         
            if not txt.strip():
                section_type    = str(section_type) 
            #print [norm_table_id, txt, section_type] 
            html_str += '<td section_type='+str(section_type)+' style="background:'+background_colr+'" colspan='+str(colspan)+' rowspan='+str(rowspan)+'><span id="'+xml_ids+'" table_id="'+str(norm_table_id)+'" id="'+xml_ids+'">'+txt+'</span></td>'
        html_str += '</tr>' 
    html_str += '</table>'
    return html_str 

if __name__ == "__main__":
    company_id = sys.argv[1]
    if len(sys.argv) > 2:
        tables_list = set(sys.argv[2].split('#'))
    else:
        tables_list = None
    generate(company_id, tables_list)
