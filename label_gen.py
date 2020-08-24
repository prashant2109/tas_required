import read_user_match_table as read_user_match_table
robj = read_user_match_table.Index()
import os, sys, copy
import shelve, time
import sets, commands, lmdb
import hashlib
from difflib import SequenceMatcher
import read_norm_cell_data as Slt_normdata
sObj = Slt_normdata.Slt_normdata()
from db.webdatastore import webdatastore
import report_year_sort as report_year_sort
import slt_map_sectiontype as slt_map_sectiontype

class Delta(object):
    def __init__(self):
        self.output_path = '/var/www/html/fundamentals_intf/output/'
        self.lmdb_obj    = webdatastore()
        self.global_delta_score = {}
        self.Mobj = slt_map_sectiontype.Map_sectiontype()
        self.value_to_txt_dict = {}
        self.txt_to_value_ref_map = {}
        self.doc_table_html_dict = {}
        self.cell_data_path = '' 
        pass

    def get_phs(self, company_name):
        fname = os.path.join(self.output_path, 'company_client_ph_meta_info.slv')
        Acdict = self.lmdb_obj.read_from_shelve(fname)
        cdict = Acdict[company_name]
        training_phs = cdict.get('tas_phs', [])
        all_phs = cdict['all_phs']
        return training_phs, all_phs


    def get_data(self, company_id):
        table_match_dict = robj.processdelta(company_id)
        deal_id = tuple(company_id.split('_'))
        import read_slt_info as read_slt_info
        upObj = read_slt_info.update(deal_id) 
        docname_docid_lst = upObj.get_documentName_id()
        ph_doc_info_dict = {}
        doc_ph_info = {}
        for (doc_name, doc_id) in docname_docid_lst:
            doc_sp_lst = doc_name.split('_') 
            ph = ''
            if len(doc_sp_lst) == 2:
                ph = doc_sp_lst[-1]
            if len(doc_sp_lst) == 4:
                ph = doc_sp_lst[-2] + doc_sp_lst[-1]
            ph = ph.replace('AR', 'FY')
            ph_doc_info_dict[ph] = (ph, doc_name, doc_id)
            doc_ph_info[doc_id] = ph
            
        sorted_ph_lst = report_year_sort.year_sort(ph_doc_info_dict.keys())
        len_phs = len(sorted_ph_lst)
        doc_sorted_lst = [ph_doc_info_dict[ph][-1] for ph in sorted_ph_lst]
        #print sorted_ph_lst
        #print doc_sorted_lst
        #sys.exit()
        new_table_match_dict = {}
        for doc_pair, pair_ars in table_match_dict.items():
            i1 = doc_sorted_lst.index(doc_pair[0])
            i2 = doc_sorted_lst.index(doc_pair[1])
            print 'doc_pair: ', doc_pair
            if (i1 < i2):
               new_table_match_dict[doc_pair] = pair_ars[:]
            else:
               print 'rev: ', doc_pair, ' == ', i1, i2 
               new_pair_ars = []
               for pair in pair_ars:
                   new_pair_ars.append((pair[1], pair[0]))
               new_table_match_dict[(doc_pair[1], doc_pair[0])] = new_pair_ars[:]    
        fname = os.path.join(self.output_path, company_id+'_table_delta.slv')
        sh = shelve.open(fname, 'n')
        sh['data'] = new_table_match_dict
        sh.close()
        return
       
    def read_delta_data(self, company_id):
        fname = os.path.join(self.output_path, company_id+'_table_delta.slv')
        sh = shelve.open(fname, 'r')
        table_match_dict = sh['data']
        sh.close()  
        return table_match_dict
            
    def get_cell_dict(self, norm_path):
        sh = shelve.open(norm_path)
        celldata = sh.get('celldata', {})
        return celldata

    def get_unique_id(self, my_string):
        m = hashlib.md5()
        try:
            m.update(my_string.encode('utf-8'))
        except:
            m.update(my_string)
        return m.hexdigest()

    def get_xml_id(self, project_id, url_id, norm_table_id):
        #norm_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh' %(project_id, url_id, norm_table_id)
        #celldata = self.get_cell_dict(norm_path)
        deal_id = '_'.join([project_id, url_id])
        #print [deal_id, norm_table_id]
        celldata = self.Mobj.run_process(norm_table_id, deal_id)
        xml_ids = [] 
        for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                section_type = cell_info.get('section_type', '')
                if not section_type:
                    section_type = ''
                text_list = cell_info.get('text_lst', [])
                text_ids = cell_info.get('text_ids', [])
                #print cell_info
                #print text_list
                #print text_ids
                if not text_ids:continue
                for i, txt in enumerate(text_list):
                    try:
                        xml_id = text_ids[i]
                    except:
                        continue     
                    if not xml_id.strip():continue
                    xml_ids.append(xml_id)
        return xml_ids 

    def display_table(self, data_ar, norm_table_id):
        html_str = '<table border=1>'
        for tmp_data in data_ar:
            html_str += '<tr>' 
            for data in tmp_data:
                row, col, txt, section_type, colspan, rowspan, section_type, xml_ids = data   
                #html_str += '<td colspan='+str(colspan)+' rowspan='+str(rowspan)+'>'+txt+'<sub><font color=blue>'+str(section_type)+'</font></sub>'+'<sup><font color=red>'+str(rowspan)+'_'+str(colspan)+'</font><sup>'+'</td>' 
                html_str += '<td colspan='+str(colspan)+' rowspan='+str(rowspan)+'><span table_id="'+str(norm_table_id)+'" id="'+xml_ids+'">'+txt+'</span></td>'
            html_str += '</tr>' 
        html_str += '</table>' 
        return html_str 

    def normalise_colspan(self, data_ar):
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
             

    def normalise_rowspan(self, data_ar):
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
                 for i in range(row_ind, row_ind+rowspan):
                     #print '<hr>row: ', i, col_ind 
                     row_data = data_ar[i]
                     if first_row_flg == 0:
                        new_row_data = data_ar[i][0:col_ind]  + [ normal_data ] + data_ar[i][col_ind+1:] 
                     else:
                        new_row_data = data_ar[i][0:col_ind]  + [ normal_data ] + data_ar[i][col_ind:] 
                     #print '<br> Before: ', row_data
                     #print '<br> After: ', new_row_data
                     data_ar[i] = new_row_data[:]
                     first_row_flg = 1
        return data_ar                     
                                     

    def delta_result_parser(self, opstr, len_txt1):
        opstr_sp = opstr.split('$')
        eq_len = 0.0 
        for elm in opstr_sp:
            if not elm.strip(): continue
            elm_sp = elm.split('#')
            if elm_sp[2] == '1':
               hyp_st_en = elm_sp[0].split(',')
               hyp_st = int(hyp_st_en[0])  
               hyp_en = int(hyp_st_en[1]) 
               eq_len += hyp_en - hyp_st 
        perc = (eq_len*100)/len_txt1 
        return perc    
                     
 

    def find_delta_relationship_maps_old(self, line_item_map_hyp, line_item_map_ref):

                  
        forward_dict = {} 
        for hyp_item in line_item_map_hyp.keys():
            txt1 = hyp_item[0].lower()
            len_txt1 = len(txt1)
            #if not txt1.strip(): continue 
            #print '==========================================='
            #print 'txt1: ', [ txt1 ], hyp_item
            vals1 =  line_item_map_hyp[hyp_item]
            #sys.exit()    
            score_ar = []
            for ref_item in line_item_map_ref.keys():
                vals2 =  line_item_map_ref[ref_item]
                txt2 = ref_item[0].lower() 
                #if not (len(vals1) == len(vals2)):  continue           # check during system optimisation - ideally we would want to do this - Avinash 
                #if not txt2.strip(): continue
                #print 'txt2: ', [ txt2 ], ref_item
                if (not txt1.strip()) and (not txt2.strip()):
                   score_ar.append((100, ref_item[0], ref_item[1]))
                   continue
                else:
                   if not txt1.strip(): continue
                   if not txt2.strip(): continue 

                #print ' === ', self.global_delta_score.keys() 
                if (txt1, txt2) in self.global_delta_score:
                   scr = self.global_delta_score[(txt1, txt2)]
                   #print 'gscr: ', scr 
                else:
                    subset_flg = 0
                    s = SequenceMatcher(None, txt1, txt2)
                    if ((txt1 in txt2) or (txt2 in txt1)):
                       subset_flg = 1
                    elif (s.ratio()*100 < 30): 
                        #print [txt1, txt2, s.ratio()*100] , ' continue '
                        continue
     
                    #print 'subset_flg: ', subset_flg 
                    #print 'scr: ', s.ratio()*100 
                    #print ' txt2: ', [ txt2 ]
                    hexfile_str = hyp_item[0]+hyp_item[1]+ref_item[0]+ref_item[1] 
                    hexfile_str = self.get_unique_id(hexfile_str)
                    fname = '/tmp/%s' %(hexfile_str)
                    f = open(fname, 'w')
                    f.write(txt1+'\n') 
                    f.write(txt2+'\n')
                    f.close()
                    cmd = './find_delta_optimized '+fname
                    opstr = commands.getoutput(cmd)
                    if subset_flg: 
                       scr = 100
                    else:  
                       scr = self.delta_result_parser(opstr, len_txt1)
                    os.system('rm -rf '+fname)
                    self.global_delta_score[(txt1, txt2)] = scr
                if scr > 0:
                   score_ar.append((scr, ref_item[0], ref_item[1]))
            #print 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZscore_ar: ', score_ar  
            if score_ar:
               score_ar.sort()
               score_ar.reverse()
            forward_dict[hyp_item] = score_ar

        return forward_dict 

    #--------------- boostInterface By Krishna Keshav
    def find_delta_relationship_maps(self, line_item_map_hyp, line_item_map_ref):

        forward_dict = {}
        for hyp_item in line_item_map_hyp.keys():
            txt1 = hyp_item[0].lower()
            len_txt1 = len(txt1)
            # if not txt1.strip(): continue
            ##print '==========================================='
            ##print 'txt1: ', [ txt1 ], hyp_item
            vals1 = line_item_map_hyp[hyp_item]
            # sys.exit()
            score_ar = []
            for ref_item in line_item_map_ref.keys():
                vals2 = line_item_map_ref[ref_item]
                txt2 = ref_item[0].lower()
                # if not (len(vals1) == len(vals2)):  continue           # check during system optimisation - ideally we would want to do this - Avinash
                # if not txt2.strip(): continue
                ##print 'txt2: ', [ txt2 ], ref_item
                if (not txt1.strip()) and (not txt2.strip()):
                    score_ar.append((100, ref_item[0], ref_item[1]))
                    continue
                else:
                    if not txt1.strip(): continue
                    if not txt2.strip(): continue

                    ##print ' === ', self.global_delta_score.keys()
                if (txt1, txt2) in self.global_delta_score:
                    scr = self.global_delta_score[(txt1, txt2)]
                    ##print 'gscr: ', scr
                else:
                    subset_flg = 0
                    s = SequenceMatcher(None, txt1, txt2)
                    if ((txt1 in txt2) or (txt2 in txt1)):
                        subset_flg = 1
                    elif (s.ratio() * 100 < 30):
                        ##print [txt1, txt2, s.ratio()*100] , ' continue '
                        continue

                    ##print 'subset_flg: ', subset_flg
                    ##print 'scr: ', s.ratio()*100
                    ##print ' txt2: ', [ txt2 ]
                    hexfile_str = hyp_item[0] + hyp_item[1] + ref_item[0] + ref_item[1]
                    hexfile_str = self.get_unique_id(hexfile_str)

                    #print txt1
                    #print txt2
                    import libDeltaBasedApplicator
                    assert 'findDeltaBoost' in dir(libDeltaBasedApplicator)
                    assert callable(libDeltaBasedApplicator.findDeltaBoost)
                    cppinput = []
                    cppinput.append(txt1)
                    cppinput.append(txt2)
                    cppoutput =[]
                    libDeltaBasedApplicator.findDeltaBoost(cppinput, cppoutput)
                    opstr = cppoutput[0]
                    #print opstr
                    if subset_flg:
                        scr = 100
                    else:
                        scr = self.delta_result_parser(opstr, len_txt1)
                    #os.system('rm -rf ' + fname)
                    self.global_delta_score[(txt1, txt2)] = scr
                if scr > 0:
                    score_ar.append((scr, ref_item[0], ref_item[1]))
            ##print 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZscore_ar: ', score_ar
            if score_ar:
                score_ar.sort()
                score_ar.reverse()
            forward_dict[hyp_item] = score_ar

        return forward_dict
        
    def get_clean_value(self, svalue):
        svalue = svalue.strip()
        flip_sign = ''
        svalue = svalue.replace(';', '')
        flip_sign = ''
        if (svalue.startswith('(')>0 and svalue.endswith(')')>0) or (svalue.startswith('-') > 0) or (svalue.startswith('&#8211') > 0) or (svalue.startswith('\xe2\x80\x93')>0) or (svalue.startswith('&#8208') > 0) or (svalue.startswith('&#8722') > 0):
            flip_sign = '-'
        svalue = svalue.replace('&#8211', '').replace('\xe2\x80\x93', '').replace('&#8364', '').replace('&#8208', '').replace('&#8722', '')
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

    def form_search_data_structure(self, data_ar, table_id, doc_id):
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
                   clean_value = self.get_clean_value(row_col_elm[2])
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
                    
                    
                   clean_value = self.get_clean_value(row_col_elm[2])
                    
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



    def generate_map_ds(self, project_id, url_id, norm_table_id, doc_id, norm_tab_data_path):
        deal_id = '_'.join([project_id, url_id])
        print [deal_id, norm_table_id]
        #celldata = self.Mobj.run_process(norm_table_id, deal_id)
        celldata_db_path = os.path.join(self.output_path, deal_id, 'cell_data', doc_id, norm_table_id)
        celldata = self.lmdb_obj.read_all_from_lmdb(celldata_db_path)
        #norm_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh' %(project_id, url_id, norm_table_id)
        #celldata = self.get_cell_dict(norm_path) 
        row_col_dict2 = {}
        value_table_id_map = {}
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
                colspan = cell_info.get('colspan', 1)
                rowspan = cell_info.get('rowspan', 1)
                if row not in row_col_dict2:
                    row_col_dict2[row] = {}
                row_col_dict2[row][col] = (row, col, txt, section_type, colspan, rowspan, section_type, '#'.join(xml_ids))
                value_table_id_map['#'.join(xml_ids)] = norm_table_id

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
        #for elm in data_ar:
        #    print elm
        html_str =  self.display_table(data_ar, norm_table_id)
        self.doc_table_html_dict[doc_id].append(html_str)
        #print '<hr>ORGINALLLLLLLLLLLLL<hr>', html_str 
        new_data_ar = self.normalise_colspan(data_ar)
        html_str2 =  self.display_table(new_data_ar, norm_table_id)
        #print '<hr> Post normalise_colspan<hr>', html_str2 
        normalised_hyp_data = self.normalise_rowspan(new_data_ar)
        html_str3 =  self.display_table(normalised_hyp_data, norm_table_id)

        norm_tab_data_path_tab = os.path.join(norm_tab_data_path, norm_table_id)
        cmd = 'rm -rf %s' %(norm_tab_data_path_tab)
        os.system(cmd)
        cmd = 'mkdir -p %s' %(norm_tab_data_path_tab)
        os.system(cmd)  
     

        env = lmdb.open(norm_tab_data_path_tab, map_size=1000*1000*1000)
        with env.begin(write=True) as txn:
            xml_id_dict = {}   
            row_cnt = 0
            for row_wise in normalised_hyp_data:
                col_cnt = 0 
                for elm in row_wise:
                    mkey = str(row_cnt)+'#'+str(col_cnt)
                    ref = elm[7]
                    txn.put('ROWCOLTOREF:'+mkey, ref)
                    mval = ref 
                    if ref not in xml_id_dict:
                       xml_id_dict[ref] = [] 
                    xml_id_dict[ref].append(str(row_cnt)+'#'+str(col_cnt))
                    col_cnt = col_cnt + 1
                row_cnt = row_cnt + 1  

            for k, vs in xml_id_dict.items():
                key = hashlib.md5(k).hexdigest()
                val = '~'.join(vs) 
                txn.put('REFTOROWCOL:'+key, val)
            #deal_id = '_'.join([project_id, url_id])
            #print [deal_id, norm_table_id]
            
            total_rows = len(normalised_hyp_data)
            total_cols = len(normalised_hyp_data[0])
            print 'TotalRows ', total_rows
            print 'TotalCols ', total_cols 
            txn.put('ROWS', str(total_rows))
            txn.put('COLS', str(total_cols))
         
        #sys.exit()

        #print '<hr>AFTER  self.normalise_rowspan <hr>', html_str3
        #normalised_hyp_data = new_data_ar[:]
        hyp_line_item_map, hyp_value_to_line_item_map, norm_row_col_map_dict, line_item_map_ref = self.form_search_data_structure(normalised_hyp_data, norm_table_id, doc_id)
        #print norm_row_col_map_dict
        #return hyp_line_item_map, hyp_value_to_line_item_map, value_table_id_map, norm_row_col_map_dict, line_item_map_ref
        return norm_row_col_map_dict


        

    def get_back_forward_pair(self, doc_id, id_key, all_doc_pair_list, pair_list):
        for doc_pair in all_doc_pair_list:
            if doc_pair in pair_list:continue
            if id_key == 'hyp':
                if doc_pair[1] == doc_id:
                    if doc_pair not in pair_list:
                        pair_list.append(doc_pair)
                    pair_list = self.get_back_forward_pair(doc_pair[1], id_key, all_doc_pair_list, pair_list)
            if id_key == 'ref':
                if doc_pair[0] == doc_id:
                    if doc_pair not in pair_list:
                        pair_list.append(doc_pair)
                    pair_list = self.get_back_forward_pair(doc_pair[0], id_key, all_doc_pair_list, pair_list)
                
        return pair_list    
 
    def generate_table_item_map(self, company_id, company_name):
        lmdb_folder = os.path.join(self.output_path, company_id) 
        if not os.path.exists(lmdb_folder):
            os.mkdir(lmdb_folder)
        #delta_table_match_dict = self.read_delta_data(company_id)
        project_id, url_id = company_id.split('_')
        norm_res_list = sObj.slt_normresids(project_id, url_id)
        doc_ref_text_info = {}
        doc_page_dict = {}
        for doc_tup in norm_res_list:
            doc_id, page_number, norm_table_id = doc_tup
            if doc_id not in doc_page_dict:
                doc_page_dict[doc_id] = []
            doc_page_dict[doc_id].append(norm_table_id)
        #==========================================================
        doc_table_map_dict = self.get_doc_id_table_dict(company_id)
        doc_table_pair_dict = {}

        value_ref_table_map = {}

        lmdb_doc_table_pair_path = os.path.join(lmdb_folder, 'doc_table_pair')
        row_col_lmdb_path = os.path.join(self.output_path, company_id, 'row_col_map_data')
        if not os.path.exists(row_col_lmdb_path):
            os.mkdir(row_col_lmdb_path) 
        self.value_to_txt_dict = {}
        self.txt_to_value_ref_map = {}
        self.doc_table_html_dict = {}
        done_doc_table_dict = {}
        
        self.cell_data_path = os.path.join(self.output_path, company_id, 'cell_data')
        if not os.path.exists(self.cell_data_path):
            cmd = "mkdir -p %s" %(self.cell_data_path)
            os.system(cmd)

        #print delta_table_match_dict.keys()
        #sys.exit()  
        #self.get_deal_info_lmdb
        delta_table_match_list = self.get_doc_pairs(company_id, company_name)
        
        line_item_value_map_dict = {}
        cell_value_ref_line_item_map_dict = {}
        cell_value_table_id_dict = {}
        norm_tab_data_path = os.path.join(self.output_path, company_id, 'norm_table_data')             
        cmd = 'rm -rf %s' %(norm_tab_data_path)
        #os.system(cmd)
        cmd = 'mkdir -p %s' %(norm_tab_data_path)
        #os.system(cmd)
        for doc_pair in delta_table_match_list:
            for nd_doc in [doc_pair[0], doc_pair[1]]:
                if nd_doc  not in self.value_to_txt_dict:
                    self.value_to_txt_dict[nd_doc] = {}
                if nd_doc not in self.txt_to_value_ref_map:
                    self.txt_to_value_ref_map[nd_doc] = {}
                if nd_doc not in self.doc_table_html_dict: 
                    self.doc_table_html_dict[nd_doc] = []
 
            hyp_ref_forward_back_ward_dict = {}
            #doc_pair_name = doc_pair[0] + '_' + doc_pair[1]
            lmdb_fname_hyp = os.path.join(lmdb_folder, doc_pair[0])
            if not os.path.exists(lmdb_fname_hyp):
                os.mkdir(lmdb_fname_hyp)
            
            lmdb_fname_ref = os.path.join(lmdb_folder, doc_pair[1])
            if not os.path.exists(lmdb_fname_ref):
                os.mkdir(lmdb_fname_ref)
            

            all_table_ids1 = doc_page_dict.get(doc_pair[0], [])
            all_table_ids2 = doc_page_dict.get(doc_pair[1], [])
              
            pos_match_pair = []
            for i1 in all_table_ids1:
                for i2 in all_table_ids2:
                    pos_match_pair.append((i1, i2))
    
            #pair_match_table_list = delta_table_match_dict[doc_pair]

                  
            #print pair_match_table_list
            htyp_doc_id, ref_doc_id = doc_pair

              
            #ph_hyp
            #ph_ref
            #if (doc_pair != ('12', '13')): continue

            for tab_pair in pos_match_pair:
                #hyp_list = tab_pair[0], doc_pair[0]
                #ref_list = tab_pair[1], doc_pair[1]
                selected_hyp = tab_pair[0]
                selected_ref = tab_pair[1]
                #if (selected_hyp, selected_ref) != ('3169', '3108'): continue
                if (not selected_hyp) or (not selected_ref):
                   print 'Learning Error....'
                   continue
                   #sys.exit()
                allkey_dict = {}
                #if not (doc_pair[0] == '11' or doc_pair[1] == '11'):continue
                #if not (tab_pair[0] == '1925' or tab_pair[1] == '1925'):continue
                #ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map, norm_row_col_map_dict = self.generate_map_ds(project_id, url_id, selected_ref, doc_pair[1])
                #sys.exit()
                if ((doc_pair[0], selected_hyp) not in done_doc_table_dict) and selected_hyp:
                    done_doc_table_dict[(doc_pair[0], selected_hyp)] = 1
                    hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map, norm_row_col_map_dict, hyp_line_item_map_ref = self.generate_map_ds(project_id, url_id, selected_hyp, doc_pair[0], norm_tab_data_path)

                    #hyp_line_item_map (line_item -> values)
                    for k, vs in hyp_line_item_map.items():                     
                         line_key = '^!!^'.join([doc_pair[0], selected_hyp, k[1]])   
                         Mkey = hashlib.md5(line_key).hexdigest()
                         line_item_value_map_dict[Mkey] = str(vs)   
                    #hyp_value_to_line_item_map (cell_value_ref -> line_item) 
                    for k, vs in hyp_value_to_line_item_map.items():                     
                        rkey = doc_pair[0] + '^!!^' + selected_hyp + '^!!^' + k 
                        Mkey = hashlib.md5(rkey).hexdigest()
                        cell_value_ref_line_item_map_dict[Mkey] = vs[1]

                    for ref, table_id in hyp_value_tableid_map.items():
                        ref_sp = ref.split('#')
                        for r in ref_sp:
                            if doc_pair[0] not in value_ref_table_map:
                               value_ref_table_map[doc_pair[0]] = {}
                            value_ref_table_map[doc_pair[0]][r] = selected_hyp
                                        
                            rkey = doc_pair[0] + '^!!^' + r
                            Mkey = hashlib.md5(rkey).hexdigest()
                            cell_value_table_id_dict[Mkey] = selected_ref
                              
                    hyp_fname = os.path.join(lmdb_fname_hyp, selected_hyp)
                    allkey_dict[selected_hyp] = (hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map, hyp_line_item_map_ref) 
                    self.lmdb_obj.write_to_lmdb(hyp_fname, allkey_dict, allkey_dict.keys())
                    
                    lmdb_value_ref_table_path = os.path.join(lmdb_folder, 'value_ref_table_map')
                    row_col_fname = os.path.join(row_col_lmdb_path, selected_hyp) 
                    self.lmdb_obj.write_to_lmdb(row_col_fname, norm_row_col_map_dict, norm_row_col_map_dict.keys())
                    



                    # LMDB WRITE HYP
                allkey_dict = {}
                if ((doc_pair[1], selected_ref) not in done_doc_table_dict) and  selected_ref:
                    done_doc_table_dict[(doc_pair[1], selected_ref)] = 1
                    ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map, norm_row_col_map_dict, ref_line_item_map_ref = self.generate_map_ds(project_id, url_id, selected_ref, doc_pair[1], norm_tab_data_path)
                    #ref_line_item_map (line_item -> values)
                    for k, vs in ref_line_item_map.items():                     
                         line_key = '^!!^'.join([doc_pair[1], selected_ref, k[1]])   
                         Mkey = hashlib.md5(line_key).hexdigest()
                         line_item_value_map_dict[Mkey] = str(vs)   
                    #ref_value_to_line_item_map (cell_value_ref -> line_item) 
                    for k, vs in ref_value_to_line_item_map.items():                     
                        rkey = doc_pair[1] + '^!!^' + selected_ref + '^!!^' + k 
                        Mkey = hashlib.md5(rkey).hexdigest()
                        cell_value_ref_line_item_map_dict[Mkey] = vs[1]
                    #line_item_value_map_dict = {}
                    #cell_value_ref_line_item_map_dict = {}
                    #cell_value_table_id_dict = {}
                     
                    for ref, table_id in ref_value_tableid_map.items():
                        ref_sp = ref.split('#')
                        for r in ref_sp:
                            if doc_pair[1] not in value_ref_table_map:
                               value_ref_table_map[doc_pair[1]] = {}
                            value_ref_table_map[doc_pair[1]][r] = selected_ref
                            rkey = doc_pair[1] + '^!!^' + r
                            Mkey = hashlib.md5(rkey).hexdigest()
                            cell_value_table_id_dict[Mkey] = selected_ref
                             
                                
                    ref_fname = os.path.join(lmdb_fname_ref, selected_ref) 
                    allkey_dict[selected_ref] = (ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map, ref_line_item_map_ref) 
                    self.lmdb_obj.write_to_lmdb(ref_fname, allkey_dict, allkey_dict.keys())
                    # LMDB WRITE REF
                    row_col_fname = os.path.join(row_col_lmdb_path, selected_ref) 
                    self.lmdb_obj.write_to_lmdb(row_col_fname, norm_row_col_map_dict, norm_row_col_map_dict.keys())
                if selected_hyp and selected_ref:
                    if doc_pair not in doc_table_pair_dict:
                        doc_table_pair_dict[doc_pair] = []
                    table_pair = (selected_hyp, selected_ref)
                    if table_pair not in doc_table_pair_dict[doc_pair]:
                        doc_table_pair_dict[doc_pair].append(table_pair)
                    # PAIR      
                    print doc_pair 
                    print [selected_hyp, selected_ref]
                    #print 'HYP', hyp_line_item_map
                    #print 'REF', ref_line_item_map
                    print "*"*100
                    #fwd_dict = self.find_delta_relationship_maps(hyp_line_item_map, ref_line_item_map)
                    #rev_dict = self.find_delta_relationship_maps(ref_line_item_map, hyp_line_item_map) 
                    #hyp_ref_info_dict_map_dict[selected_hyp] = (hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map) 
                    #hyp_ref_info_dict_map_dict[selected_ref] = (ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map)
        #store value_ref_table_map
        #sys.exit() 
        doc_html_path = os.path.join(lmdb_folder, 'Doc_Htmls')
        if not os.path.exists(doc_html_path):
            os.mkdir(doc_html_path)

        lmdb_value_line_path = os.path.join(lmdb_folder, 'slice_cell_line_item_map')
        cmd = 'rm -rf %s' %(lmdb_value_line_path)
        os.system(cmd)
        cmd = 'mkdir -p %s' %(lmdb_value_line_path)    
        os.system(cmd)
        #line_item_value_map_dict = {}
        #cell_value_ref_line_item_map_dict = {}
        #cell_value_table_id_dict = {}
        env = lmdb.open(lmdb_value_line_path, map_size=1000*1000*1000)
        with env.begin(write=True) as txn:
             for k, v in line_item_value_map_dict.items():
                 txn.put('LINEITEMKEYS:'+k, v) 
             for k, v in cell_value_ref_line_item_map_dict.items():
                 txn.put('TABLEVALUEREFKEYS:'+k, v) 
             for k, v in cell_value_table_id_dict.items():
                 txn.put('VALUEREFS:'+k, v)  

        for doc_id in self.value_to_txt_dict.keys():
            t1_dict = self.value_to_txt_dict[doc_id]
            t2_dict = self.txt_to_value_ref_map[doc_id]
            doc_kname = os.path.join(lmdb_folder, doc_id)
            if not os.path.join(doc_kname):
                os.mkdir(doc_kname)
            lmdb_ref_txt_path = os.path.join(doc_kname, 'txt_value_ref_value_map')
            final_dict = {'value_to_txt_dict':t1_dict, 'txt_value_ref_map_dict':t2_dict}
            self.lmdb_obj.write_to_lmdb(lmdb_ref_txt_path, final_dict, final_dict.keys())
            
        lmdb_value_ref_table_path = os.path.join(lmdb_folder, 'value_ref_table_map')
        self.lmdb_obj.write_to_lmdb(lmdb_doc_table_pair_path, doc_table_pair_dict, doc_table_pair_dict.keys())
        self.lmdb_obj.write_to_lmdb(lmdb_value_ref_table_path, value_ref_table_map, value_ref_table_map.keys())
        import table_html_creation_new_all as Delta
        Hobj = Delta.Delta()
        try:
            Hobj.run(company_id)
        except:
            print "ERROR", company_id
        
    

    def get_sorted_doc_list(self, company_id):
        delta_table_match_dict = self.read_delta_data(company_id)
        #print delta_table_match_dict.keys()
        #sys.exit()
        deal_id = tuple(company_id.split('_'))
        import read_slt_info as read_slt_info
        upObj = read_slt_info.update(deal_id) 
        docname_docid_lst = upObj.get_documentName_id()
        ph_doc_info_dict = {}
        doc_ph_info = {}
        for (doc_name, doc_id) in docname_docid_lst:
            doc_sp_lst = doc_name.split('_') 
            ph = ''
            if len(doc_sp_lst) == 2:
                ph = doc_sp_lst[-1]
            if len(doc_sp_lst) == 4:
                ph = doc_sp_lst[-2] + doc_sp_lst[-1]
            ph = ph.replace('AR', 'FY')
            ph_doc_info_dict[ph] = (ph, doc_name, doc_id)
            doc_ph_info[doc_id] = ph

        lmdb_folder = os.path.join(self.output_path, company_id) 
        if not os.path.exists(lmdb_folder):
            os.mkdir(lmdb_folder)
        
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        self.lmdb_obj.write_to_lmdb(dfname, doc_ph_info, doc_ph_info.keys())

        sorted_ph_lst = report_year_sort.year_sort(ph_doc_info_dict.keys())
        len_phs = len(sorted_ph_lst)
        doc_sorted_lst = [ph_doc_info_dict[ph][-1] for ph in sorted_ph_lst]
        sorted_combination_doc_lst = []
        for i, ph in enumerate(sorted_ph_lst):
            map_doc_tup = doc_sorted_lst[i]
            if (ph[:2] != 'FY') and (i < len(sorted_ph_lst) - 2):
                next_ph = sorted_ph_lst[i+1]
                if ph[:1] in ['Q', 'H'] and next_ph[:1] in ['F']:continue
                next_doc_tup = doc_sorted_lst[i+1]
                sorted_combination_doc_lst.append((map_doc_tup, next_doc_tup))
            elif (ph[:2] == 'FY') and (i < len(sorted_ph_lst) - 2):
                next_fy = ph[:2] + str(int(ph[2:])+1)  
                if next_fy not in sorted_ph_lst:continue
                next_fy_indx = sorted_ph_lst.index(next_fy)
                next_doc_tup = doc_sorted_lst[next_fy_indx]
                sorted_combination_doc_lst.append((map_doc_tup, next_doc_tup))

        ###################################################################### 
        project_id, url_id = company_id.split('_')
        norm_res_list = sObj.slt_normresids(project_id, url_id)
        doc_page_dict = {}
        for doc_tup in norm_res_list:
            doc_id, page_number, norm_table_id = doc_tup
            if doc_id not in doc_page_dict:
                doc_page_dict[doc_id] = {}
            if page_number not in doc_page_dict[doc_id]:
                doc_page_dict[doc_id][page_number] = []
            doc_page_dict[doc_id][page_number].append(norm_table_id)
       

        cache_xml_ids = {}   
        val_cons_dict = {}             
        doc_pair_table_pair_dict = {}   
        doc_id_pairs = delta_table_match_dict.keys() 
        for doc_pair in doc_id_pairs:
            hyp_doc, ref_doc = doc_pair
            hyf_ref_lst = delta_table_match_dict[doc_pair]
            for (hyp_list, ref_list) in hyf_ref_lst:
                pg1 = hyp_list[0].split('_')[-1]  
                pg2 = ref_list[0].split('_')[-1] 
                #print doc_pair, pg1, pg2, hyp_list, ref_list 
                if (pg2 not in doc_page_dict[doc_pair[1]]) or (pg1 not in doc_page_dict[doc_pair[0]]):
                    continue
                norm_table_id_hyps = doc_page_dict[doc_pair[0]][pg1]
                norm_table_id_refs = doc_page_dict[doc_pair[1]][pg2]
                selected_hyp = ''

                if (len(norm_table_id_hyps) == 1):
                   selected_hyp = norm_table_id_hyps[0]
                else:
                    n_hyp_list = []
                    for r in hyp_list:
                        n_hyp_list += r.split('#') 
                     
                    for norm_table_id_hyp in norm_table_id_hyps:
                        if (project_id, url_id, norm_table_id_hyp) in cache_xml_ids:
                           xmlids1 = cache_xml_ids[(project_id, url_id, norm_table_id_hyp)] 
                        else:  
                           xmlids1 = self.get_xml_id(project_id, url_id, norm_table_id_hyp)
                           cache_xml_ids[(project_id, url_id, norm_table_id_hyp)] = xmlids1
                        s1 = sets.Set(xmlids1).intersection(sets.Set(n_hyp_list))
                        if list(s1):
                           selected_hyp = norm_table_id_hyp
                           break


                selected_ref = ''

                if (len(norm_table_id_refs) == 1):
                   selected_ref = norm_table_id_refs[0]
                else: 
                    n_ref_list = []
                    for r in ref_list:
                        n_ref_list += r.split('#') 
                    for norm_table_id_ref in norm_table_id_refs:
                        if (project_id, url_id, norm_table_id_ref) in cache_xml_ids:
                           xmlids1 = cache_xml_ids[(project_id, url_id, norm_table_id_ref)]
                        else:
                           xmlids1 = self.get_xml_id(project_id, url_id, norm_table_id_ref)
                           cache_xml_ids[(project_id, url_id, norm_table_id_ref)] = xmlids1

                        s1 = sets.Set(xmlids1).intersection(sets.Set(n_ref_list))
                        if list(s1):
                           selected_ref = norm_table_id_ref
                           break
               
                #print ' doc_pair: ', doc_pair  
                #print 
                if (not selected_hyp) or (not selected_ref):
                   print 'mmmm Learning Error....'
                   continue
                   #sys.exit()
                if selected_hyp and selected_ref:
                    #print [selected_hyp, selected_ref]
                    if (doc_pair[0], selected_hyp) not in doc_pair_table_pair_dict:
                        doc_pair_table_pair_dict[ (doc_pair[0], selected_hyp) ] = []
                    #if selected_hyp not in doc_pair_table_pair_dict[doc_pair]:
                    #    doc_pair_table_pair_dict[doc_pair][selected_hyp] = []
                    if (doc_pair[1], selected_ref) not in  doc_pair_table_pair_dict[ (doc_pair[0], selected_hyp)]:
                       doc_pair_table_pair_dict[ (doc_pair[0], selected_hyp) ].append((doc_pair[1], selected_ref))
                       val_cons_dict[(doc_pair[1], selected_ref)] = 1 
         
        init_values = [] 
        for k, vs in doc_pair_table_pair_dict.items():  
            if k not in val_cons_dict:
               init_values.append([k])
       

        #print len(init_values)
        #sys.exit() 
 
        flg = 1 
        while flg:
              flg = 0
              
              new_init_values = []   
              for init_val in init_values:
                  last_key = init_val[-1]
                  if last_key in doc_pair_table_pair_dict:
                     flg = 1 
                     extended_pos = doc_pair_table_pair_dict[last_key]
                     print ' extended_pos: ', extended_pos, len(extended_pos)
                     new_init_val = init_val[:]
                     for e in extended_pos:
                         new_init_values.append(init_val[:] + [ e ])
                         print len(init_val[:])+1, ' === ALL: ', len_phs 
                  else:
                         new_init_values.append(init_val[:])
              init_values = new_init_values[:]                    
                       

        '''                    
        new_init_values = []
        for init_value in init_values:
            l1 = map(lambda x:x[0]+'#'+x[1], init_value[:])
            s1 = sets.Set(l1)  
            flg = 0
            for init_value1 in init_values:
                l2 = map(lambda x:x[0]+'#'+x[1], init_value1[:])
                s2 = sets.Set(l2)
                if (s1 == s2): continue
                if s1.issubset(s2):
                   flg = 1
                   break   
            if (flg == 0):
               new_init_values.append(init_value)   
        '''                    
        ar = []          
        for init_value in init_values:
            ar.append((len(init_value), init_value))

        ar.sort()
        ar.reverse()

        print 'Total ar: ', len(ar)
        for ar_elm in ar:
            print 'Len: ', ar_elm[0], 'ELMS: ', ar_elm[1] 
        #sys.exit()

        ofname = os.path.join(lmdb_folder, 'doc_table_final_chain_pair')
        final_pair_dict = {'sorted_comb_list':sorted_combination_doc_lst, 'doc_table_chain_pair_list':new_init_values}
        self.lmdb_obj.write_to_lmdb(ofname, final_pair_dict, final_pair_dict.keys())  
        #print 'FINAL'  
        #for e in new_init_values: 
        #    print 'New List: ', e    
        #sys.exit() 
              
        #path_possibilities = [] 
        #for doc_pair in sorted_combination_doc_lst:
        #    hyp_ref_dict = doc_pair_table_pair_dict.get(doc_pair, {})
        #    if hyp_ref_dict:
        #       tmp_poss = []
        #       for k, vs in hyp_ref_dict.items():
        #           for v in vs:
        #               tmp_poss.append(( doc_pair[0], k, doc_pair[1], v)) 
        #       path_possibilities.append(tmp_poss)

        #pair_count = 1
        #for path_elm in path_possibilities:
        #     print path_elm , ' pair_count: ', pair_count, ' == ', sorted_combination_doc_lst[pair_count-1] 
        #     pair_count = pair_count + 1
        #sys.exit()        
               

                  
        #print "norm table pair", delta_table_match_dict.keys()
        #print  
        #print sorted_ph_lst
        #print
        #print doc_sorted_lst
        #print 
        #print sorted_combination_doc_lst
        #print 
    def generate_label_delta_new(self, company_id):
        lmdb_folder = os.path.join(self.output_path, company_id)
        lmdb_doc_table_pair_path = os.path.join(lmdb_folder, 'doc_table_pair')
        doc_table_pair_dict = self.lmdb_obj.read_all_from_lmdb(lmdb_doc_table_pair_path)
        new_allkey_dict = {}
        all_doc_pairs = doc_table_pair_dict.keys()
        ind1 = -1
        fwd_table_dict = {}
        rev_table_dict = {} 

        for doc_pair, table_pairs in doc_table_pair_dict.items()[:]:
            ind1 += 1
            ind2 = -1
            #if doc_pair not in [('12', '11'), ('11', '12')]: continue
            #if doc_pair not in [('26', '27')]:continue
            for table_pair in table_pairs[:]:
                    #if (table_pair[0] != '3108'): continue
                    #if (table_pair[1] != '5404'): continue
                    #if table_pair not in [('2920', '271'), ('271', '2920') ]:continue    
                    ind2 += 1
                    print ' Complete: ', ind1 , ' / ', len(all_doc_pairs), ' : ', ind2, ' / ', len(table_pairs)
                    hyp_doc_id = doc_pair[0]
                    ref_doc_id = doc_pair[1]
                    hyp_tab_id = table_pair[0]
                    ref_tab_id = table_pair[1]
                    if (hyp_doc_id, hyp_tab_id) not in  new_allkey_dict:
                        lmdb_fname_hyp = os.path.join(lmdb_folder, hyp_doc_id)
                        hyp_fname = os.path.join(lmdb_fname_hyp, hyp_tab_id) 
                        allkey_dict = self.lmdb_obj.read_all_from_lmdb(hyp_fname)
                        new_allkey_dict[(hyp_doc_id, hyp_tab_id)] =  allkey_dict[hyp_tab_id] 
                    hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map, hyp_line_item_map_ref = new_allkey_dict[(hyp_doc_id, hyp_tab_id)]
                              
                    if (ref_doc_id, ref_tab_id) not in  new_allkey_dict:
                        lmdb_fname_ref = os.path.join(lmdb_folder, ref_doc_id)
                        ref_fname = os.path.join(lmdb_fname_ref, ref_tab_id)
                        allkey_dict = self.lmdb_obj.read_all_from_lmdb(ref_fname)
                        new_allkey_dict[(ref_doc_id, ref_tab_id)] = allkey_dict[ref_tab_id]
                    ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map, ref_line_item_map_ref = new_allkey_dict[(ref_doc_id, ref_tab_id)]
                    

                    hkeys = hyp_line_item_map.keys()
                    rkeys = ref_line_item_map.keys()

                    #print hkeys, rkeys 
                    if not hkeys: continue
                    if not rkeys: continue  
                    #vals1 = hyp_line_item_map[hkeys[0]]
                    #vals2 = ref_line_item_map[rkeys[0]]
                    #if not (len(vals1) == len(vals2)):          continue  
                    if ((len(hkeys) > 10) and (len(rkeys) > 10)):
                        hkeys = map(lambda x:x[0].lower(), hkeys[:]) 
                        rkeys = map(lambda x:x[0].lower(), rkeys[:]) 
                        s1 = sets.Set(hkeys)
                        s2 = sets.Set(rkeys)
                        s_inter = s1.intersection(s2)
                        if not (s_inter): continue

                        s_inter_li = list(s_inter)
                        if ( (len(s_inter_li) == 1) and (not s_inter_li[0].strip()) ): continue  
                        print 'Common: '      , s1.intersection(s2) 
             
                    print 'KKKKKKKKK'
                    t1 = time.time()
                    #print hkeys
                    #print rkeys
                    #sys.exit()
                    fwd_dict = self.find_delta_relationship_maps(hyp_line_item_map, ref_line_item_map)
                    rev_dict = self.find_delta_relationship_maps(ref_line_item_map, hyp_line_item_map)
                    #print rev_dict   
                    t2 = time.time()
                    print 'Time Taken: ', t2 - t1
                    #print fwd_dict
                    print "*"*100
                    #print rev_dict
                    #sys.exit()
                    #print rev_dict[('Company restaurants (excluding depreciation and amortization) ~ Cost of sales', 'x1000129_88#x476_88#x552_88')]
                    #(hyp_doc_id, hyp_tab_id) -> { (ref_doc_id, ref_tab_id): fwd_dict }
                    #(ref_doc_id, ref_tab_id) -> { (hyp_doc_id, hyp_tab_id) : rev_dict }      
                    if (hyp_doc_id, hyp_tab_id) not in fwd_table_dict:
                       fwd_table_dict[(hyp_doc_id, hyp_tab_id)] = {} 
                    fwd_table_dict[(hyp_doc_id, hyp_tab_id)][(ref_doc_id, ref_tab_id)] = fwd_dict
                    if (ref_doc_id, ref_tab_id) not in rev_table_dict:
                       rev_table_dict[(ref_doc_id, ref_tab_id)] = {} 
                    rev_table_dict[(ref_doc_id, ref_tab_id)][(hyp_doc_id, hyp_tab_id)] = rev_dict
        #doc_table_pair_forward_reverse_dict = {'fwd_table_dict':fwd_table_dict, 'rev_table_dict':rev_table_dict}
        #lmdb_folder = os.path.join(self.output_path, company_id) 
        fname = os.path.join(lmdb_folder, 'doc_pair_forward_reverse')    
        #self.lmdb_obj.write_to_lmdb(fname, doc_table_pair_forward_reverse_dict, doc_table_pair_forward_reverse_dict.keys())            
        os.system('rm -rf '+fname)
        os.system('mkdir -p '+fname)
        results_dict = {} 
        all_results = {}
        for doc_tab1, vdict in fwd_table_dict.items():
            for doc_tab2, ddict in vdict.items():
                for dkey, vals in ddict.items():
                    if not vals: continue
                    mkey = doc_tab1[0]+'-'+doc_tab1[1] + '-'+ dkey[1]  
                    child_key = doc_tab2[0]+'-'+doc_tab2[1]
                    new_vals = map(lambda x:str(x[0])+'~'+x[2]+'~'+child_key, vals)
                    key1 = hashlib.md5(mkey).hexdigest()
                    key2 = hashlib.md5(child_key).hexdigest()
                    if key1 not in all_results:
                       all_results[key1] = ''  
                    all_results[key1] += key2 + '~'
                    results_dict[key1+'~'+key2] = str(new_vals)      
        

        results_dict2 = {} 
        all_results2 = {}
        for doc_tab1, vdict in rev_table_dict.items():
            for doc_tab2, ddict in vdict.items():
                for dkey, vals in ddict.items():
                    if not vals: continue
                    mkey = doc_tab1[0]+'-'+doc_tab1[1] + '-'+ dkey[1]  
                    child_key = doc_tab2[0]+'-'+doc_tab2[1]
                    new_vals = map(lambda x:str(x[0])+'~'+x[2]+'~'+child_key, vals)
                    key1 = hashlib.md5(mkey).hexdigest()
                    key2 = hashlib.md5(child_key).hexdigest()
                    if key1 not in all_results2:
                       all_results2[key1] = ''  
                    all_results2[key1] += key2 + '~'
                    results_dict2[key1+'~'+key2] = str(new_vals)      
    
 
        env = lmdb.open(fname, map_size=10*1000*1000*1000)
        with env.begin(write=True) as txn:
             for k, v in all_results.items():
                 txn.put('FWDKEYS:'+k, v) 
             for k, v in all_results2.items():
                 txn.put('REVKEYS:'+k, v) 
             for k, v in results_dict.items():
                 txn.put('FWDRESULTS:'+k, v)  
             for k, v in results_dict2.items():
                 txn.put('REVRESULTS:'+k, v)  
        #self.read_all_lmdb_data(fname)
   
    def read_all_lmdb_data(self, lmdb_folder):
        env = lmdb.open(lmdb_folder)
        with env.begin() as txn:
             cursor = txn.cursor()
             for key, value in cursor:
                 print(key, value) 
    
    def get_doc_pairs(self, company_id, company_name):
        deal_id = tuple(company_id.split('_'))
        import read_slt_info as read_slt_info
        upObj = read_slt_info.update(deal_id) 
        docname_docid_lst = upObj.get_documentName_id()
        ph_doc_info_dict = {}
        doc_ph_info = {}
        tphs, all_phs = self.get_phs(company_name)
        for (doc_name, doc_id) in docname_docid_lst:
            doc_sp_lst = doc_name.split('_') 
            ph = ''
            if len(doc_sp_lst) == 2:
                ph = doc_sp_lst[-1]
            if len(doc_sp_lst) in [ 4 ]:
                ph = doc_sp_lst[-2] + doc_sp_lst[-1]
            if len(doc_sp_lst) in [ 3 ]:
                ph = doc_sp_lst[-1]
            
            if (ph[:2] not in ['AR', 'FY', 'Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2']):
               if (doc_name in ['2017-annual-report-20-f', 'CRH_Pressrelease_2017']):
                  ph = 'AR2017' 
               elif (doc_name in ['Unilever PLC__New_AR2013']):
                  ph = 'AR2013' 
               elif (doc_name in ['JPMorgan_ESG_2015']):
                  ph = 'AR2015' 
               elif (doc_name in ['JPMorgan_ESG_2016']):
                  ph = 'AR2016' 
               else:   
                  print 'ERROR PH: ', [ph, doc_id, doc_name]
                  sys.exit() 
            ph = ph.replace('AR', 'FY')
            if ph not in all_phs:continue
            if ph not in ph_doc_info_dict:
               ph_doc_info_dict[ph] = []
            ph_doc_info_dict[ph].append(doc_id)
            doc_ph_info[doc_id] = ph
        
        lmdb_folder = os.path.join(self.output_path, company_id) 
        if not os.path.exists(lmdb_folder):
            os.mkdir(lmdb_folder)
        
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        self.lmdb_obj.write_to_lmdb(dfname, doc_ph_info, doc_ph_info.keys())

 
        sorted_ph_lst = report_year_sort.year_sort(ph_doc_info_dict.keys())
        doc_pair_list = []
        ph_pair_list = []
        for i, ph in enumerate(sorted_ph_lst):
            doc_id1s = ph_doc_info_dict[ph]
            for kph in sorted_ph_lst[i+1:]:
                doc_id2s = ph_doc_info_dict[kph]
                for doc_id1 in doc_id1s:
                  for doc_id2 in doc_id2s:
                    if (doc_id1 == doc_id2):continue    
                    if (doc_id1, doc_id2) not in doc_pair_list:
                        doc_pair_list.append((doc_id1, doc_id2))
                    if (ph, kph) not in ph_pair_list:
                        ph_pair_list.append((ph, kph))
        print doc_pair_list
        print 
        print ph_pair_list
        return doc_pair_list
 
    def get_doc_pairs_old(self, company_id): 
        deal_id = tuple(company_id.split('_'))
        import read_slt_info as read_slt_info
        upObj = read_slt_info.update(deal_id) 
        docname_docid_lst = upObj.get_documentName_id()
        ph_doc_info_dict = {}
        doc_ph_info = {}
        for (doc_name, doc_id) in docname_docid_lst:
            doc_sp_lst = doc_name.split('_') 
            ph = ''
            if len(doc_sp_lst) == 2:
                ph = doc_sp_lst[-1]
            if len(doc_sp_lst) == 4:
                ph = doc_sp_lst[-2] + doc_sp_lst[-1]
            ph = ph.replace('AR', 'FY')
            ph_doc_info_dict[ph] = doc_id
            doc_ph_info[doc_id] = ph

        lmdb_folder = os.path.join(self.output_path, company_id) 
        if not os.path.exists(lmdb_folder):
            os.mkdir(lmdb_folder)
        
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        self.lmdb_obj.write_to_lmdb(dfname, doc_ph_info, doc_ph_info.keys())

 
        sorted_ph_lst = report_year_sort.year_sort(ph_doc_info_dict.keys())
        doc_pair_list = []
        ph_pair_list = []
        done_dict = {}
        for i, ph in enumerate(sorted_ph_lst):
            qh_flg1 = ph[:2]
            if ph[2:] in done_dict:continue
            done_dict[ph[2:]] = 1
            current_phs = self.get_current_phs(sorted_ph_lst, ph)
            next_ph = self.get_next_ph(sorted_ph_lst, ph)
            if next_ph:
                indx = sorted_ph_lst.index(next_ph)
                for ph in  current_phs:
                    doc_id1 = ph_doc_info_dict[ph]
                    for kph in sorted_ph_lst[indx:]:
                        doc_id2 = ph_doc_info_dict[kph]
                        if (doc_id1, doc_id2) not in doc_pair_list:
                            doc_pair_list.append((doc_id1, doc_id2))
                        if (ph, kph) not in ph_pair_list:
                            ph_pair_list.append((ph, kph))
        return doc_pair_list

    def get_current_phs(self, sorted_ph_lst, ph):
        cphs = []
        for each_ph in sorted_ph_lst:
           if each_ph[2:] == ph[2:]:
               cphs.append(each_ph)
        return cphs
        
    def get_next_ph(self, sorted_ph_lst, ph):
        
        indx = sorted_ph_lst.index(ph)
        print "from", sorted_ph_lst[indx:]
        next_ph = ''
        for each_ph in sorted_ph_lst[indx:]:
            if each_ph[2:] != ph[2:]:
               next_ph = each_ph    
               break 
        print "next", next_ph
        print "*"*100
        return next_ph




                    
         
    def generate_label_delta(self, company_id):
        lmdb_folder = os.path.join(self.output_path, company_id) 
        ofname = os.path.join(lmdb_folder, 'doc_table_final_chain_pair')
        final_pair_dict = self.lmdb_obj.read_all_from_lmdb(ofname)
        final_paths = final_pair_dict['doc_table_chain_pair_list']
      
        new_allkey_dict = {}

        print 'Creating: new_allkey_dict'

        for each_path in final_paths:
            #print each_path  
            for i1 in range(0, len(each_path)):
                for i2 in range(i1+1, len(each_path)):
                    hyp_doc_id = each_path[i1][0]
                    hyp_tab_id = each_path[i1][1]
                    ref_doc_id = each_path[i2][0]
                    ref_tab_id = each_path[i2][1]
                    #if not ((hyp_doc_id == '9' and hyp_tab_id == '5769') and (ref_doc_id == '10' and ref_tab_id == '3859')):continue
                    #if not ((hyp_doc_id == '8') and (ref_doc_id == '9')):continue
                    #print 'PAIR: ', [ hyp_doc_id, hyp_tab_id, ref_doc_id, ref_tab_id ]
                    #print 'got it: ' 
                    if (hyp_doc_id, hyp_tab_id) not in  new_allkey_dict:
                        lmdb_fname_hyp = os.path.join(lmdb_folder, hyp_doc_id)
                        hyp_fname = os.path.join(lmdb_fname_hyp, hyp_tab_id) 
                        allkey_dict = self.lmdb_obj.read_all_from_lmdb(hyp_fname)
                        new_allkey_dict[(hyp_doc_id, hyp_tab_id)] =  allkey_dict[hyp_tab_id] 
                        
                    if (ref_doc_id, ref_tab_id) not in  new_allkey_dict:
                        lmdb_fname_ref = os.path.join(lmdb_folder, ref_doc_id)
                        ref_fname = os.path.join(lmdb_fname_ref, ref_tab_id)
                        allkey_dict = self.lmdb_obj.read_all_from_lmdb(ref_fname)
                        new_allkey_dict[(ref_doc_id, ref_tab_id)] = allkey_dict[ref_tab_id]

        print 'Done Creating: new_allkey_dict'

        #sys.exit()
        done_list = {}
        fwd_table_dict = {} 
        rev_table_dict = {} 
        path_index = -1
        for each_path in final_paths:
            path_index = path_index + 1
            for i1 in range(0, len(each_path)):
                print 'Path: ', path_index,  '/', len(final_paths), ' ===> ', i1, '/' , len(each_path)
                for i2 in range(i1+1, len(each_path)):
                    hyp_doc_id = each_path[i1][0]
                    hyp_tab_id = each_path[i1][1]
                    ref_doc_id = each_path[i2][0]
                    ref_tab_id = each_path[i2][1]

                    if (hyp_doc_id, hyp_tab_id, ref_doc_id, ref_tab_id) in done_list: continue
                    done_list[(hyp_doc_id, hyp_tab_id, ref_doc_id, ref_tab_id)] = 1

                    #if not ((hyp_doc_id == '9' and hyp_tab_id == '1382') and (ref_doc_id == '10' and ref_tab_id == '3262')):continue
                    #if not ((hyp_doc_id == '8' and hyp_tab_id == '3773') and (ref_doc_id == '9' and ref_tab_id == '6208')):continue
                    #if not ((hyp_doc_id == '9' and hyp_tab_id == '5769') and (ref_doc_id == '10' and ref_tab_id == '3859')):continue
                    hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map = new_allkey_dict[(hyp_doc_id, hyp_tab_id)]
                    ref_line_item_map, ref_value_to_line_item_map, ref_value_tableid_map = new_allkey_dict[(ref_doc_id, ref_tab_id)]
                    t1 = time.time()

                    #print '1 in : '
                    #print hyp_value_to_line_item_map
                    #print hyp_value_tableid_map
                    #sys.exit()   
 
                    fwd_dict = self.find_delta_relationship_maps(hyp_line_item_map, ref_line_item_map)
                    rev_dict = self.find_delta_relationship_maps(ref_line_item_map, hyp_line_item_map)
                    t2 = time.time()
                    print 'Time Taken: ', t2 - t1 
                    #print [hyp_doc_id, hyp_tab_id, ref_doc_id, ref_tab_id]
                    #print 
                    #print fwd_dict 

                    #print hyp_line_item_map.keys()
                    #print ref_line_item_map.keys()  
                    #print hyp_line_item_map
                    #print 
                    #print ref_line_item_map 
                    #print rev_dict 
                    #sys.exit()
                    #print rev_dict[('Company restaurants (excluding depreciation and amortization) ~ Cost of sales', 'x1000129_88#x476_88#x552_88')]
                    #sys.exit()
                    #(hyp_doc_id, hyp_tab_id) -> { (ref_doc_id, ref_tab_id): fwd_dict }
                    #(ref_doc_id, ref_tab_id) -> { (hyp_doc_id, hyp_tab_id) : rev_dict }      
                    if (hyp_doc_id, hyp_tab_id) not in fwd_table_dict:
                       fwd_table_dict[(hyp_doc_id, hyp_tab_id)] = {} 
                    fwd_table_dict[(hyp_doc_id, hyp_tab_id)][(ref_doc_id, ref_tab_id)] = fwd_dict
                    if (ref_doc_id, ref_tab_id) not in rev_table_dict:
                       rev_table_dict[(ref_doc_id, ref_tab_id)] = {} 
                    rev_table_dict[(ref_doc_id, ref_tab_id)][(hyp_doc_id, hyp_tab_id)] = rev_dict
        doc_table_pair_forward_reverse_dict = {'fwd_table_dict':fwd_table_dict, 'rev_table_dict':rev_table_dict}
        lmdb_folder = os.path.join(self.output_path, company_id) 
        fname = os.path.join(lmdb_folder, 'doc_pair_forward_reverse')    
        self.lmdb_obj.write_to_lmdb(fname, doc_table_pair_forward_reverse_dict, doc_table_pair_forward_reverse_dict.keys())            
        
    def get_doc_ph_map(self, company_id):
        lmdb_folder = os.path.join(self.output_path, company_id) 
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        doc_ph_map_dict = self.lmdb_obj.read_all_from_lmdb(dfname)
        return doc_ph_map_dict
        
    def get_table_id(self, doc_id, value_ref_table_map, value_ref_ids):
        xml_id = value_ref_ids.split('#')[0]
        table_id = value_ref_table_map.get(doc_id, {}).get(xml_id, '')
        return table_id
         
    def find_other_values_gen(self, value_ref_ids,  company_id, doc_id):
        lmdb_folder = os.path.join(self.output_path, company_id) 

        lmdb_value_ref_table_path = os.path.join(lmdb_folder, 'value_ref_table_map')
        value_ref_table_map = self.lmdb_obj.read_all_from_lmdb(lmdb_value_ref_table_path)

        table_id = self.get_table_id(doc_id, value_ref_table_map, value_ref_ids) 

        doc_kname = os.path.join(lmdb_folder, doc_id)
        lmdb_ref_txt_path = os.path.join(doc_kname, 'txt_value_ref_value_map')
        final_dict = self.lmdb_obj.read_all_from_lmdb(lmdb_ref_txt_path)
        
        value_text_dict = final_dict['txt_value_ref_map_dict']        
        value_text_to_ref_dict = final_dict['value_to_txt_dict']        

        value_text, clean_value = 0, 0
        if table_id:  
            value_text, clean_value = value_text_dict.get((table_id, value_ref_ids), (0, 0)) 
        
        ph_values = [] 
        if clean_value != 0:
           all_value_ref_ids =  value_text_to_ref_dict[value_text]
           print 'all_value_ref_ids ' , all_value_ref_ids   
           for all_value_ref_id in all_value_ref_ids:
               print 'EACH', [all_value_ref_id] 
               ph_values += self.find_other_values(all_value_ref_id,  company_id, doc_id)
               print ph_values
               print "*"*100  
        else:
            ph_values = self.find_other_values(value_ref_ids,  company_id, doc_id)
        return ph_values


    def remove_null_values(self, fwd_table_dict):
        new_fwd_table_dict = {}
        for k, vdict in fwd_table_dict.items():
            for v, ddict in vdict.items():
                allow_flg = 0
                for m, n in ddict.items():
                    if n:
                       allow_flg = 1
                       break
                if (allow_flg == 1):
                   if k not in new_fwd_table_dict:
                      new_fwd_table_dict[k] = {}      
                   new_fwd_table_dict[k][v] = ddict
        return new_fwd_table_dict          


   

    def run_applicator_new(self, company_id ):
        #print obj.find_other_values('x1703_9', '1_62', '10')
        # ('4', '3237') ('1', '667') '10', '3859'
        t_doc_id = '10'
        t_table_id =  '3859'
        ref_id = 'x1703_9' 

        line_item_detail = ('Cost of sales', 'x1688_9', 0)        

        lmdb_folder = os.path.join(self.output_path, company_id) 
        fname = os.path.join(lmdb_folder, 'doc_pair_forward_reverse')    
        doc_table_pair_forward_reverse_dict = self.lmdb_obj.read_all_from_lmdb(fname)    
        fwd_table_dict = doc_table_pair_forward_reverse_dict['fwd_table_dict'] 
        rev_table_dict = doc_table_pair_forward_reverse_dict['rev_table_dict'] 
        all_starts = fwd_table_dict.keys()

        #rev_table_dict = self.remove_null_values(copy.deepcopy(rev_table_dict))
        #fwd_table_dict = self.remove_null_values(copy.deepcopy(fwd_table_dict))
                  
        back_results = []
        mytup = (t_doc_id, t_table_id)
        if mytup in rev_table_dict:
            init_values = [ [ [mytup, line_item_detail ] ] ]
            flg = 1 
            while flg:
                  flg = 0
                  new_init_values = []   
                  for init_val in init_values:
                      #print init_val[-1] 
                      last_key = init_val[-1][0]
                      last_key_ref =  init_val[-1][1] 
                      chk_tup = (last_key_ref[0], last_key_ref[1])
                      if last_key in rev_table_dict:
                         flg = 1
                         extended_pos = rev_table_dict[last_key].keys()[:]
                         #print 'last_key_ref: ', last_key_ref
                         #print extended_pos  
                         new_init_val = init_val[:]
                         for e in extended_pos:
                             #print '+++++++++++++++++++++++++++++++++++++++++++++++++++', e
                             #print rev_table_dict[last_key][e]
                             if (rev_table_dict[last_key][e].get(chk_tup, [])):
                                #print rev_table_dict[last_key][e][chk_tup] 
                                for m in rev_table_dict[last_key][e][chk_tup]:
                                    #print 'm: ', m
                                    if m[0] > 95:
                                       new_init_values.append(init_val[:] + [ [ e, (m[1], m[2]) ]  ])
                      else:
                             new_init_values.append(init_val[:])
                  init_values = new_init_values[:]                    
            #print len(init_values) 
            #for init_value in init_values: 
            #    print init_value   , len(init_value)
            back_results = init_values[:]  


        fwd_results = []
        mytup = (t_doc_id, t_table_id)
        if mytup in fwd_table_dict:
            init_values = [ [ [mytup, line_item_detail ] ] ]
            flg = 1 
            while flg:
                  flg = 0
                  new_init_values = []   
                  for init_val in init_values:
                      #print init_val[-1] 
                      last_key = init_val[-1][0]
                      last_key_ref =  init_val[-1][1] 
                      chk_tup = (last_key_ref[0], last_key_ref[1])
                      if last_key in fwd_table_dict:
                         flg = 1
                         extended_pos = fwd_table_dict[last_key].keys()[:]
                         #print 'last_key_ref: ', last_key_ref
                         #print extended_pos  
                         new_init_val = init_val[:]
                         for e in extended_pos:
                             #print '+++++++++++++++++++++++++++++++++++++++++++++++++++', e
                             #print fwd_table_dict[last_key][e]
                             if (fwd_table_dict[last_key][e].get(chk_tup, [])):
                                #print fwd_table_dict[last_key][e][chk_tup] 
                                for m in fwd_table_dict[last_key][e][chk_tup]:
                                    #print 'm: ', m
                                    if m[0] > 95:
                                       new_init_values.append(init_val[:] + [ [ e, (m[1], m[2]) ]  ])
                      else:
                             new_init_values.append(init_val[:])
                  init_values = new_init_values[:]                    
            #print len(init_values) 
            #for init_value in init_values: 
            #    print init_value   , len(init_value)
            fwd_results = init_values[:]  
        all_res = back_results + fwd_results
        return all_res
                

    def find_other_values(self, value_ref_ids,  company_id, doc_id):
        doc_ph_map_dict = self.get_doc_ph_map(company_id)
        lmdb_folder = os.path.join(self.output_path, company_id) 
        lmdb_value_ref_table_path = os.path.join(lmdb_folder, 'value_ref_table_map')
        value_ref_table_map = self.lmdb_obj.read_all_from_lmdb(lmdb_value_ref_table_path) 
        xml_id = value_ref_ids.split('#')[0]    
        table_id = value_ref_table_map.get(doc_id, {}).get(xml_id, '')     
        
        if not table_id.strip(): return []
        #print table_id 
        #sys.exit()

        lmdb_fname = os.path.join(lmdb_folder, doc_id)
        table_fname = os.path.join(lmdb_fname, table_id)
        allkey_dict = self.lmdb_obj.read_all_from_lmdb(table_fname)
        if table_id not in allkey_dict:
           print "NOT FOUND" 
           return [] 
        sel_line_item_map, sel_value_to_line_item_map, sel_value_tableid_map = allkey_dict[table_id]

        lmdb_folder = os.path.join(self.output_path, company_id) 
        fname = os.path.join(lmdb_folder, 'doc_pair_forward_reverse')    
        doc_table_pair_forward_reverse_dict = self.lmdb_obj.read_all_from_lmdb(fname)
        fwd_table_dict = doc_table_pair_forward_reverse_dict['fwd_table_dict']
        rev_table_dict = doc_table_pair_forward_reverse_dict['rev_table_dict']
        #================================================================================================
        line_item_info = ()
        map_val_ref = ''
        for ref in sel_value_to_line_item_map.keys():
            if xml_id in ref.split('#'):
               line_item_info = sel_value_to_line_item_map[ref]       
               map_val_ref = ref
               print line_item_info 
               break
        print [doc_id, table_id, xml_id]
        all_ph_values =  self.get_extraceted_values(doc_id, line_item_info, table_id, sel_value_to_line_item_map, rev_table_dict, lmdb_folder, doc_ph_map_dict)
        all_ph_values += self.get_extraceted_values(doc_id, line_item_info, table_id, sel_value_to_line_item_map, fwd_table_dict, lmdb_folder, doc_ph_map_dict)
        #================================================================================================
        return all_ph_values

    def get_extraceted_values(self, doc_id, line_item_info, table_id, sel_value_to_line_item_map, rev_table_dict, lmdb_folder, doc_ph_map_dict):
               
        #print line_item_info 

        all_ph_values = [] 
        all_revs = rev_table_dict.get((doc_id, table_id), {})
        val_count = line_item_info[2] 
        #print line_item_info
        #print all_revs  
        for rev_k, rev_vals in all_revs.items():
            print '---rev_k: ', rev_k
            print ' rev_vals: ', rev_vals
            print ' search key: ', (line_item_info[0], line_item_info[1]) 
            matches = rev_vals.get((line_item_info[0], line_item_info[1]), [])
            print 'MATCHES', matches
            if matches:
               #match = matches[0] # only first match as of now
               match_flg = 0  
               for match in matches:
                   #if match_flg != 0:
                   if not (match[0] > 95): continue
                   #     print "LESS THAN 50",  match
                   #     break
                   match_flg = 0 
                   match_line_item = (match[1], match[2])
                   lmdb_fname = os.path.join(lmdb_folder, rev_k[0])
                   table_fname = os.path.join(lmdb_fname, rev_k[1])
                   allkey_dict = self.lmdb_obj.read_all_from_lmdb(table_fname)
                   nsel_line_item_map, nsel_value_to_line_item_map, nsel_value_tableid_map = allkey_dict[rev_k[1]]
                   all_vals = nsel_line_item_map.get(match_line_item, [])
                   #print all_vals
                   #print 'val_count: ', val_count 
                   extracted_val = all_vals[val_count]
                   #print extracted_val

                   # (value, ref, doc_id, page, PH)
                   #(extracted_val[1], extracted_val[2], doc_id, table_id) 
                   ph = doc_ph_map_dict.get(rev_k[0], '') 
                   all_ph_values.append((rev_k[0], rev_k[1],  ph, extracted_val[1], extracted_val[2]))
         
        return all_ph_values    
 
 
    def get_doc_id_table_dict(self, company_id):
        project_id, url_id = company_id.split('_')
        norm_res_list = sObj.slt_normresids(project_id, url_id)
        doc_id_table_dict = {}
        for doc_tup in norm_res_list:
            doc_id, page_number, norm_table_id = doc_tup
            if doc_id not in doc_id_table_dict:
                doc_id_table_dict[doc_id] = []
            doc_id_table_dict[doc_id].append(norm_table_id)
        
        return doc_id_table_dict


    def generate_row_col_data(self, company_id, company_name):
        norm_tab_data_path = os.path.join(self.output_path, company_id, 'norm_table_data')             
        project_id, url_id = company_id.split('_')
        norm_res_list = sObj.slt_normresids(project_id, url_id)
        doc_page_dict = {}
        for doc_tup in norm_res_list:
            doc_id, page_number, norm_table_id = doc_tup
            if doc_id not in doc_page_dict:
                doc_page_dict[doc_id] = []
            doc_page_dict[doc_id].append(norm_table_id)
        doc_id = '4' 
        if doc_id not in self.doc_table_html_dict: 
            self.doc_table_html_dict[doc_id] = []
        if doc_id  not in self.value_to_txt_dict:
            self.value_to_txt_dict[doc_id] = {}
        if doc_id not in self.txt_to_value_ref_map:
            self.txt_to_value_ref_map[doc_id] = {}
        table_lst = doc_page_dict.get(doc_id, [])
        row_col_lmdb_path = os.path.join(self.output_path, company_id, 'row_col_map_data')
        for table_id in table_lst:
            if str(table_id) not in  ['15352']:continue
            norm_row_col_map_dict = self.generate_map_ds(project_id, url_id, table_id, doc_id, norm_tab_data_path)
            row_col_fname = os.path.join(row_col_lmdb_path, table_id) 
            self.lmdb_obj.write_to_lmdb(row_col_fname, norm_row_col_map_dict, norm_row_col_map_dict.keys())
        print row_col_lmdb_path

    def generate_map_ds_new(self, project_id, url_id, norm_table_id, doc_id):
        deal_id = '_'.join([project_id, url_id])
        print [deal_id, norm_table_id]
        celldata = self.Mobj.run_process(norm_table_id, deal_id)
        row_col_dict2 = {}
        value_table_id_map = {}
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
                colspan = cell_info.get('colspan', 1)
                rowspan = cell_info.get('rowspan', 1)
                if row not in row_col_dict2:
                    row_col_dict2[row] = {}
                txt = txt.replace('&nbsp;', ' ')
                row_col_dict2[row][col] = (row, col, txt, section_type, colspan, rowspan, section_type, '#'.join(xml_ids))
                value_table_id_map['#'.join(xml_ids)] = norm_table_id

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
        #for elm in data_ar:
        #    print elm
        html_str =  self.display_table(data_ar, norm_table_id)
        new_data_ar = self.normalise_colspan(data_ar)
        html_str2 =  self.display_table(new_data_ar, norm_table_id)
        normalised_hyp_data = self.normalise_rowspan(new_data_ar)
        html_str3 =  self.display_table(normalised_hyp_data, norm_table_id)
         
        hyp_line_item_map, hyp_value_to_line_item_map, norm_row_col_map_dict, line_item_map_ref = self.form_search_data_structure(normalised_hyp_data, norm_table_id, doc_id)
        return hyp_line_item_map, hyp_value_to_line_item_map, value_table_id_map, norm_row_col_map_dict, line_item_map_ref


    def update_line_db(self, company_id, idoc_ids):
        lmdb_folder = os.path.join(self.output_path, company_id) 
        if not os.path.exists(lmdb_folder):
            os.mkdir(lmdb_folder)
        project_id, url_id = company_id.split('_')
        norm_res_list = sObj.slt_normresids(project_id, url_id)
        doc_page_dict = {}
        for doc_tup in norm_res_list:
            doc_id, page_number, nnorm_table_id = map(lambda x:str(x), doc_tup)
            if doc_id not in idoc_ids:continue
            if doc_id not in doc_page_dict:
                doc_page_dict[doc_id] = []
            doc_page_dict[doc_id].append(nnorm_table_id)
        for doc_id, table_ids in doc_page_dict.items():
            lmdb_fname = os.path.join(lmdb_folder, doc_id)
            if not os.path.exists(lmdb_fname):
                os.mkdir(lmdb_fname)
            for table_id in table_ids:
                hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map, norm_row_col_map_dict, hyp_line_item_map_ref = self.generate_map_ds_new(project_id, url_id, table_id, doc_id)
                table_fname = os.path.join(lmdb_fname, table_id)
                if not os.path.exists(table_fname):
                    os.mkdir(table_fname)
                
                allkey_dict = {}
                allkey_dict[table_id] = (hyp_line_item_map, hyp_value_to_line_item_map, hyp_value_tableid_map, hyp_line_item_map_ref)
                self.lmdb_obj.write_to_lmdb(table_fname, allkey_dict, allkey_dict.keys())
                print table_id

if __name__ == "__main__":
    obj = Delta()
    #company_id = '1_34' #done
    #company_id = '1_39' #done
    #company_id = '1_43' #done
    #company_id = '1_73' #done
    #for company_id_str in ['1_35#bayerag']:
    company_id = sys.argv[1]
    obj.update_line_db(company_id)
