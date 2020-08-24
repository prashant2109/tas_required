import MySQLdb
import os,sets
import shelve
import lmdb
import common_func_class as common_func
import common.GlobalData as GlobalData
idbkey = 'QngxaW1pcGZGeWwxNUFBNmshqVLICs12'
idbkeysize = 32
gobj = GlobalData.GlobalData()
if idbkey is not None: gobj.add('dbkey', idbkey)
if idbkeysize is not None: gobj.add('dbkeysize', idbkeysize)
class Map_sectiontype():
    def __init__(self):
        self.app_keypath = '/var/www/html/TASFundamentalsV2/data/output/%s/%s/1_1/21/rdata/APP_RESULT_GC/%s_%s/'
        pass
    def db_connection_batch_name(self,project_id,url_id):
        db=MySQLdb.connect("172.16.20.229","root","tas123","tfms_urlid_%s_%s" %(str(project_id),str(url_id)))
        cursor=db.cursor()
        return db,cursor

    def get_app_key(self,norm_resid,dealid):
        project_id, url_id = dealid.split('_')
        db, cursor = self.db_connection_batch_name(project_id, url_id)        
        sql = "select docid, pageno, applicator_key from data_mgmt where process_status =  'Y' and active_status = 'Y' and resid in (select ref_resid from norm_data_mgmt where norm_resid=%s)"%(norm_resid)
        cursor.execute(sql)
        result = cursor.fetchall()
        app_key_data = []
        for r in result:
            app_key_data.append((str(r[0]),str(r[1]),str(r[2])))
        cursor.close()
        db.close()
        return app_key_data

    def load_brm_dict(self, docid, pageno, project_id,urlid):
        self.__opath = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/'%(project_id,urlid)
        self.__ipath = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/'%(project_id,urlid)
        self.__isdb = 1
        self.__isenc = 1
        self.common_func = common_func.common_func(self.__ipath,self.__opath,self.__isdb,self.__isenc)
        #brm_dict = self.common_func.get_cell_info_dict_level(docid, pageno,'TOC_SPLIT')
        brm_dict = self.common_func.get_cell_info_dict_level(docid, pageno,'TOC_L3')
        return brm_dict

    def populate_cell_id_bbox_dict(self, brm_dict):
        cell_keys = brm_dict.keys()
        cell_keys.sort()
        cell_bbox_dict = {}
        xml_id_bbox = {}
        rev_xml_id_bbox = {}
	cell_id_bbox_dict = {}
	cell_lst =[]
        for cell in cell_keys:
             bbox_list = [] 
	     if int(cell[0]) > 998:
		continue
             xml_id_list = []
             for each in brm_dict[cell]['cell_dict']['chunks']:
                 bbox = (int(each['grid_bbox']['x0']), int(each['grid_bbox']['y0']), int(each['grid_bbox']['x1']), int(each['grid_bbox']['y1'])) 
                 bbox_list.append(bbox)
                 xml_id_list.append(each['xmlID']) 
             rbbox = self.redefine_bbox(bbox_list)
             for xml_id in xml_id_list:
                 for xml_i in xml_id.split('#'):
                     xml_id_bbox[xml_i] = rbbox
                     li = rev_xml_id_bbox.get(rbbox,[])
                     li.append(xml_i)
                     rev_xml_id_bbox[rbbox] = li  
	     cell_lst.append(rbbox)
             cell_bbox_dict[rbbox] = cell
	     cell_id_bbox_dict[cell] = rbbox
	return cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox

    def redefine_bbox(self, bbox_list): 
          if not bbox_list:
              return ()
          xmin_li = []  
          xmax_li = []  
          ymin_li = []
          ymax_li = []
          for each in bbox_list:
              xmin_li.append(each[0]) 
              ymin_li.append(each[1])
              xmax_li.append(each[2])   
              ymax_li.append(each[3])
          xmin = min(xmin_li) 
          ymin = min(ymin_li)
          xmax = max(xmax_li)
          ymax = max(ymax_li)
          return (xmin, ymin, xmax, ymax)
        
    def get_app_result_data(self,app_key_data, dealid):
        project_id, url_id = dealid.split('_')
        rev_map_dict = {}
        xml_id_bbox = {} 
        cell_bbox_dict = {} 
        cell_id_bbox_dict, rev_xml_id_bbox, brm_dict = {}, {}, {}
        for (docid, pno, app_key ) in app_key_data:
            brm_dict = self.load_brm_dict(docid,pno,project_id, url_id)
            cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox = self.populate_cell_id_bbox_dict(brm_dict)
            env = lmdb.open(self.app_keypath%(project_id,url_id,docid, pno))
            with env.begin() as txn:
                v = txn.get(app_key)
                v = eval(v)
                opcombo_list = v['data'][7]
                for each in opcombo_list:
                    section_type = each[0]
                    xmlid = each[1][4]  
                    #print "ggg",section_type, xmlid 
                    for cnt, ee in enumerate(xmlid):
                        section_type1 = ''
                        if section_type=='Horizontal Grid Header':
                           section_type1 = 'HGH' 
                        elif section_type=='Vertical Grid Header':
                           section_type1 = 'VGH' 
                        elif section_type=='Grid Value':
                           section_type1 = 'GV' 
                        elif section_type=='Grid Header':
                           section_type1 = 'GH' 
                        elif section_type=='Para Grid Header':
                           section_type1 = 'PARAGH' 
                      #  if not section_type1:
                      #     section_type1 = section_type 
                        if not xml_id_bbox.has_key(ee):continue
                        rev_map_dict[ee] = section_type1
#        sys.exit()
        return rev_map_dict,  cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict

    def check_for_intersection(self,gv_col_freq, hgh_col_freq):
        if gv_col_freq>hgh_col_freq:
           return 1
        return 0  

    def validate_section_type(self, rev_map_dict, cell_rev_map_dict, cell_dict): 
        section_dict = {}
        for k, v in rev_map_dict.items():
        #    if v=='HGH':continue
        #    print "k", k, "v", v
            ref_list = section_dict.get(v,[])      
            ref_list+=cell_rev_map_dict.get(k,[])
            section_dict[v] = ref_list
        #print section_dict['GV']
        #print section_dict['HGH']
        #sys.exit() 
        min_max_dict = {}
        for k, v in section_dict.items():
            mdict = min_max_dict.get(k, {})
#            if k!='GV':continue
            for (cell,txt,ref_ids) in v:
 #               print "cell", cell, "txt", txt
                colspan = cell_dict.get(cell, {}).get('colspan',1)
                rowspan = cell_dict.get(cell, {}).get('rowspan',1)
                d_dict = mdict.get('col',{})
                for c in range(cell[1], cell[1]+colspan):
                    cnt = d_dict.get(c,0)
                    cnt+=1
                    d_dict[c] = cnt
                mdict['col'] = d_dict
                d_dict = mdict.get('row',{})
                for r in range(cell[0], cell[0]+rowspan):
                    cnt = d_dict.get(r,0)
                    cnt+=1
                    d_dict[r] = cnt
                mdict['row'] = d_dict
            min_max_dict[k] = mdict
        section_d = {}
#        print min_max_dict
 #       sys.exit()
        for k, v in min_max_dict.items():
            if k=='Grid':continue
            if not v:continue
            row_keys = v['row'].keys()
            col_keys = v['col'].keys()
            row_keys.sort()
            col_keys.sort()
            section_d[k] = [(row_keys[0],row_keys[-1]), (col_keys[0], col_keys[-1])]
        #print section_d
        #sys.exit()
        HGH_s = section_d.get('HGH',[])    
        GV_s = section_d.get('GV',[])    
        VGH_s = section_d.get('VGH',[])    
        if HGH_s and GV_s:
           col1 = HGH_s[1]
           col2 = GV_s[1]
        #   print col1[-1], col2[0]
        #   sys.exit()  
           if col1[-1]==col2[0]:
              if self.check_for_intersection(min_max_dict['GV']['col'][col2[0]], min_max_dict['HGH']['col'][col1[-1]]):
                  section_d['HGH'] = [HGH_s[0], (HGH_s[1][0],HGH_s[1][-1]-1)]   
              else:  
                  section_d['GV'] = [GV_s[0], (HGH_s[1][-1]+1, GV_s[1][-1])]   
                  pass
        #print section_d
        #sys.exit()
        GH_s = section_d.get('GH',[])
        if VGH_s and HGH_s: 
           col1 = range(VGH_s[1][0], VGH_s[1][-1]+1)
           col2 = range(HGH_s[1][0], HGH_s[1][-1]+1)
           row1 = range(VGH_s[0][0], VGH_s[0][-1]+1)
           row2 = range(HGH_s[0][0], HGH_s[0][-1]+1)
           if (sets.Set(col1)&sets.Set(col2)) and (sets.Set(row1)&sets.Set(row2)):
              section_d['HGH_s'] = [(row1[-1]+1, row2[0]), col2]    
#        print HGH_s
 #       print GH_s
  #      sys.exit()
        if HGH_s and GH_s:
           row1 = HGH_s[0]
           row2 = GH_s[0]
           if row1[0]<row2[-1]:
              section_d['HGH'] = [(row2[-1]+1,row1[-1]), HGH_s[1]]       
        #print section_d
        #sys.exit()
        section_dict =  {}
        rev_section_d_r = {}
        rev_section_d_c = {}
        for k, v in section_d.items():
            row_keys = v[0]
            col_keys = v[1]
            #print k
            #print row_keys
            #print col_keys
            for r in range(row_keys[0], row_keys[-1]+1):
                for c in range(col_keys[0], col_keys[-1]+1):
                    section_dict[(r, c)] = k
                    li = rev_section_d_r.get(k,[])
                    if (r not in li):
                        li.append(r)
                    rev_section_d_r[k] = li

                    li = rev_section_d_c.get(k,[])
                    if (c not in li):
                        li.append(c)
                    rev_section_d_c[k] = li
        #print rev_section_d_c
        #sys.exit()
        return section_dict, rev_section_d_r, rev_section_d_c
            
        
        

    def update_dict(self, rev_map_dict, norm_resid, cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict,dealid):
        pid, uid = dealid.split('_')
        path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/'%(pid, uid)
        sh_path = os.path.join(path, norm_resid)
        sh = shelve.open(sh_path+'.sh')
        cell_dict = {}
        try:
            cell_dict = sh['celldata'][0]
            sh.close()
        except:
            print '-------------------------------This Table is not present', sh_path
        return {0:cell_dict}
        cell_keys = cell_dict.keys()
        cell_rev_map_dict = {}
        mul_sec_type = {}
        for cell_k in cell_keys:
    #        print cell_dict[cell_k]
            if cell_dict[cell_k].has_key('text_ids'):
        #        print "TXT ID:",cell_dict[cell_k]['text_ids']
                for txtid in cell_dict[cell_k]['text_ids']:
                    if not txtid:continue
                    if (not xml_id_bbox.has_key(txtid)):continue
                    #print "11",txtid, rev_map_dict[txtid], cell_dict[cell_k]['text_lst']
                    #print "22",brm_dict[cell_bbox_dict[xml_id_bbox[txtid]]]['all_text']
                    #print cell_dict[cell_k]['section_type']
                    li = cell_rev_map_dict.get(txtid,[])
                   # print 'txtid',txtid, 'text_lst',cell_dict[cell_k]['text_lst']
                    #s_li = mul_sec_type.get(txtid,[])
                    #s_li.append(rev_map_dict[txtid])
                    #mul_sec_type[txtid] = s_li
                    li.append([cell_k,cell_dict[cell_k]['text_lst'], cell_dict[cell_k]['text_ids']])
                    cell_rev_map_dict[txtid] = li
#        for kk, vv in mul_sec_type.items():
 #           print '-'*100
  #          print kk
   #         print vv    
        #sys.exit()
        #print rev_map_dict
        section_map_dict,rev_section_d_row, rev_section_d_col = self.validate_section_type(rev_map_dict, cell_rev_map_dict, cell_dict)
             
        for k, v in cell_rev_map_dict.items():
            #section_typ = rev_map_dict.get(k,'')
            #if not section_typ:continue
            
            for (cell,txt,ref) in v:
                section_typ = section_map_dict.get(cell,'')
#                print section_typ
#                sys.exit()
#                if cell_dict[cell].has_key('section_type') and (cell_dict[cell]['section_type']=='' or cell_dict[cell]['section_type']==1 or cell_dict[cell]['section_type']==[]):
                if 1 and section_typ!='':
                   cell_dict[cell]['section_type'] = section_typ
        #print cell_dict
        null_cells = []
        for kk, vv in cell_dict.items():
            if (not vv.has_key('section_type')):
               vv['section_type'] = ''
            if vv['section_type']=='' or vv['section_type']==[] or vv['section_type'] == 1:
               for rk, rv in rev_section_d_row.items():
                   row_range = range(rv[0], rv[-1]+1)
                   col = rev_section_d_col[rk]
                   col.sort()
                   col_range = range(col[0], col[-1]+1)
                   if (kk[0] in row_range) and (kk[1] in col_range):
                      vv['section_type']  = rk
                      break    
        return {0:cell_dict}
          

    def run_process(self,norm_resid,dealid):
        app_key_data = self.get_app_key(norm_resid,dealid)
        rev_map_dict,  cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict = self.get_app_result_data(app_key_data, dealid)
        norm_cell_dict = self.update_dict(rev_map_dict, norm_resid, cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict,dealid)
        #print "norm_celldict:::::::::::::",norm_cell_dict
        return norm_cell_dict

    def run_ppprocess(self, deal_table_tup_lst):
        norm_table_dict = {}
        for ttup in deal_table_tup_lst:
            norm_resid,dealid = ttup
            app_key_data = self.get_app_key(norm_resid,dealid)
            rev_map_dict,  cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict = self.get_app_result_data(app_key_data, dealid)
            norm_cell_dict = self.update_dict(rev_map_dict, norm_resid, cell_bbox_dict, cell_id_bbox_dict, xml_id_bbox, rev_xml_id_bbox, brm_dict,dealid)
            norm_table_dict[norm_resid]=norm_cell_dict
            #print "norm_celldict:::::::::::::",norm_cell_dict
        return norm_table_dict

if __name__=='__main__':
    mobj = Map_sectiontype()    
    #print mobj.run_process('3502','1_133')
    print mobj.run_process('20543','3_152')


