import MySQLdb, os, shelve, sys
import common.webdatastore as webdatastore
class s():
    def __init__(self):
        self.webObj = webdatastore.webdatastore()
        self.dbname = "tfms_urlid"
        dbconnstr = "172.16.20.122#root#tas123"
        self.ip_addr, self.uname, self.passwd = dbconnstr.split('#') 
        self.cellpath = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_resdata/%s'
        self.html_path ='/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_results/norm_resid_%s.html'
        self.celldict_path ='/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh'
        self.cellpath_beforenorm ='/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/celldata/%s'
        pass


    def slt_normresids(self, project_id, url_id, valid_norm_list):
        
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
#        sql = "select ref_resid,norm_resid,docid, pageno from norm_data_mgmt where process_status= 'Y' and active_status= 'Y' and review_flag='Y' and ref_resid in (select resid from data_mgmt where process_status='Y' and active_status='Y')";
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid"
       # sql = "select ref_resid,norm_resid,docid, pageno from norm_data_mgmt where process_status= 'Y' and active_status= 'Y'";
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        norm_resid_d = {}
        for r in results:
            resid = str(r[0])
            norm_resid = str(r[1])
            docid = str(r[2])
            pageno = str(r[3])
            normlst = norm_resid_d.get(resid, [])
            if (docid, pageno,norm_resid) not in normlst:
                if valid_norm_list and (norm_resid not in valid_norm_list):continue
                normlst.append((docid, pageno,norm_resid))
                norm_resid_d[resid] = normlst

        norm_resid_lst = []
        for red, normdata in norm_resid_d.items():
          #  if len(normdata)==1:
          #      norm_resid_lst.append(normdata[0])
                norm_resid_lst += normdata
        norm_resid_dict = {}
        for (docid, pageno, normid) in norm_resid_lst:
            lst = norm_resid_dict.get((docid, pageno),[])
            lst.append(normid)
            norm_resid_dict[(docid, pageno)] = lst
        return norm_resid_dict


    def get_document_pageno_celldict(self,norm_resid,project_id, url_id):
        tmp_path=self.cellpath%(project_id, url_id,norm_resid)    
        tmpd = self.webObj.read_all_from_lmdb(tmp_path, {})
        d0 = tmpd
        return d0['celldata'][0]

    def get_document_pageno_celldict_beformnorm(self,project_id, url_id, docid, pageno):
        tmp_path=self.cellpath_beforenorm%(project_id, url_id, docid, pageno)    
        tmpd = self.webObj.read_all_from_lmdb(tmp_path, {})
        d0 = tmpd
        return d0[int(pageno)]['cell_dict']

    def get_celldict(self,pid, uid, norm_resid):
        path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/'%(pid, uid)
        sh_path = os.path.join(path, norm_resid)
        sh = shelve.open(sh_path+'.sh')
        cell_dict = sh['celldata']
        sh.close()
        return cell_dict[0]

    def get_xml_id_list(self, celldict):
        all_id_list = []
        for k, v in celldict.items():
            text_ids = [x.strip() for x in v.get('text_ids',[]) if x.strip()]
            for txt_id in text_ids: 
                  id_list = txt_id.split('x')
                  id_l = id_list[1].split('_')[0]
                  all_id_list.append(int(id_l))
        all_id_list.sort()
        return all_id_list

    def assign_id_for_dummy_cell(self, cell_dict, pageno, empty_id):
            empty_dict = {}
            for kk, vv in cell_dict.items():
                if vv.has_key('text_ids') and ''.join(vv['text_ids']).strip()=='' :#or ('DUMM' in ''.join(vv['text_ids']).strip()):
                   empty_dict[kk] = 1 
                elif vv.has_key('section_type') and (not vv.has_key('text_ids')):#or ('DUMM' in ''.join(vv['text_ids']).strip()):
                   vv['text_ids'] = []
                   if (not vv.has_key('text_lst')): 
                       vv['text_lst'] = [''] 
                   empty_dict[kk] = 1 
            emp_keys = empty_dict.keys()
            emp_keys.sort()
            xml_id = 'x'
            for emp_k in emp_keys:
                cell_dict[emp_k]['text_ids'] = [xml_id+str(empty_id)+'_'+pageno]
                empty_id+=1
            return cell_dict, empty_id
 
    def update_celldict(self, project_id, url_id, celldict, norm_resid):
        
        celldict_path = self.celldict_path %(project_id, url_id, norm_resid)  
        sh = shelve.open(celldict_path)
        sh['celldata'] = {0:celldict}
        sh.close()
        
        norm_res_ofname = self.cellpath%(project_id, url_id, norm_resid)
        tmpd = self.webObj.read_all_from_lmdb(norm_res_ofname, {})
        d0 = tmpd
        d0['celldata'] = {0:celldict}
        self.webObj.write_to_lmdb(norm_res_ofname, d0, d0.keys(), 1)
        return 
        
        

    def get_allresid_celldict_to_web(self,project_id, url_id):
        f = open('update_xml_ids.txt','r')
        lines = f.readlines()
        f.close()
        valid_norm_list = [x.strip() for x in lines]
        #valid_norm_list = []
        normresid_dict = self.slt_normresids(project_id, url_id, valid_norm_list)
        for (docid, pageno), norm_resid_st in normresid_dict.items():
#            if (docid, pageno)!=('35', '10'):continue
    #        if '41794' not in norm_resid_st:continue
            print docid, pageno, norm_resid_st
            #sys.exit()
            doc_page_celldict = self.get_document_pageno_celldict_beformnorm(project_id, url_id, docid, pageno)
            all_id_list = self.get_xml_id_list(doc_page_celldict)
            all_cell_dict = {}
            for normresid in norm_resid_st:
                norm_celldict = self.get_celldict(project_id,url_id,normresid)
                all_cell_dict[normresid] = norm_celldict
                all_id_list += self.get_xml_id_list(norm_celldict)
                 
            max_id =  max(all_id_list)+1000
            for kk, vv in all_cell_dict.items():
                vv, max_id = self.assign_id_for_dummy_cell(vv, pageno,max_id) 
                self.update_celldict(project_id, url_id,vv, kk)
                

if __name__=='__main__':
    #project_id =  '1'
    #url_id =  '183'
    dealid = sys.argv[1] 
    project_id, url_id = dealid.split('_')
    sobj = s()
    sobj.get_allresid_celldict_to_web(project_id, url_id)


        

