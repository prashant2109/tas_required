import MySQLdb, sys, os
import shelve
class Slt_normdata():
    def __init__(self):
        self.dbname = "tfms_urlid"
        dbconnstr = "172.16.20.229#root#tas123"
        self.ip_addr, self.uname, self.passwd = dbconnstr.split('#') 
        pass

    def slt_normresids(self, project_id, url_id):
        
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        #sql = "select ref_resid,norm_resid,docid, pageno from norm_data_mgmt where process_status= 'Y' and active_status= 'Y' and review_flag='Y' and ref_resid in (select resid from data_mgmt where  process_status= 'Y' and active_status= 'Y')" ;
#        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid" ;
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')"
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
                normlst.append((docid, pageno,norm_resid))
            norm_resid_d[resid] = normlst
        norm_resid_lst = []
        for red, normdata in norm_resid_d.items():
            norm_resid_lst+=normdata
        return norm_resid_lst
    
    def slt_normresids_20(self, project_id, url_id):
        
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        #sql = "select ref_resid,norm_resid,docid, pageno from norm_data_mgmt where process_status= 'Y' and active_status= 'Y' and review_flag='Y' and ref_resid in (select resid from data_mgmt where  process_status= 'Y' and active_status= 'Y')" ;
#        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid" ;
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno, n.source_table_info from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')"
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        norm_resid_d = {}
        for r in results:
            resid = str(r[0])
            norm_resid = str(r[1])
            docid = str(r[2])
            pageno = str(r[3])
            if r[4]:
                gr_id = str(eval(r[4])[-1])
            else:
                gr_id   = '()'
            normlst = norm_resid_d.get(resid, [])
            if (docid, pageno,norm_resid, gr_id) not in normlst:
                normlst.append((docid, pageno,norm_resid, gr_id))
            norm_resid_d[resid] = normlst
        norm_resid_lst = []
        for red, normdata in norm_resid_d.items():
            norm_resid_lst+=normdata
        return norm_resid_lst

    def read_norm_cell_dict(self, project_id, url_id, norm_id):
        norm_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh'
        sh = shelve.open(norm_path%(project_id, url_id, norm_id[2]))
        if 'celldata' not in sh:
            return ''
        celldata = sh['celldata']
        row_col_dict = {}
        for key, value_dict in celldata.items():
            cell_ids = value_dict.keys()
            cell_ids.sort()
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                section_type = cell_info.get('section_type', '')
                if not section_type:
                    section_type = ''
                txt = ' '.join(cell_info.get('text_lst', []))
                row, col = cell_id
                if row not in row_col_dict:
                    row_col_dict[row] = []
                row_col_dict[row].append((col, txt, section_type))
        html_str = '<table border="1">'
        rows = row_col_dict.keys() 
        rows.sort()
        for row in rows:
            html_str+='<tr>'
            cols = row_col_dict[row]
            cols.sort()
            for col_tup in cols:
                col, txt, section_type = map(lambda x:str(x), list(col_tup))
                #html_str+='<td>%s</td>' %(txt+'____'+section_type)
                html_str+='<td>%s</td>' %(txt)
            html_str+='</tr>'
        html_str+='</table>'
        return html_str

if __name__=='__main__':
    project_id = '1'
    url_id = '75'
    sObj = Slt_normdata()
    norm_res_list = sObj.slt_normresids(project_id, url_id)
    print norm_res_list    
