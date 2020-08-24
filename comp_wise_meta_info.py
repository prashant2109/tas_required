import os, sys, json
import common.xlsxReader as xlsxReader
import sqLite.sqLiteApi as sqLiteApi
import ConfigParser
import schrodersData as schrodersData
import common.report_year_sort as report_year_sort
from db.webdatastore import webdatastore
from datetime import datetime

class Metadata(object):
    def __init__(self, config_path='/root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/dbConfig.ini'):
        self.config_path    = config_path
        self.config         = ConfigParser.ConfigParser()
        self.config.read(config_path)
        self.dbpath         =  self.config.get('modeldb', 'value')
        self.qObj           = sqLiteApi.sqLiteApi()
        self.objX           = xlsxReader.xlsxReader()
        self.db_tab         = 'tas_company.db'
        self.cObj           = schrodersData.schrodersData(config_path)
        self.output_path    = '/var/www/html/fundamentals_intf/output/'
        self.lmdb_obj       = webdatastore()
        self.md_col_list    = [('row_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('old_doc_name', 'VARCHAR(256)'), ('document_type', 'VARCHAR(256)'), ('filing_type', 'VARCHAR(256)'), ('period', 'VARCHAR(256)'), ('reporting_year', 'VARCHAR(256)'), ('doc_name', 'VARCHAR(256)'), ('doc_release_date', 'VARCHAR(256)'), ('doc_from', 'VARCHAR(256)'), ('doc_to', 'VARCHAR(256)'), ('doc_download_date', 'VARCHAR(256)'), ('doc_prev_release_date', 'VARCHAR(256)'), ('doc_next_release_date', 'VARCHAR(256)'), ('review_flg', 'INTEGER'), ('doc_id', 'INTEGER')]
    
    def get_connection(self, db_path):
        conn = self.qObj.create_connection(db_path)
        cur  = conn.cursor()
        return conn, cur

    def get_ijson_data(self, ijson):
        company_name    = str(ijson['company_name'])
        model_number    = str(ijson['model_number'])
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = '%s_%s' %(project_id, deal_id)
        machine_id      = ijson.get('machine_id', '')
        return company_name, model_number, company_id, machine_id

    #ANIL GIVEN NEW CODE
    def metadata_info_fn(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        sys.path.append('/root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/')
        import MRD.newbuilderapi as NB
        nbObj       = NB.Builder(self.config_path, company_id)
        msg         = nbObj.insert_company_meta(company_name, model_number)
        msg_dump    = self.generate_txt(ijson)
        return [{'message':msg}]

    def insert_xlsx_data(self, company_name, model_number, data_rows, excelFlg):
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur   = self.get_connection(db_path)
        table_name  = 'company_meta_info'
        if excelFlg:
            try:
                self.qObj.dropTable(conn, cur, '', table_name)
            except:
                pass          

        self.qObj.createLiteTable(conn, cur, '', table_name, self.md_col_list)
        column_tup  = tuple(map(lambda x:str(x[0]), self.md_col_list[1:]))

        msg         = self.qObj.insertIntoLite(conn, cur, '', table_name, column_tup, data_rows)
        conn.close()
        return msg
    
    # reading data from company_meta_info table and converting to list of dict 1101
    def get_md_info_old(self, ijson):#company_name, model_number, company_id):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson) 
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur   = self.get_connection(db_path)

        ## Inserting new element
    
        insert_msg = self.insert_doc_information(conn, cur, company_name, model_number, company_id)
        #print insert_msg 

        #ddict = self.get_doc_dict(company_name, company_id) #ddict, getting doc_names with respective doc_id's

        table_path  = os.path.join(self.dbpath, company_name, model_number, self.db_tab)
        table_name  = 'company_meta_info'

        column_list = ['row_id','old_doc_name', 'document_type', 'period', 'filing_type', 'doc_release_date', 'doc_name', 'reporting_year', 'doc_id']
        data_company_meta_info_tbl = self.qObj.readFromLite(conn, cur, table_path, table_name, column_list) 
    
        lst_dict_rows_company_meta_info = []
        sno = 1
        for row_tup in data_company_meta_info_tbl:
            row_id ,old_doc_name, document_type, period, filing_type, doc_release_date, doc_name, reporting_year, doc_id = map(str, row_tup)
            atr_splt = doc_release_date.split('@@')
            #if not doc_id:
            #    continue
            if len(atr_splt) > 1:
                rd = atr_splt[0]
                if rd == '-':
                    rd = ''
                doc_from =  atr_splt[1]  
                if doc_from == '-':
                    doc_from = ''
                doc_to = atr_splt[2]
                if doc_to == '-':
                    doc_to= ''
                doc_dwld_date = atr_splt[3]
                if doc_dwld_date == '-':
                    doc_dwld_date=''
                previous_release_date = atr_splt[4]
                if previous_release_date == '-':
                    previous_release_date = ''
                next_release_date = atr_splt[5]
                if next_release_date == '-':
                    next_release_date = ''
            else:
                rd = doc_release_date[:]
                doc_from = ''
                doc_to = ''
                doc_dwld_date = ''
                previous_release_date = ''
                next_release_date = ''
            #print atr_splt
            #print rd, doc_from, doc_to, doc_dwld_date, previous_release_date, next_release_date
            #print '\n'
            #print '\n' 
             
            lst_dict_rows_company_meta_info.append({'r':row_id, 'od':old_doc_name, 'dt':document_type, 'p':period, 'f':filing_type, 'rd':rd, 'd':doc_name, 'sn':sno, 'ry':reporting_year, 'sf':0, 'id':doc_id, 'dfd':doc_from, 'dtd':doc_to, 'ddd':doc_dwld_date, 'prd':previous_release_date, 'nrd':next_release_date})
            sno += 1
        #sys.exit()
        conn.close()
        dropdown_dict = self.get_dropdown_data()
        #return lst_dict_rows_company_meta_info, dropdown_dict  
        return [{'message':'done', 'data':lst_dict_rows_company_meta_info, 'dd':dropdown_dict}]

    # reading data from company_meta_info table and converting to list of dict 1101
    def get_md_info(self, ijson):#company_name, model_number, company_id):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson) 
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur   = self.get_connection(db_path)

        ## Inserting new element
        #insert_msg = self.insert_doc_information(conn, cur, company_name, model_number, company_id)

        table_path  = os.path.join(self.dbpath, company_name, model_number, self.db_tab)
        table_name  = 'company_meta_info'

        column_list = ['row_id', 'old_doc_name', 'document_type', 'filing_type', 'period', 'reporting_year', 'doc_name', 'doc_release_date', 'doc_from', 'doc_to', 'doc_download_date', 'doc_prev_release_date', 'doc_next_release_date', 'review_flg', 'doc_id']
        data_company_meta_info_tbl = self.qObj.readFromLite(conn, cur, table_path, table_name, column_list) 
   
        def change_date_format(date_str):
            try:
                d, m, y = date_str.split('-')
                dt = datetime(int(y), int(m), int(d))
                date_format = datetime.strftime(dt, "%Y-%m-%d")
                return date_format
            except:
                return date_str
 
        lst_dict_rows_company_meta_info = []
        sno = 1
        for row_tup in data_company_meta_info_tbl:
            row_id ,old_doc_name, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date, review_flg, doc_id = map(str, row_tup)
            
            ## CHANGING FORMAT
            doc_release_date        = change_date_format(doc_release_date)
            doc_from                = change_date_format(doc_from)
            doc_to                  = change_date_format(doc_to)
            doc_download_date       = change_date_format(doc_download_date)
            doc_prev_release_date   = change_date_format(doc_prev_release_date)
            doc_next_release_date   = change_date_format(doc_next_release_date)
            old_doc_name            = doc_name
             
            lst_dict_rows_company_meta_info.append({'r':row_id, 'od':old_doc_name, 'dt':document_type, 'p':period, 'f':filing_type, 'rd':doc_release_date, 'd':doc_name, 'sn':sno, 'ry':reporting_year, 'sf':0, 'id':doc_id, 'dfd':doc_from, 'dtd':doc_to, 'ddd':doc_download_date, 'prd':doc_prev_release_date, 'nrd':doc_next_release_date})
            sno += 1
        conn.close()
        dropdown_dict = self.get_dropdown_data()
        return [{'message':'done', 'data':lst_dict_rows_company_meta_info, 'dd':dropdown_dict}]

    # getting data from oper_flg =  and updating company_meta_info 1103 
    def update_md_info(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        dict_update_li = ijson.get('rd', {})

        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)

        table_name = 'company_meta_info'
        for dict_update in dict_update_li:
            rw_id = dict_update.get('r','')
            odname, doc_type, period, ff, rel_date, doc_name, reporting_year, doc_id, pre_rel_date, nxt_rel_date, from_date, to_date, download_date = dict_update.get('od',''), dict_update.get('dt',''), dict_update.get('p',''), dict_update.get('f',''), dict_update.get('rd',''), dict_update.get('d',''), dict_update.get('ry',''), dict_update.get('id',''), dict_update.get('prd',''), dict_update.get('nrd',''), dict_update.get('dfd',''), dict_update.get('dtd',''), dict_update.get('ddd','')
            rel_date, pre_rel_date, nxt_rel_date, from_date, to_date, download_date  = str(rel_date.split('T')[0]), str(pre_rel_date.split('T')[0]), str(nxt_rel_date.split('T')[0]), str(from_date.split('T')[0]), str(to_date.split('T')[0]), str(download_date.split('T')[0])

            if rw_id.strip():
                ##UPDATE
                tup_lst = [('old_doc_name', str(odname)), ('document_type', str(doc_type)), ('period', str(period)), ('filing_type', str(ff)), ('doc_release_date', str(rel_date)), ('doc_name',str(doc_name)), ('reporting_year', str(reporting_year)), ('doc_from', from_date), ('doc_to', to_date), ('doc_download_date', download_date), ('doc_prev_release_date', pre_rel_date), ('doc_next_release_date', nxt_rel_date)]
                condition = 'row_id = %s'%(rw_id)
                self.qObj.updateToLite(conn, cur, '', table_name, tup_lst, condition) 
                msg = 'done'
            else:
                ##INSERT
                if odname or doc_type or period or ff or rel_date or doc_name:
                    column_list = ['old_doc_name', 'document_type', 'filing_type', 'period', 'reporting_year', 'doc_name', 'doc_release_date', 'doc_from', 'doc_to', 'doc_download_date', 'doc_prev_release_date', 'doc_next_release_date', 'review_flg', 'doc_id']
                    ddata = [(str(odname), str(doc_type),  str(ff),  str(period), str(reporting_year), str(doc_name), rel_date, from_date, to_date, download_date, pre_rel_date, nxt_rel_date, 1, str(doc_id))]
                    msg = self.qObj.insertIntoLite(conn, cur, '', table_name, tuple(column_list), ddata) 
                else:
                    msg ='Please enter atleast 1 field'
        conn.close()
        return [{'message':'done'}]

    # getting data from oper_flg =  and updating company_meta_info 1103 
    def update_md_info_old(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        dict_update_li = ijson.get('rd', {})

        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)

        table_name = 'company_meta_info'
        #print dict_update_li; sys.exit()

        for dict_update in dict_update_li:
            rw_id = dict_update.get('r','')
            odname, doc_type, period, ff, rel_date, doc_name, reporting_year, doc_id, pre_rel_date, nxt_rel_date, from_date, to_date, download_date = dict_update.get('od',''), dict_update.get('dt',''), dict_update.get('p',''), dict_update.get('f',''), dict_update.get('rd',''), dict_update.get('d',''), dict_update.get('ry',''), dict_update.get('id',''), dict_update.get('prd',''), dict_update.get('nrd',''), dict_update.get('dfd',''), dict_update.get('dtd',''), dict_update.get('ddd','')
            rel_date, pre_rel_date, nxt_rel_date, from_date, to_date, download_date  = rel_date.split('T')[0], pre_rel_date.split('T')[0], nxt_rel_date.split('T')[0], from_date.split('T')[0], to_date.split('T')[0], download_date.split('T')[0]

            rel_date =  '@@'.join([rel_date, from_date, to_date, download_date, pre_rel_date, nxt_rel_date])
            #print rel_date;sys.exit()
            
            if rw_id.strip():
                ##UPDATE
                
                tup_lst = [('old_doc_name', str(odname)), ('document_type', str(doc_type)), ('period', str(period)), ('filing_type', str(ff)), ('doc_release_date', str(rel_date)), ('doc_name',str(doc_name)), ('reporting_year', str(reporting_year))]
                condition = 'row_id = %s'%(rw_id)
                #print tup_lst, condition
                self.qObj.updateToLite(conn, cur, '', table_name, tup_lst, condition) 
                msg = 'done'
            else:
                ##INSERT
                if odname or doc_type or period or ff or rel_date or doc_name:
                    column_list = ['old_doc_name', 'document_type', 'period', 'filing_type', 'doc_release_date', 'doc_name', 'reporting_year', 'doc_id'] 
                    ddata = [(str(odname), str(doc_type),  str(period),  str(ff),  str(rel_date),  str(doc_name),  str(reporting_year),  str(doc_id))]
                    #print ddata
                    msg = self.qObj.insertIntoLite(conn, cur, '', table_name, tuple(column_list), ddata) 
                else:
                    msg ='Please enter atleast 1 field'
        conn.close()
        #return msg
        return [{'message':'done'}]

    # getting data from oper_flg =  and updating company_meta_info 1110 
    def save_group_md_info(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        row_ids             = ijson.get('rids', [])
        update_dict         = ijson.get('ud', {})    
    
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur   = self.get_connection(db_path)

        table_name  = 'company_meta_info'
        key_map     = {'od':'old_doc_name', 'dt':'document_type', 'p':'period', 'f':'filing_type', 'rd':'doc_release_date', 
                        'd':'doc_name', 'ry':'reporting_year', 'id':'doc_id', 'dfd': 'doc_from', 'dtd': 'doc_to', 
                        'ddd': 'doc_download_date', 'prd': 'doc_prev_release_date', 'nrd':'doc_next_release_date'
                      }
   
        update_li   = []
        for k, v in update_dict.items():
            update_li.append((key_map.get(k, ''), v))
        ##UPDATE
        for rw_id in row_ids:
            condition = 'row_id = %s'%(rw_id)
            self.qObj.updateToLite(conn, cur, '', table_name, update_li, condition) 
        conn.close()
        return [{'message':'done'}]

    # getting data from oper_flg =  and updating company_meta_info 1110 
    def save_group_md_info_old(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        row_ids             = ijson.get('rids', [])
        update_dict         = ijson.get('ud', {})    
    
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)

        table_name = 'company_meta_info'
        key_map = {'od':'old_doc_name', 'dt':'document_type', 'p':'period', 'f':'filing_type', 'rd':'doc_release_date', 
                    'd':'doc_name', 'ry':'reporting_year', 'id':'doc_id', 'dfd': 'doc_from_date', 'dtd': 'doc_to_date', 
                    'ddd': 'doc_download_date', 'prd': 'prev_release_date'
                    }
   
        update_li = []
        for k, v in update_dict.items():
            if k in ('rd', 'dfd', 'ddd', 'dtd', 'prd', 'nrd'):continue
            update_li.append((key_map.get(k, ''), v))

        #msg = 'done'
        
        for rw_id in row_ids:
            doc_release_date, doc_from_date, doc_to_date = update_dict.get('rd'), update_dict.get('dfd'), update_dict.get('dtd')
            doc_download_date, prev_release_date,  next_release_date = update_dict.get('ddd'), update_dict.get('prd'), update_dict.get('nrd')
            lst_tup = update_li[:]
            condition = 'row_id = %s'%(rw_id)
            stmt = "select doc_release_date from company_meta_info where row_id = %s"%rw_id 
            cur.execute(stmt)
            drow = cur.fetchone()
            data_str = ''
            if drow and drow[0]:
                data_str = str(drow[0])
            data_str_li = data_str.split('@@')
              
            if doc_release_date == None:
                try:doc_release_date = data_str_li[0]   
                except:doc_release_date = ''
            if doc_from_date == None:
                try:doc_from_date = data_str_li[1]
                except:doc_from_date = ''
            if doc_to_date == None:
                try:doc_to_date = data_str_li[2]
                except:doc_to_date = ''
            if doc_download_date == None:
                try:doc_download_date = data_str_li[3]
                except:doc_download_date = ''
            if prev_release_date == None:
                try:prev_release_date = data_str_li[4]
                except:prev_release_date = ''
            if next_release_date == None:
                try:next_release_date = data_str_li[5]
                except:next_release_date = ''
            ##UPDATE
            dt_string = '@@'.join([doc_release_date, doc_from_date, doc_to_date, doc_download_date, prev_release_date, next_release_date])
            lst_tup.append(('doc_release_date', str(dt_string)))
            self.qObj.updateToLite(conn, cur, '', table_name, lst_tup, condition) 
        conn.close()
        #return 'done'
        return [{'message':'done'}]

    # delete respective row according to given row_id 1104 
    def delete_md_row(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        row_id = ijson.get('rid', '')

        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)
        
        table_name = 'company_meta_info'
        cond = 'row_id = %s'%(row_id)
        self.qObj.deleteSQLite(conn, cur, '', table_name, cond)
        conn.close()   
        #return 'done'
        return [{'message':'done'}]

    def insert_doc_information(self, conn, cur, company_name, model_number, company_id):
        """
        Inserting Document Information
        """
        ddict = self.get_doc_dict(company_name, company_id)

        doc_page_info, doc_name_id_dict, doc_ids_name_mapping, all_doc_names, html_type = self.cObj.get_doc_wise_pages_info(company_name,company_id,'P')
        #print all_doc_names
        #sys.exit()
        table_name  = 'company_meta_info'

        self.qObj.createLiteTable(conn, cur, '', table_name, self.md_col_list)

        column_list = ['doc_id']
        data_company_meta_info_tbl = self.qObj.readFromLite(conn, cur, '', table_name, column_list) 
    
        #print data_company_meta_info_tbl
        #sys.exit()
        all_included_docs = []
        for row in data_company_meta_info_tbl:
            doc_id = str(row[0])
            if doc_id.strip() and (doc_id not in all_included_docs):
                all_included_docs.append(doc_id)

        #print all_included_docs
        #sys.exit() 
        doc_name_rw_flg_lst = []
        for doc_info in all_doc_names:
            doc_str_li  = doc_info.split('-')
            doc_name    = '-'.join(doc_str_li[:-1])
            doc_id      = str(doc_str_li[-1])
            fdoc_name   = '.'.join([str(doc_name),'pdf'])
            if doc_id in all_included_docs:
                continue
            doc_name_rw_flg_lst.append((fdoc_name, '', '', '', '', doc_name, '', '', '', '', '', '', 1, doc_id))

        if doc_name_rw_flg_lst:
            msg = self.insert_xlsx_data(company_name, model_number, doc_name_rw_flg_lst, 0)
        else:
            msg = 'No docid to insert'
        return msg
   
    # 1109 
    def doc_type_wrtng_txt(self, ijson):#company_name, model_number, gtng_info_dict):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        gtng_info_dict         = ijson.get('dd', {})
        """
        Getting information in dict format and writing it to txt file @ /root/eMB_db
        """
        document_type, period, filing_type, reporting_year = gtng_info_dict.get('dt', ''), gtng_info_dict.get('p', ''), gtng_info_dict.get('f', ''), gtng_info_dict.get('ry', '')

        file_path = os.path.join(self.dbpath, 'md_drop_down_info.txt')

        with open(file_path, 'a') as w_f:
            w_f.write('\t'.join([document_type, period, filing_type, reporting_year]) + '\n')

        dropdown_dict = self.get_dropdown_data()
        #return 'done', dropdown_dict
        return [{'message':'done', 'dd':dropdown_dict}]

    def get_dropdown_data(self):
        file_path = os.path.join(self.dbpath, 'md_drop_down_info.txt')
        if not os.path.exists(file_path):
            return {'dt': [], 'p':[], 'f':[], 'ry':[]}
        ddict = {}

        dt_li, p_li, f_li, ry_li = [], [], [], []
        f = open(file_path, 'r')
        lines = f.readlines()
        f.close()
        for l in lines:
            lli = l.split('\t')
            document_type, period, filing_type, reporting_year = lli
            if document_type.strip() and document_type not in dt_li:
                dt_li.append(document_type.strip())
            if period.strip() and period not in p_li:
                p_li.append(period.strip())
            if filing_type.strip() and filing_type not in f_li:
                f_li.append(filing_type.strip())
            if reporting_year.strip() and reporting_year not in ry_li:
                ry_li.append(reporting_year.strip())
        ddict['dt'] = dt_li
        ddict['p'] = p_li
        ddict['f'] = f_li
        ddict['ry'] = ry_li
        return ddict 

    def get_doc_dict(self, company_name,company_id):
        doc_page_info, doc_name_id_dict, doc_ids_name_mapping, all_doc_names, html_type = self.cObj.get_doc_wise_pages_info(company_name,company_id,'P')

        ddict = {}
        for doc_info in all_doc_names:
            doc_str_li  = doc_info.split('-')
            doc_name    = '-'.join(doc_str_li[:-1])
            doc_id      = doc_str_li[-1] 

            ddict[doc_name] = doc_id
        return ddict

    def get_doc_ph_map(self, company_id):
        lmdb_folder = os.path.join(self.output_path, company_id)
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        doc_ph_map_dict = self.lmdb_obj.read_all_from_lmdb(dfname)
        return doc_ph_map_dict

    #1108
    def generate_txt(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)
        #ddict = self.get_doc_dict(company_name, company_id) #ddict, getting doc_names with respective doc_id's
        doc_ph_map_dict = self.get_doc_ph_map(company_id)
        slv_path = os.path.join(self.dbpath, 'company_client_ph_meta_info.slv')
        Cdict = self.lmdb_obj.read_from_shelve(slv_path)
        ordered_phs = Cdict.get(company_name, {}).get('all_phs', [])

        pid, deal_id = company_id.split('_')

        table_name = 'company_meta_info'
        column_list = ['old_doc_name', 'document_type', 'period', 'filing_type', 'doc_release_date', 'doc_name', 'reporting_year', 'doc_id', 'doc_from', 'doc_to', 'doc_download_date', 'doc_prev_release_date', 'doc_next_release_date']
        data_company_meta_info_tbl = self.qObj.readFromLite(conn, cur, '', table_name, column_list)
    
        dlist = []
        for row_tup in data_company_meta_info_tbl:
            old_doc_name, document_type, period, filing_type, doc_release_date, doc_name, reporting_year, doc_id, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date = map(str, row_tup)

            ## Newly Added
            ph = doc_ph_map_dict.get(str(doc_id), '')
            #if ph not in ordered_phs:
            #    continue

            if not document_type.strip():
                document_type = '-'
            if not period.strip():
                period = '-'
            if not filing_type.strip():
                filing_type = '-'
            if not doc_release_date.strip():
                doc_release_date = '-'
            if not doc_name.strip():
                doc_name = '-'
            if not reporting_year.strip():
                reporting_year = '-'
            if not doc_from.strip():
                doc_from = '-'
            if not doc_to.strip():
                doc_to = '-'
            if not doc_download_date.strip():
                doc_download_date = '-'
            if not doc_prev_release_date.strip():
                doc_prev_release_date = '-'
            if not doc_next_release_date.strip():
                doc_next_release_date = '-'
                
            dlist.append([doc_id, old_doc_name, document_type, period, filing_type, doc_release_date, doc_name, reporting_year, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date])

        header = ['Doc Id', 'Old Name', 'Document Type', 'Period', 'Filing Type', 'Document Release Date', 'Document Name', 'Financial Year', 'Doc From', 'Doc To', 'Download Date', 'Prev Release Date', 'Next Release Date']

        #txt_full_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/'%(pid, deal_id)
        
        txt_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/doc_map.txt'%(pid, deal_id)
        if machine_id == '227':
            txt_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/doc_map.txt'%(pid, deal_id)

        f = open(txt_path, 'w')
        f.write('\t'.join(header) + '\n')
        for d in dlist:
            f.write('\t'.join(d) + '\n')

        f.close()
        conn.close()
        #return 'done'  
        return [{'message':'done'}]

    #1102
    def get_training_preview_phs(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        tflg              = int(ijson.get('tflg', 0))
    
        db_path     = os.path.join(self.dbpath, company_name, str(model_number), self.db_tab)
        conn, cur = self.get_connection(db_path)
    
        ddict = self.get_doc_dict(company_name, company_id)
        doc_ph_map_dict = self.get_doc_ph_map(company_id)

        table_name = 'company_meta_info'
        column_list = ['row_id', 'old_doc_name', 'period', 'reporting_year', 'doc_id']
        md_info = self.qObj.readFromLite(conn, cur, '', table_name, column_list)
        
        phs_keyname = 'all_phs'
        if tflg == 1:
            phs_keyname = 'tas_phs'

        ## ADDED MANUALLY ARUN
        tflg = 1

        final_ph_li = []
        doc_ph_dict = {}
        row_id_dict = {}
        for row in md_info:
            row = map(str, row)
            row_id, old_doc_name, period, reporting_year, doc_id = row
            if not (period.strip() and reporting_year.strip()):continue

            doc_name = old_doc_name.split('.')[0]
            if tflg == 1:
                ph = ''.join([period, reporting_year])
                if ph not in final_ph_li:
                    final_ph_li.append(ph)
                if ph not in doc_ph_dict:
                    doc_ph_dict[ph] = []
                if doc_id not in doc_ph_dict[ph]:
                    doc_ph_dict[ph].append(doc_id)
                ## NEWLY ADDED PHS
                if not old_doc_name.strip():
                    if ph not in row_id_dict:
                        row_id_dict[ph] = row_id
            else:
                ph = reporting_year
                if ph not in final_ph_li:
                    final_ph_li.append(ph)

        slv_path = os.path.join(self.dbpath, 'company_client_ph_meta_info.slv')
        Cdict = self.lmdb_obj.read_from_shelve(slv_path)
        existing_phs = Cdict.get(company_name, {}).get(phs_keyname, [])

        #print final_ph_li

        if tflg == 1:
            final_ph_li = report_year_sort.year_sort(final_ph_li)
            final_ph_li = final_ph_li[::-1]
        else:
            final_ph_li.sort()
            final_ph_li = final_ph_li[::-1]
            existing_phs = [ph[2:] for ph in existing_phs]

        ph_data = []
        for ph in final_ph_li:
            doc_ids = doc_ph_dict.get(ph, [])
            dids = []
            
            for nd in doc_ids:
                #nd = ddict.get(doc_name, '')
                if nd.strip():
                    dfFlg = False
                    if int(nd) not in doc_ph_map_dict:
                        dfFlg = True
                    dids.append({'d':nd, 's':dfFlg})

            delFlg = 0
            #dids = [d for d in dids if d.strip()]

            rid = row_id_dict.get(ph, '')
            if not dids:
                delFlg = 1
            sf = False
            if ph in existing_phs:
                sf = True
            ph_data.append({'sf':sf, 'ph':ph, 'd':dids, 'del':delFlg, 'r':rid})
        #return ph_data
        return [{'message':'done', 'phs':ph_data}]
   
    #1105 
    def save_training_preview_phs(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        tflg              = int(ijson.get('tflg', 0))
        lst_dict_phs      = ijson.get('rd', [])
        """
        #Getting list of dict of ph,
        making a list of ph,
        #Reading Company_md_info table with dictinct periods and converting it to list 
        """
        lst_phs = []
        doc_ph_dict = {}

        for ele_dict in lst_dict_phs:
            cph = ele_dict.get('ph', '')
            cdocs = ele_dict.get('d', [])
            lst_phs.append(cph)
            for cdoc_dict in cdocs:
                cdoc = cdoc_dict['d']
                if cdoc and cdoc not in doc_ph_dict:
                    doc_ph_dict[cdoc] = cph

        final_ph_li = []
        phs_keyname = 'all_phs'
        if tflg == 1:
            phs_keyname = 'tas_phs'

        ## ADDED MANUALLY ARUN
        tflg = 1

        if not tflg:
            db_path = os.path.join(self.dbpath, company_name, model_number, self.db_tab)
            stmt = 'select distinct(period) from company_meta_info'
            conn, cur = self.get_connection(db_path)
            rows_data = self.qObj.readFromLiteComplex(conn, cur, '', stmt)
            distinct_prd_lst = []
               
            for row_tup in rows_data:
                prd = str(row_tup[0])
                distinct_prd_lst.append(prd)

            for ph in lst_phs:
                final_ph_li += [''.join([p, ph]) for p in distinct_prd_lst]
            final_ph_li = report_year_sort.year_sort(final_ph_li)
        else:
            final_ph_li = report_year_sort.year_sort(lst_phs)

        #print final_ph_li
        #print phs_keyname

        slv_path = os.path.join(self.dbpath, 'company_client_ph_meta_info.slv')
        Cdict = self.lmdb_obj.read_from_shelve(slv_path)
        if company_name not in Cdict:
            Cdict[company_name] = {}
        if phs_keyname not in Cdict[company_name]:
            Cdict[company_name][phs_keyname] = []
        Cdict[company_name][phs_keyname] = final_ph_li
        Cdict = self.lmdb_obj.write_to_shelve(slv_path, Cdict)

        #print doc_ph_dict
        lmdb_folder = os.path.join(self.output_path, company_id)
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')    
        if doc_ph_dict:
            os.system('rm -rf %s'%dfname)
        self.lmdb_obj.write_to_lmdb(dfname, doc_ph_dict, doc_ph_dict.keys()) 
        #return 'done'
        return [{'message':'done'}]

    # 1107
    def delete_newly_added_ordered_phs(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        row_id            = ijson.get('del_id', '')
        tflg              = int(ijson.get('tflg', 0))
        lst_dict_phs      = ijson.get('rd', [])
        deal_id           = ijson.get('deal_id', '')
        project_id        = ijson.get('project_id', '')
        machine_id        = ijson.get('machine_id', '')

        #delete from table 
        del_json = {"company_name":company_name, "model_number":model_number, "deal_id":deal_id, 'project_id':project_id, 'machine_id':machine_id, 'rid':row_id}
        del_msg = self.delete_md_row(del_json)
        # Save into slv file
        save_json = {"company_name":company_name, "model_number":model_number, "deal_id":deal_id, 'project_id':project_id, 'machine_id':machine_id, 'rd':lst_dict_phs, 'tflg':tflg} 
        #save_msg = self.save_training_preview_phs(company_name, model_number, company_id, lst_dict_phs, tflg)
        save_msg  = self.save_training_preview_phs(save_json)
        #return save_msg
        return [{'message':'done'}]
    
    # 1106
    def update_doc_ph_rel_info_lmdb(self, ijson):
        company_name, model_number, company_id, machine_id = self.get_ijson_data(ijson)
        doc_li            = ijson.get('doc_li', [])
        doc_ph_map_dict = self.get_doc_ph_map(company_id)
        #doc_li = [int(x) for x in doc_li]
        updated_doc_ph_map_dict = {}
        for docid, ph in doc_ph_map_dict.items():
            if docid not in doc_li:
                continue
            updated_doc_ph_map_dict[docid] = ph
        ## WRITING
        #print updated_doc_ph_map_dict
        lmdb_folder = os.path.join(self.output_path, company_id)
        dfname = os.path.join(lmdb_folder, 'doc_ph_info')
        self.lmdb_obj.write_to_lmdb(dfname, updated_doc_ph_map_dict, updated_doc_ph_map_dict.keys())
        #return 'done'
        return [{'message':'done'}]

if __name__ == '__main__':
    obj = Metadata() 
