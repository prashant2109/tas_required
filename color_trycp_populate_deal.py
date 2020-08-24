import os, sys, sqlite3, logging, subprocess, datetime, copy
from collections import OrderedDict as OD
from model_view.cp_company_docTablePh_details import Company_docTablePh_details
sObj = Company_docTablePh_details()

sys.path.append('/root/prashant/')
import TextFormatter as TF
txt_color = TF.TextFormatter()
txt_color.cfg('y', 'r', 'b')

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

import traceback
def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line

class Populate_deal(object):
    def __init__(self):
        self.dbinfo  = {'user':'root', 'password':'tas123', 'host':'172.16.20.229', 'db':'populate_info'}

    def data_new_old_doc_info(self, company_id, doc_ids, table_ids):
        doc_str = ', '.join(["'"+ str(e) +"'" for e in doc_ids.keys()])
        company_db = {'user':'root', 'password':'tas123', 'host':'172.16.20.229', 'db':'tfms_urlid_%s'%(company_id)} 
        conn, cur = self.get_connection(company_db) 
        ex_flg = 0
        try:
            read_qry = """ SELECT docid, norm_resid, source_table_info FROM norm_data_mgmt WHERE docid in (%s); """%(doc_str)        
            cur.execute(read_qry)
        except:
            read_qry = """ SELECT docid, norm_resid FROM norm_data_mgmt WHERE docid in (%s); """%(doc_str)
            print read_qry
            cur.execute(read_qry)
            ex_flg = 1

        t_data = cur.fetchall()
        conn.close()
        new_inc, old_inc = {}, {}
        new_table, old_table = {}, {}
        for row in t_data:
            if not ex_flg:
                docid, norm_resid, source_table_info = row
                if source_table_info:
                    new_inc[str(docid)] = 1
                    if table_ids and str(norm_resid) in table_ids:
                        new_table[str(norm_resid)] = 1
                elif not source_table_info and company_id not in ('1_116', '1_86', '1_22', '1_261', '1_159', '1_163', '1_50', '1_71', '1_79'):
                    old_inc[str(docid)] = 1
                    if table_ids and str(norm_resid) in table_ids:
                        old_table[str(norm_resid)] = 1
            elif ex_flg:
                docid, norm_resid  = row
                old_inc[str(docid)] = 1
                if table_ids and str(norm_resid) in table_ids:
                    old_table[str(norm_resid)] = 1
        return new_inc, old_inc, new_table, old_table

    def get_connection(self, dbinfo):
        return conn_obj.MySQLdb_connection(dbinfo)

    def update_stage_status_all(self, prof_ids, flag):
        conn, cur = self.get_connection(self.dbinfo)
        stages  = []    
        for r in range(0,20):
            stages.append('stage%s="%s"'%(r, flag))
        qry	= "update populate_status set %s where company_id in(%s)"%(', '.join(stages), ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_stage_status(self, prof_ids, flag, stage, error_message=''):
        conn, cur = self.get_connection(self.dbinfo)
        qry	= """update populate_status set %s ='%s', error_message='%s' where company_id in(%s)"""%(stage, flag, error_message, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_status(self, prof_ids, flag):
        conn, cur = self.get_connection(self.dbinfo)
        qry	= "update populate_status set status ='%s' where company_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_doc_table_inc(self, flag, prof_ids):
        conn, cur = self.get_connection(self.dbinfo)
        qry	= "update company_doc_table_info set from_inc ='%s', process_time=Now() where row_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_doc_table(self, flag, prof_ids):
        conn, cur = self.get_connection(self.dbinfo)
        qry	= "update company_doc_table_info set status ='%s', process_time=Now() where row_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def both_docs_and_tables_only_html(self, doc_ids, table_ids, meta_tup, lang, src_db_name, src_project_id):
        company_id, company_name, machine_id, project_id, url_id    = meta_tup
        logging.basicConfig(filename='%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
        #populating new deal
        if 1:
            self.update_stage_status([company_id], 'P', 'stage1')
            print 'Stage 1 ....'
            print lang, type(lang)
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs_60.py %s %s %s'%(company_id, doc_ids, table_ids)
            if lang:
                cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/lang_populate_populate_multi_docs_60.py %s %s %s %s'%(company_id, doc_ids, lang, table_ids)
            process = subprocess.Popen(cmd_populating_new_deal, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                self.update_stage_status([company_id], 'E', 'stage1')
                #print 'check error in %s_error.log'%(company_id)
                logging.info('************* HTML Creation error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage1')
        
        #updating table indices
        if 1:
            self.update_stage_status([company_id], 'P', 'stage2')
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py Table %s'%(company_id)
            process = subprocess.Popen(cmd_updating_table_indices, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage2')
                logging.info('************* table_indices **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage2')
         
        self.update_stage_status([company_id], 'Y', 'stage3')
    
        self.update_stage_status([company_id], 'Y', 'stage4')
    
        self.update_stage_status([company_id], 'Y', 'stage5')
        
        self.update_stage_status([company_id], 'Y', 'stage6')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage7')
            cmd_idx = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py %s %s'%(company_id, table_ids)
            process = subprocess.Popen(cmd_idx, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage7')
                logging.info('*************** index_all_table **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage7')

        # triplet 
        if 1:
            self.update_stage_status([company_id], 'P', 'stage8')
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/cp_wrap_gen_default_trip_60.py %s %s'%(company_id, table_ids)
            print [cmd_triplet]
            process = subprocess.Popen(cmd_triplet, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage8')
                logging.info('*************** triplet **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage8')
        if 1:
            self.update_stage_status([company_id], 'P', 'stage9')
            import populate_projectid20_deals as py
            print 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD'
            try:
                pObj        = py.Populate_deal(company_id, src_project_id)
                pObj.read_all_docs(doc_ids.split('#'), db_name=src_db_name, inc_pid=src_project_id)  
                self.update_stage_status([company_id], 'Y', 'stage9')
            except:
                self.update_stage_status([company_id], 'E', 'stage9')

        self.update_stage_status([company_id], 'Y', 'stage10')
        self.update_stage_status([company_id], 'Y', 'stage11')
        self.update_stage_status([company_id], 'Y', 'stage12')
        self.update_stage_status([company_id], 'Y', 'stage13')
        self.update_stage_status([company_id], 'Y', 'stage14')
        self.update_stage_status([company_id], 'Y', 'stage15')
        self.update_stage_status([company_id], 'Y', 'stage16')
        self.update_stage_status([company_id], 'Y', 'stage17')
        self.update_stage_status([company_id], 'Y', 'stage18')
        self.update_stage_status([company_id], 'Y', 'stage19')
        return 'done' 
    
    def both_docs_and_tables(self, doc_ids, table_ids, meta_tup, lang):
        company_id, company_name, machine_id, project_id, url_id    = meta_tup
        logging.basicConfig(filename='%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
        #populating new deal
        if 1:
            self.update_stage_status([company_id], 'P', 'stage1')
            print 'Stage 1 ....'
            print lang, type(lang)
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s %s'%(company_id, doc_ids)
            if lang:
                cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs_a126.py %s %s %s'%(company_id, lang, doc_ids)
            process = subprocess.Popen(cmd_populating_new_deal, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                self.update_stage_status([company_id], 'E', 'stage1')
                #print 'check error in %s_error.log'%(company_id)
                logging.info('************* HTML Creation error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage1')
        
        #updating table indices
        if 1:
            self.update_stage_status([company_id], 'P', 'stage2')
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py Table %s'%(company_id)
            process = subprocess.Popen(cmd_updating_table_indices, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage2')
                logging.info('************* table_indices **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage2')
         
        #generate bbox
        if 1:
            self.update_stage_status([company_id], 'P', 'stage3')
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(company_id)
            process = subprocess.Popen(cmd_bbox, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage3')
                logging.info('************* Gen_Coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage3')
    
        #bbox
        if 1:
            self.update_stage_status([company_id], 'P', 'stage4')
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(company_id)
            process = subprocess.Popen(cmd_bbox2, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage4')
                logging.info('************* generating coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage4')
    
        #PDF
        if 1:
            self.update_stage_status([company_id], 'P', 'stage5')
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(company_id)
            process = subprocess.Popen(cmd_pdf, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage5')
                logging.info('************ pdf split **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage5')
        
        # create_table_seq muthu 
        if 1:
            self.update_stage_status([company_id], 'P', 'stage6')
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/create_table_seq.py 11 %s %s'%(company_id, machine_id)
            process = subprocess.Popen(cmd_table_seq, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage6')
                logging.info('************ create_table_seq **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage6')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage7')
            cmd_idx = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py %s %s'%(company_id, 'ALL')
            process = subprocess.Popen(cmd_idx, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage7')
                logging.info('*************** index_all_table **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage7')

        #system groups
        if 0:
            sys_grps = 'python /root/Akshay/generate_system_groups.py %s %s'%(company_id, table_ids)
            process = subprocess.Popen(sys_grps, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                logging.info('*************** System Groups  **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                sys.exit()
        
        # Index_tag
        if 0: 
            #cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(company_id)
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_tag_pyapi.py %s %s %s %s'%(company_name, project_id, url_id, company_id)
            process = subprocess.Popen(cmd_indextag, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                logging.info('*************** Index Tag **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                sys.exit()
   
        # triplet 
        if 1:
            self.update_stage_status([company_id], 'P', 'stage8')
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(url_id, table_ids)
            print [cmd_triplet]
            process = subprocess.Popen(cmd_triplet, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage8')
                logging.info('*************** triplet **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage8')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage9')
            cmd_currency_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s CurrencyPattern_CSV %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_currency_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                err_data = err_data.replace("'", '"')
                self.update_stage_status([company_id], 'E', 'stage9', str(err_data))
                logging.info('*************** Currency_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage9')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage10')
            cmd_date_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Date_Pattern %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_date_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage10')
                logging.info('*************** Date_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage10')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage11')
            cmd_pattern_periodend = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Pattern_PeriodEnd %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_pattern_periodend, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage11')
                logging.info('*************** Pattern_PeriodEnd **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage11')
            
        if 1:
            self.update_stage_status([company_id], 'P', 'stage12')
            cmd_BNUM_PATTERNS = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_PATTERNS %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_PATTERNS, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage12')
                logging.info('*************** BNUM_PATTERNS **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage12')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage13')
            cmd_measurement_patterns = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Measurement_Patterns %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_measurement_patterns, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage13')
                logging.info('*************** Measurement_Patterns **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage13')
        if 1:
            self.update_stage_status([company_id], 'P', 'stage14')
            cmd_BNUM_marker_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_Marker_Pattern %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_marker_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage14')
                logging.info('*************** BNUM_Marker_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage14')
        if 1:
            self.update_stage_status([company_id], 'P', 'stage15')
            cmd_ph_entities = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s PH_entities %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_ph_entities, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage15')
                logging.info('*************** PH_entities **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage15')
        if 1:
            self.update_stage_status([company_id], 'P', 'stage16')
            cmd_wrap_rule_gen_new = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen_new.py %s'%(url_id) 
            process = subprocess.Popen(cmd_wrap_rule_gen_new, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage16')
                logging.info('*************** wrap_rule_gen_new **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage16')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage17')
            cmd_csv_wrapper = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/csv_wrapper.py %s %s'%(url_id, table_ids) 
            process = subprocess.Popen(cmd_csv_wrapper, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage17')
                logging.info('*************** csv_wrapper **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage17')

        if 1:
            self.update_stage_status([company_id], 'P', 'stage18')
            cmd_cols = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py 37 %s \'{"table_str":"%s"}\' > /dev/null; cd -'%(company_id, table_ids) 
            process = subprocess.Popen(cmd_cols, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage18')
                logging.info('*************** Generate Cols **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage18')
        if 1:
            self.update_stage_status([company_id], 'P', 'stage19')
            cmd_run_db_init = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python run_db_init.py %s > /dev/null; cd -'%(url_id) 
            process = subprocess.Popen(cmd_run_db_init, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                #print 'check error in %s_error.log'%(company_id)
                self.update_stage_status([company_id], 'E', 'stage19')
                logging.info('*************** run_db_init **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                txt_color.out('check error in %s_error.log'%(company_id))
                xxxxxxxxxx #sys.exit()
            else:
                self.update_stage_status([company_id], 'Y', 'stage19')
        return 'done' 
        
    def create_input_info(self, ijson, project_id, deal_id):
        table_id, doc_id, page_no, grid_id, sdb_name, sproject_id = ijson['table_id'], ijson['doc_id'], ijson['page_no'], ijson['grid_id'], ijson['db_name'], ijson['inc_pid']
        data = ((0, doc_id, table_id, '', 'Y', sdb_name, sproject_id), )
        return data
        
    def populate(self, company_id, ijson={}):
        print company_id
        getCompanyName_machineId = sObj.get_company_name_MID()
        company_name, machine_id    = getCompanyName_machineId[company_id]
        project_id, url_id          = company_id.split('_')
        self.update_status([company_id], 'P')
        if not ijson:
            conn, cur = self.get_connection(self.dbinfo)
            sql = "select row_id, doc_id, table_id, language_info, from_inc, src_db_name, src_project_id from company_doc_table_info where company_id= '%s' and status='N' "%(company_id)
            cur.execute(sql)
            res = cur.fetchall()
        elif ijson:
            res = self.create_input_info(ijson, project_id, url_id) 
        doc_ids     = {}
        table_ids   = {}
        lang   = ''
        rids    = {}
        inc_docs    = {}
        
        src_db_name, src_project_id = '', '' 
        for r in res:
            row_id, doc_id, table_id, language_info, from_inc, sdb_name, sproject_id  = r
            if not src_db_name:
                src_db_name = sdb_name
                src_project_id = sproject_id
            #print row_id
            if doc_id:
                for d in doc_id.split('#'):
                    if from_inc == 'Y':
                        inc_docs.setdefault(str(d), {})
                    doc_ids[str(d)]    = 1
            if table_id:
                for t in table_id.split('#'):
                    if from_inc == 'Y':
                        if len(t.split('-')) == 2:
                            inc_docs.setdefault(str(d), {}).setdefault(t.split('-')[0], {})[t.split('-')[1]]    = 1
                    else:
                        table_ids[str(t)]    = 1
            rids[str(row_id)]    = 1
            if language_info:
                lang = copy.deepcopy(language_info)
        print rids.keys(), inc_docs
        #sys.exit()
        self.update_stage_status_all([company_id], 'N')
        if inc_docs:
            self.update_stage_status([company_id], 'P', 'stage0')
            import auto_inc_projectid20#mp_pdf_upload_auto_inc_20
            mo_obj  = auto_inc_projectid20.BDS_Data()
            if 1:
            #try:
                table_ids, gh_d   = mo_obj.data_preparation_bds(company_name, inc_docs.keys(), company_id, inc_docs, dbname=src_db_name, project_id=src_project_id)
                #sys.exit()
                self.update_stage_status([company_id], 'Y', 'stage0')
                #table_ids, inc_docs.keys(
                import classification_insert_new_inc as pyf
                s_Obj = pyf.PYAPI()
                print table_ids
                print ':::::::::::::::::::::::::::', inc_docs.keys(), [src_db_name]
                
                s_Obj.insert_auto_inc_classification(company_id, '#'.join(inc_docs.keys()), table_ids, [], src_db_name, gh_d)
            #except:
            #    self.update_stage_status([company_id], 'E', 'stage0')
            #    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            self.update_doc_table_inc('D', rids.keys())
        self.update_stage_status([company_id], 'Y', 'stage0')
         
        print (company_id, doc_ids, table_ids)
        new_inc, old_inc, new_table, old_table = self.data_new_old_doc_info(company_id, doc_ids, table_ids)        
        print 'KKKKKKKKKKKKKKKKKKKKKKKKKKKKKK', new_inc, old_inc, new_table, old_table
        try:
            self.update_doc_table('P', rids.keys())
            #if str(project_id) == '1':
            #    self.both_docs_and_tables('#'.join(doc_ids.keys()), '#'.join(table_ids.keys()), (company_id, company_name, machine_id, project_id, url_id), lang)
            #else:
            #    self.both_docs_and_tables_only_html('#'.join(doc_ids.keys()), '#'.join(table_ids.keys()), (company_id, company_name, machine_id, project_id, url_id), lang)
            if old_inc:#str(project_id) == '1':
                self.both_docs_and_tables('#'.join(old_inc.keys()), '#'.join(old_table.keys()), (company_id, company_name, machine_id, project_id, url_id), lang)
            if new_inc:
                self.both_docs_and_tables_only_html('#'.join(new_inc.keys()), '#'.join(new_table.keys()), (company_id, company_name, machine_id, project_id, url_id), lang, src_db_name, src_project_id)
            
                pass
            self.update_doc_table('Y', rids.keys())
            self.update_status([company_id], 'Y')
        except:
            print 'Error ',[company_id]
            print_exception()
            self.update_doc_table('E', rids.keys())
            self.update_status([company_id], 'E')
        print 'DONE FINAL'
        return [{'message':'done'}]
        
        

if __name__ == '__main__':
    obj = Populate_deal()

    obj.populate(sys.argv[1])
