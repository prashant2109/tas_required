import os, sys, sqlite3, logging, subprocess, datetime
from collections import OrderedDict as OD
from model_view.cp_company_docTablePh_details import Company_docTablePh_details
sObj = Company_docTablePh_details()
getCompanyName_machineId = sObj.getCN_MID()
class Populate_deal(object):
    
    def __init__(self, company_id):
        self.company_id                       = company_id
        self.company_name, self.machine_id    = getCompanyName_machineId[company_id]
        self.project_id, self.url_id          = company_id.split('_')
        
    def populate_whole_deal(self):
        logging.basicConfig(filename='%s_error.log'%(self.company_id), filemode='w', format='%(message)s', level=logging.INFO)        
         
        # populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s'%(self.company_id)
            process = subprocess.Popen(cmd_populating_new_deal, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* HTML Creation error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
            
        # updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py Table %s'%(self.url_id)
            process = subprocess.Popen(cmd_updating_table_indices, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* table_indices **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()

        # generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            process = subprocess.Popen(cmd_bbox, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* Gen_Coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
               
 
        #bbox
        if 1:
            cmd_bbox2 = 'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            process = subprocess.Popen(cmd_bbox2, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* generating coordinates pwrapper **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            process = subprocess.Popen(cmd_pdf, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ pdf split **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
   
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            process = subprocess.Popen(cmd_table_seq, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ create_table_seq **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_idx = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py %s %s'%(self.company_id, 'ALL')
            process = subprocess.Popen(cmd_idx, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** index_all_table **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()

        # systems groups 
        if 1:
            sys_grps = 'python /root/Akshay/generate_system_groups.py %s'%(self.company_id)
            process = subprocess.Popen(sys_grps, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** System Groups **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()

        # Index_tag
        if 1: 
            #cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_tag_pyapi.py %s %s %s %s'%(self.company_name, self.project_id, self.url_id, self.company_id)
            process = subprocess.Popen(cmd_indextag, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Index Tag **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
       
        # make directory 
        if 1:
            path = '/mnt/eMB_db/%s/%s/table_tagging_dbs/'%(self.company_name, '1')
            if not os.path.exists(path):
                cmd_dir = os.system('mkdir -p %s'%(path))

        if 1:
            path = '/mnt/eMB_db/%s/%s/tas_company.db'%(self.company_name, '1')
            conn = sqlite3.connect(path)
            cur  = conn.cursor()
            crt_qry = 'CREATE TABLE IF NOT EXISTS table_group_mapping(row_id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id INTEGER, doc_id VARCHAR(256), doc_name VARCHAR(250), table_id VARCHAR(250), parent_table_type TEXT)'
            cur.execute(crt_qry)
            conn.close()

        # triplet        
        if 1:
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(self.url_id, 'ALL')
            process = subprocess.Popen(cmd_triplet, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** triplet **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
   
        # phcsv 
        if 0:
            cmd_phcsv = 'sh /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/rule_gen_sh.sh %s %s'%(self.url_id, 'ALL')
            process = subprocess.Popen(cmd_phcsv, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Phcsv Error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_currency_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s CurrencyPattern_CSV %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_currency_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Currency_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_date_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Date_Pattern %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_date_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Date_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_pattern_periodend = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Pattern_PeriodEnd %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_pattern_periodend, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Pattern_PeriodEnd **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_PATTERNS = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_PATTERNS %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_BNUM_PATTERNS, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_PATTERNS **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_measurement_patterns = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Measurement_Patterns %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_measurement_patterns, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Measurement_Patterns **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_marker_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_Marker_Pattern %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_BNUM_marker_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_Marker_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_ph_entities = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s PH_entities %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_ph_entities, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** PH_entities **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_wrap_rule_gen_new = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen_new.py %s'%(self.url_id) 
            process = subprocess.Popen(cmd_wrap_rule_gen_new, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** wrap_rule_gen_new **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_csv_wrapper = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/csv_wrapper.py %s %s'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_csv_wrapper, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** csv_wrapper **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_cols = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py 37 %s \'{"table_str":"%s"}\' > /dev/null; cd -'%(self.url_id, 'ALL') 
            process = subprocess.Popen(cmd_cols, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Generate Cols **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_run_db_init = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python run_db_init.py %s; cd -'%(self.url_id) 
            process = subprocess.Popen(cmd_run_db_init, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** run_db_init **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        return 'done'
    
    def both_docs_and_tables(self, doc_ids, table_ids):
        logging.basicConfig(filename='%s_error.log'%(self.company_id), filemode='a', format='%(message)s', level=logging.INFO)        
        #populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s %s'%(self.company_id, doc_ids)
            process = subprocess.Popen(cmd_populating_new_deal, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* HTML Creation error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        #updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py Table %s'%(self.url_id)
            process = subprocess.Popen(cmd_updating_table_indices, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* table_indices **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
         
        #generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            process = subprocess.Popen(cmd_bbox, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* Gen_Coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
    
        #bbox
        if 1:
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            process = subprocess.Popen(cmd_bbox2, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* generating coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            process = subprocess.Popen(cmd_pdf, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ pdf split **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            process = subprocess.Popen(cmd_table_seq, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ create_table_seq **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        if 1:
            cmd_idx = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py %s %s'%(self.company_id, table_ids)
            process = subprocess.Popen(cmd_idx, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** index_all_table **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        #system groups
        if 1:
            sys_grps = 'python /root/Akshay/generate_system_groups.py %s %s'%(self.company_id, table_ids)
            process = subprocess.Popen(sys_grps, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** System Groups  **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        # Index_tag
        if 1: 
            #cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_tag_pyapi.py %s %s %s %s'%(self.company_name, self.project_id, self.url_id, self.company_id)
            process = subprocess.Popen(cmd_indextag, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Index Tag **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
   
        # triplet 
        if 1:
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(self.url_id, table_ids)
            process = subprocess.Popen(cmd_triplet, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** triplet **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        if 1:
            cmd_currency_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s CurrencyPattern_CSV %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_currency_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Currency_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_date_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Date_Pattern %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_date_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Date_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_pattern_periodend = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Pattern_PeriodEnd %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_pattern_periodend, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Pattern_PeriodEnd **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_PATTERNS = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_PATTERNS %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_PATTERNS, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_PATTERNS **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_measurement_patterns = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Measurement_Patterns %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_measurement_patterns, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Measurement_Patterns **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_marker_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_Marker_Pattern %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_marker_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_Marker_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_ph_entities = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s PH_entities %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_ph_entities, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** PH_entities **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_wrap_rule_gen_new = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen_new.py %s'%(self.url_id) 
            process = subprocess.Popen(cmd_wrap_rule_gen_new, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** wrap_rule_gen_new **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_csv_wrapper = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/csv_wrapper.py %s %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_csv_wrapper, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** csv_wrapper **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_cols = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py 37 %s \'{"table_str":"%s"}\' > /dev/null; cd -'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_cols, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Generate Cols **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_run_db_init = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python run_db_init.py %s > /dev/null; cd -'%(self.url_id) 
            process = subprocess.Popen(cmd_run_db_init, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** run_db_init **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        return 'done' 


    def if_only_docs(self, doc_ids):    
        logging.basicConfig(filename='%s_error.log'%(self.company_id), filemode='a', format='%(message)s', level=logging.INFO)        
        ###################
        # get tables list passing doc_ids
        get_tableList_passing_doc = sObj.get_tableList_passing_doc(self.company_id)
        table_lst = []
        for doc_id in doc_ids.split('#'):
            getTableList = get_tableList_passing_doc[doc_id]
            table_lst += getTableList
        table_ids = '#'.join(table_lst)
        ###################
        #populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s %s'%(self.company_id, doc_ids)
            process = subprocess.Popen(cmd_populating_new_deal, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* HTML Creation error **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        #updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py Table %s'%(self.url_id)
            process = subprocess.Popen(cmd_updating_table_indices, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* table_indices **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
         
        #generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            process = subprocess.Popen(cmd_bbox, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* Gen_Coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
    
        #bbox
        if 1:
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            process = subprocess.Popen(cmd_bbox2, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************* generating coordinates **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            process = subprocess.Popen(cmd_pdf, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ pdf split **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            process = subprocess.Popen(cmd_table_seq, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('************ create_table_seq **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        if 1:
            cmd_idx = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_all_table.py %s %s'%(self.company_id, table_ids)
            process = subprocess.Popen(cmd_idx, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** index_all_table **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        #system groups
        if 1:
            sys_grps = 'python /root/Akshay/generate_system_groups.py %s %s'%(self.company_id, table_ids)
            process = subprocess.Popen(sys_grps, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** System Groups  **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        
        # Index_tag
        if 1: 
            #cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_tag_pyapi.py %s %s %s %s'%(self.company_name, self.project_id, self.url_id, self.company_id)
            process = subprocess.Popen(cmd_indextag, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Index Tag **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
   
        # triplet 
        if 1:
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(self.url_id, table_ids)
            process = subprocess.Popen(cmd_triplet, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** triplet **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()

        if 1:
            cmd_currency_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s CurrencyPattern_CSV %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_currency_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Currency_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_date_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Date_Pattern %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_date_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Date_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_pattern_periodend = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Pattern_PeriodEnd %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_pattern_periodend, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Pattern_PeriodEnd **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_PATTERNS = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_PATTERNS %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_PATTERNS, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_PATTERNS **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_measurement_patterns = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s Measurement_Patterns %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_measurement_patterns, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Measurement_Patterns **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_BNUM_marker_pattern = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s BNUM_Marker_Pattern %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_BNUM_marker_pattern, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** BNUM_Marker_Pattern **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_ph_entities = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen.py %s PH_entities %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_ph_entities, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** PH_entities **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_wrap_rule_gen_new = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_rule_gen_new.py %s'%(self.url_id) 
            process = subprocess.Popen(cmd_wrap_rule_gen_new, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** wrap_rule_gen_new **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_csv_wrapper = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/csv_wrapper.py %s %s'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_csv_wrapper, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** csv_wrapper **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_cols = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python web_api.py 37 %s \'{"table_str":"%s"}\'; cd -'%(self.url_id, table_ids) 
            process = subprocess.Popen(cmd_cols, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** Generate Cols **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        if 1:
            cmd_run_db_init = 'cd /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc; python run_db_init.py %s; cd -'%(self.url_id) 
            process = subprocess.Popen(cmd_run_db_init, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            err_data = error[1]
            if err_data:
                print 'check error in %s_error.log'%(self.company_id)
                logging.info('*************** run_db_init **************')
                dt = str(datetime.datetime.now())
                logging.info(dt)
                logging.info(table_ids)
                logging.info(err_data)
                sys.exit()
        return 'done' 
        

if __name__ == '__main__':
    # eg: python populate_deal.py --company_id=1_219 --doc_id=21#32#43 --table_id=2133#3243#5465
    # eg: python populate_deal.py -c=1_192 -d=21#32#43 -t=2133#3243#5465

    # command line keyword arguments
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-c', '--company_id', dest='company_id', help='Its a unique company id')    
    parser.add_argument('-d', '--doc_id', dest='doc_id', help='Distinct doc ids joined with # for respective company id')
    parser.add_argument('-t', '--table_id', dest='table_id', help='Distinct table ids joined with # for respective company id')

    args = parser.parse_args()
    
    company_id  = args.company_id
    doc_ids     = args.doc_id
    table_ids   = args.table_id
    
    pObj        = Populate_deal(company_id)

    if (doc_ids == None) and (table_ids == None):
        pObj.populate_whole_deal()
    
    elif (doc_ids != None) and (table_ids != None):
        pObj.both_docs_and_tables(doc_ids, table_ids)
    
    elif (doc_ids != None) and (table_ids == None):
        pObj.if_only_docs(doc_ids)
    
