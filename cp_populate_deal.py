import os, sys, sqlite3
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
        # populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s'%(self.company_id)
            try:
                response_populating_new_deal = os.system(cmd_populating_new_deal)
            except:sys.exit()
        
        # generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            try:
                response_bbox = os.system(cmd_bbox)
            except:sys.exit()

        # updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py Table %s'%(self.url_id)
            try:
                response_updating_table_indices = os.system(cmd_updating_table_indices)
            except:sys.exit()
    
        #bbox
        if 1:
            cmd_bbox2 = 'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            try:
                resp = os.system(cmd_bbox2)
            except:sys.exit()
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            try:
                os.system(cmd_pdf)
            except:sys.exit()
   
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            try:
                os.system(cmd_table_seq)
            except:sys.exit()
        if 1:
            try:
                os.system('python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py %s %s'%(self.company_id, 'ALL'))
            except:sys.exit()

        # systems groups 
        if 1:
            os.system('python /root/Akshay/generate_system_groups.py %s'%(self.company_id))       

        # Index_tag
        if 1: 
            #cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/index_tag_pyapi.py %s %s %s %s'%(self.company_name, self.project_id, self.url_id, self.company_id)
            try:
                os.system(cmd_indextag)
            except:sys.exit()
       
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
            try:
                os.system(cmd_triplet)
            except:sys.exit()
   
        # phcsv 
        if 1:
            cmd_phcsv = 'sh /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/rule_gen_sh.sh %s %s'%(self.url_id, 'ALL')
            try:
                os.system(cmd_phcsv)
            except:sys.exit()
        return 'done'
    
    def both_docs_and_tables(self, doc_ids, table_ids):
        #populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s %s'%(self.company_id, doc_ids)
            response_populating_new_deal = os.system(cmd_populating_new_deal)
        
        #updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py Table %s'%(self.url_id)
            response_updating_table_indices = os.system(cmd_updating_table_indices)
         
        #generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            response_bbox = os.system(cmd_bbox)
    
        #bbox
        if 1:
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            resp = os.system(cmd_bbox2)
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            os.system(cmd_pdf)
        
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            os.system(cmd_table_seq)
        if 0:
            os.system('python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py %s %s'%(self.company_id, table_ids))

        #system groups
        if 1:
            os.system('python /root/Akshay/generate_system_groups.py %s %s'%(self.company_id, table_ids))          
        
        # Index_tag
        if 1: 
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            os.system(cmd_indextag)
        # triplet 
        if 1:
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(self.url_id, table_ids)
            os.system(cmd_triplet)
        # phcsv 
        if 1:
            cmd_phcsv = 'sh /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/rule_gen_sh.sh %s %s'%(self.url_id, table_ids)
            os.system(cmd_phcsv)
        return 'done' 


    def if_only_docs(self, doc_ids):    
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
            response_populating_new_deal = os.system(cmd_populating_new_deal)
        
        #updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py Table %s'%(self.url_id)
            response_updating_table_indices = os.system(cmd_updating_table_indices)
         
        #generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            response_bbox = os.system(cmd_bbox)
    
        #bbox
        if 1:
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            resp = os.system(cmd_bbox2)
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            os.system(cmd_pdf)
        
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            os.system(cmd_table_seq)
        if 0:
            os.system('python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py %s %s'%(self.company_id, table_ids))

        #system groups
        if 1:
            os.system('python /root/Akshay/generate_system_groups.py %s %s'%(self.company_id, table_ids))          
        
        # Index_tag
        if 1: 
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            os.system(cmd_indextag)
   
        # triplet 
        if 1:
            cmd_triplet = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/wrap_gen_default_trip.py %s %s'%(self.url_id, table_ids)
            os.system(cmd_triplet)

        # phcsv 
        if 1:
            cmd_phcsv = 'sh /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/rule_gen_sh.sh %s %s'%(self.url_id, table_ids)
            os.system(cmd_phcsv)
        return 'done' 
        
    def if_only_tables(self, table_ids):
        ################################
        # get doc_ids passing table_ids
        table_doc_map = sObj.get_docId_passing_tableId(self.company_id)
        doc_lst = []
        for table in table_ids.split('#'):
            doc_id  = table_doc_map[table]
            doc_lst.append(doc_id)
        doc_ids = '#'.join(list(OD.fromkeys(doc_lst)))
        ################################
        
        #populating new deal
        if 1:
            cmd_populating_new_deal = 'python /var/www/cgi-bin/table_delta/populate_multi_docs.py %s %s'%(self.company_id, doc_ids)
            response_populating_new_deal = os.system(cmd_populating_new_deal)
        
        #updating table indices
        if 1:
            cmd_updating_table_indices = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/index_all_table.py Table %s'%(self.url_id)
            response_updating_table_indices = os.system(cmd_updating_table_indices)
         
        #generate bbox
        if 1:
            cmd_bbox = 'python /var/www/cgi-bin/table_delta/gen_coordinates.py %s'%(self.company_id)
            response_bbox = os.system(cmd_bbox)
    
        #bbox
        if 1:
            cmd_bbox2 =  'python /root/databuilder_train_ui/tenkTraining/Table_Tagging_Training_V2/pysrc/pwrapper.py %s 14'%(self.company_id)
            resp = os.system(cmd_bbox2)
    
        #PDF
        if 1:
            cmd_pdf  = 'python /var/www/html/muthu/PDF_split/run_allpdf.py %s'%(self.url_id)
            os.system(cmd_pdf)
        
        # create_table_seq muthu 
        if 1:
            cmd_table_seq = 'python /root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc/create_table_seq.py 11 %s %s'%(self.url_id, self.machine_id)
            os.system(cmd_table_seq)

        #system groups
        if 1:
            os.system('python /root/Akshay/generate_system_groups.py %s %s'%(self.company_id, table_ids))          
        
        # Index_tag
        if 1: 
            cmd_indextag = 'python /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/Index_tag.py %s'%(self.company_id)
            os.system(cmd_indextag)
    
        # phcsv 
        if 0:
            cmd_phcsv = 'sh /root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/rule_gen_sh.sh %s %s'%(self.url_id, table_ids)
            os.system(cmd_phcsv)

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
    
