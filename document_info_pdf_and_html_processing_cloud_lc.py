import os, sys, subprocess
import urllib
import json, ast
import MySQLdb as mydb
import datetime, time
import shutil
import MultiProcessing as multi_process
import MultiProcessing_v1 as multi_process2
import socket


class ProcessDocuments:

    def __init__(self, inp_dir, out_dir, stage_tym_dict, epath): 

        self.inp_dir = inp_dir
        self.err_dir = epath
        self.out_dir = out_dir
        self.stage_tym_dict = stage_tym_dict
        self.wdbpath = '/root/tas_cloud/WorkSpaceBuilder_DB/pysrc'
        #self.wdbpath_old = '/root/db_demo/WorkSpaceBuilder_DB/pysrc'
        self.ng_norpath = '/root/tas_processing_code/NG_Normalization'
        #self.table_analysis = '/root/tas_processing_code/TA2'
        self.table_analysis = '/root/tas_processing_code/TA4'
        self.preist = '/root/tas_processing_code/PreIst/preist_standalone'
        self.preist_key_value = '/root/tas_processing_code/PreIst/preist_standalone_key_value'
        #self.num_grid = '/root/tas_processing_code/Number_Grid_v1/'
        self.num_grid = '/root/tas_processing_code/Number_Grid/'
        #self.pdfparser = '/root/tas_processing_code/DataStudio_Parser/Executables/'
        #/root/tas_processing_code/pdfParser/DataStudio_Parser-v1.01/Executables
        self.pdfparser = '/root/tas_processing_code/pdfParser/DataStudio_Parser-v1.01/Executables/'
        self.ip = socket.gethostbyname(socket.gethostname())
        self.pwd = 'tas123'
        self.nport = '5007'

    def getDBconn(self, dbstring):

        args = dbstring.split('#')
        conn = mydb.connect(args[0], args[1], args[2], args[3])
        curr = conn.cursor()
        return conn, curr

    def __get_total_pages(self, ipath, docid):
        total_pages = -1
        fname = os.path.join(ipath, docid, 'pdf_total_pages')
        if os.path.isfile(fname):
            fp = open(fname, 'r')
            lines = fp.readlines()
            fp.close()
            lines = [line.strip() for line in lines if line.strip()]
            total_pages = int(lines[0])
        return total_pages

    def readInputDirectory(self, uploaded_files):

        dir_data = []
        for each in os.listdir(self.inp_dir):
            if str(each) not in uploaded_files: continue
            dir_data.append(str(each))
        return dir_data 
        
    def move_to_output_dir(self, all_pdfs):
    
        opath = '/var/www/html/PDF_Processing/PDF_Done/'
        for pdf_name in all_pdfs:
            n1 = os.path.join(self.inp_dir, pdf_name)
            n2 = os.path.join(opath, pdf_name)
            if os.path.exists(n2):
                os.remove(n2)
            shutil.move(n1, opath)
        return

    def update_batch_mgmt(self, docid, dbstring, status, process_time=0):

        con, cur = self.getDBconn(dbstring)
        ttt = sum([v[1] for k, v in self.stage_tym_dict[docid].items() if k!='TTT'])
        flg = 'G'
        if 'F' in status: flg='R'
        self.stage_tym_dict[docid]['TTT'] = [flg, ttt]
        sql = "update batch_mgmt_upload set status='%s', process_time_data='%s' where doc_id=%s"
        my_str = json.dumps(self.stage_tym_dict[docid])
        my_str = urllib.quote(my_str)
        cur.execute(sql%(status, my_str, docid))
        con.commit()
        cur.close()
        con.close()
        return

    def pdf_processing_service(self, doc_id, ipath, opath, dbstring, stage1, stage2):
        print 'IN pdf_processing_service================= SUNIL'
        t1 = time.time()
        process = 'sh Main_Wrapper.sh %s %s %s'
        cwd = os.getcwd()
        os.chdir(self.pdfparser)
        pret1 = multi_process.process_pdfs().run_single_process(process%(ipath, opath, doc_id))
        os.chdir(cwd)
        t2 = time.time()
        pret = pret1[0]
        print 'pret: ', pret 
        if pret:
            pret = 1
            tb = ''
            self.stage_tym_dict[doc_id][stage1] = [pret, (t2-t1), {'pdf_parser':[pret, tb]}]
            self.update_batch_mgmt(doc_id, dbstring, '%sF' %stage1, (t2-t1))
            print 'status', doc_id, pret, '%sF' %stage1
        else:
            pret = 0
            #untar_flg = self.untar_folder(doc_id, opath)
            self.stage_tym_dict[doc_id][stage1] = [pret, (t2-t1), {'pdf_parser':[pret, '']}]
            self.update_batch_mgmt(doc_id, dbstring, '%sN' %stage2, (t2-t1))
            print 'status', doc_id, pret, '%sN' %stage2
        return pret

    def run_pdf_parser_as_serivce(self, docid, nipath, nopath, dbstring, stage1, stage2):
        print "pdf parser service...................... SUNIL" 
        t1 = time.time()
        #url = "http://172.16.20.152:9071/submit_test?doc_id=%s&ipath=%s&opath=%s&c_flg=1" %(docid, nipath, nopath)
        url = "http://172.16.20.71:9071/submit_test?doc_id=%s&ipath=%s&opath=%s&c_flg=1" %(docid, nipath, nopath)
        content = urllib.urlopen(url).read()
        content = content.strip()
        pret = -1
        if content.strip() in ['F', 'N']: 
            t2 = time.time()
            pret = 1
            tb = content.replace('\n', ' ')
            self.stage_tym_dict[docid][stage1] = [pret, (t2-t1), {'pdf_parser':[pret, tb]}]
            self.update_batch_mgmt(docid, dbstring, '%sF' %stage1, (t2-t1))
            print 'status', docid, pret, '%sF' %stage1
        else:
            untar_flg = self.untar_folder(docid, nopath)
            pret = 0
            t2 = time.time()
            self.stage_tym_dict[docid][stage1] = [pret, (t2-t1), {'pdf_parser':[pret, '']}]
            self.update_batch_mgmt(docid, dbstring, '%sN' %stage2, (t2-t1))
            print 'status', docid, pret, '%sN' %stage2
        return pret
 
    def untar_folder(self, docid, nopath):
        cwd = os.getcwd()
        os.chdir(nopath)
        print "nopath: ", nopath
        print "tar -xf %s.tar.gz" %docid
        pret, tb = multi_process.process_pdfs().run_single_process("tar -xf %s.tar.gz" %docid)
        os.chdir(cwd)
        return pret

    def delete_doc_except_sieve_input(self, nopath, docid):
        opath = os.path.join(nopath, str(docid))
        if os.path.exists(opath):
            flst = os.listdir(opath)
            for each in flst:
                if not each.strip() == "sieve_input":
                    fpath =  os.path.join(opath, each)
                    if os.path.isfile(fpath):
                        os.remove(fpath)
                    else:
                        shutil.rmtree(fpath)

    def run_NumGrid_pagewise(self, project_id, sid, docid, dbstring, s1, s2):

        t1 = time.time()
        nnlst = []
        ng_process = "python winmain_NG_page.py -o %s -d %s -t %s"
        tf_process = "python table_formation_using_lines_generic.py %s %s %s"
        cwd = os.getcwd()
        print 'NumGrid..........'
        self.update_batch_mgmt(docid, dbstring, '2P')
        total_pages = self.__get_total_pages(self.out_dir, docid)        
        cid_path = os.path.join(self.out_dir, docid, 'CID')

        job_ar = []
        for pno in range(1, total_pages+1):
            job_ar.append(ng_process%(cid_path, docid, pno)) 

        os.chdir(self.num_grid)
        ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 240)
        ngret = 0
        if any([e[0] for k, e in ret_dict.items()]): ngret = 1
        nnlst.append(ngret)

        job_ar = []
        for pno in range(1, total_pages+1):
            job_ar.append(tf_process%(self.out_dir, docid, pno)) 
        print 'table_formation_using_lines_generic.py..........'
        os.chdir(self.table_analysis)
        ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 240)

        os.chdir(cwd)
        print s1, s2 
        print 'NG_Normalization..........'
        os.chdir(self.ng_norpath)
        proj_key = '%s_%s' %(project_id, sid)
        new_ng_path = os.path.join(self.out_dir, docid, 'New_NG','')
        os.system("rm -rf "+new_ng_path+'/*')
        for pg in range(1, total_pages+1): 
            #ng_normalization_process = "python NumGrid_Normalization.py %s %s %s %s" %( proj_key, docid, pg, self.out_dir)
            ng_normalization_process = "python NumGrid_Normalization.py %s %s %s" %(docid, pg, self.out_dir)
            nret, tb2 = multi_process.process_pdfs().run_single_process(ng_normalization_process)
            print "'NG_Normalization", pg, "=====", nret
            ret_dict[ng_normalization_process] = [nret, tb2]
            nnlst.append(nret)
        os.chdir(cwd)
        fngret = 0
        if any(nnlst): fngret = 1
        print 'status', fngret 
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [fngret, (t2-t1), ret_dict, (t2-t1)]
        self.print_error_process(ret_dict)
        return self.update_stage(docid, dbstring, fngret, s1, s2, (t2-t1))


    def run_NumGrid(self, project_id, sid, docid, dbstring, s1, s2):

        t1 = time.time()
        nnlst = []
        tmp_dict = {}
        ng_process = "python winmain_NG_v1.py -o %s -d %s -t %s"
        cwd = os.getcwd()
        print 'NumGrid..........'
        self.update_batch_mgmt(docid, dbstring, '2P')
        total_pages = self.__get_total_pages(self.out_dir, docid)        
        cid_path = os.path.join(self.out_dir, docid, 'CID')
        os.chdir(self.num_grid)
        print ng_process%(cid_path, docid, total_pages)  
        ngret, tb1 = multi_process.process_pdfs().run_single_process(ng_process%(cid_path, docid, total_pages))
        nnlst.append(ngret)
        tmp_dict[ng_process%(cid_path, docid, total_pages)] = [ngret, tb1]
        print s1, s2 
        print 'NG_Normalization..........'
        os.chdir(self.ng_norpath)
        proj_key = '%s_%s' %(project_id, sid)
        new_ng_path = os.path.join(self.out_dir, docid, 'New_NG','')
        os.system("rm -rf "+new_ng_path+'/*')
        for pg in range(1, total_pages+1): 
            #ng_normalization_process = "python NumGrid_Normalization.py %s %s %s %s" %( proj_key, docid, pg, self.out_dir)
            ng_normalization_process = "python NumGrid_Normalization.py %s %s %s" %(docid, pg, self.out_dir)
            nret, tb2 = multi_process.process_pdfs().run_single_process(ng_normalization_process)
            print "'NG_Normalization", pg, "=====", nret
            tmp_dict[ng_normalization_process] = [nret, tb2]
            nnlst.append(nret)
        fngret = 0
        if any(nnlst): fngret = 1
        print 'status', nnlst 
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [fngret, (t2-t1), tmp_dict, (t2-t1)]
        return self.update_stage(docid, dbstring, fngret, s1, s2, (t2-t1))

    def run_text_applicator(self, docid, dbstring, pid, sid, project_type, s1, s2):

        t1 = time.time()
        cwd = os.getcwd()
        print 'Text Applicator.........'
        self.update_batch_mgmt(docid, dbstring, '%sP'%s1)
        text_app_process = 'time python2.7 toapplytextMain.py -p %s -r %s -t %s -d %s'%(pid, sid, project_type, docid)
        db_ins = 'python populate_db_from_app_data_v2.py %s %s %s %s'%(docid, self.out_dir, dbstring, project_type)
        os.chdir(self.wdbpath)
        mret, mtb = multi_process.process_pdfs().run_single_process(text_app_process)
        dret, dtb = multi_process.process_pdfs().run_single_process(db_ins)
        os.chdir(cwd)
        print 'status -->>', mret, dret
        t2 = time.time()
        ret = 0
        if mret or dret: ret = 1
        self.stage_tym_dict[docid][s1] = [ret, (t2-t1), {text_app_process:[ret, mtb+dtb]}]
        self.update_stage(docid, dbstring, ret, s1, s2, (t2-t1))
        return ret

    def run_preist_bond_pagewise(self, docid, dbstring, s1, s2):

        t1 = time.time()
        #preist_process = 'python new_cmd_run_v1.py %s %s %s'
        preist_process = 'python new_cmd_run_page_cid.py %s %s %s'
        cwd = os.getcwd()
        print 'PREIST KEY VALUE.........'
        self.update_batch_mgmt(docid, dbstring, '%sP'%s1)
        total_pages = self.__get_total_pages(self.out_dir, docid)
        job_ar = []
        for pno in range(1, total_pages+1):
            job_ar.append(preist_process%(self.out_dir, docid, pno))
        os.chdir(self.preist_key_value)
        ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 120)
        os.chdir(cwd)
        iret = 0
        if any([e[0] for k, e in ret_dict.items()]):
           iret = 1
        print 'status -->>', iret
        t2 = time.time()
        self.print_error_process(ret_dict)
        self.stage_tym_dict[docid][s1] = [iret, (t2-t1), ret_dict]
        self.update_stage(docid, dbstring, iret, s1, s2, (t2-t1))
        return iret

    def run_preist_pagewise(self, docid, dbstring, s1, s2):

        t1 = time.time()
        preist_process = 'python new_cmd_run_page_cid.py %s %s %s'
        cwd = os.getcwd()
        print 'PREIST.........'
        self.update_batch_mgmt(docid, dbstring, '3P')
        total_pages = self.__get_total_pages(self.out_dir, docid)
        job_ar = []
        for pno in range(1, total_pages+1):
            job_ar.append(preist_process%(self.out_dir, docid, pno))
        os.chdir(self.preist)
        ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 120)
        os.chdir(cwd)
        iret = 0
        if any([e[0] for k, e in ret_dict.items()]):
           iret = 1
        print 'status -->>', iret
        t2 = time.time()
        self.print_error_process(ret_dict)
        self.stage_tym_dict[docid][s1] = [iret, (t2-t1), ret_dict]
        self.update_stage(docid, dbstring, iret, s1, s2, (t2-t1))
        return iret


    def run_preist(self, docid, dbstring, s1, s2):

        t1 = time.time()
        preist_process = 'python new_cmd_run.py %s %s'
        cwd = os.getcwd()
        print 'PREIST.........'
        self.update_batch_mgmt(docid, dbstring, '3P')
        os.chdir(self.preist)
        iret, tb1 = multi_process.process_pdfs().run_single_process(preist_process%(self.out_dir, docid))
        print 'status -->>', iret
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [iret, (t2-t1), {preist_process%(self.out_dir, docid): [iret, tb1]}]
        self.update_stage(docid, dbstring, iret, s1, s2, (t2-t1))
        return iret

    def print_error_process(self, ret_dict):

        for key, each in ret_dict.items():
            if each[0] == 0: continue
            print 'ERROR!!!!----%s\t%s\t%s'%(key, each[0], each[1])
        return

    def run_MRT_Multi(self, docid, dbstring, project_id, sid, s1, s2):

        t1 = time.time()
        total_pages = self.__get_total_pages(self.out_dir, docid)
        mrt_process = 'python pdfmrt_v2_page.py -d %s -p %s -w %s -n %s > a10'
        cwd = os.getcwd()
        print 'MRT..........'
        self.update_batch_mgmt(docid, dbstring, '4P')
        os.chdir(self.wdbpath)
        job_ar = []
        for pno in range(1, total_pages+1): 
            job_ar.append(mrt_process %(docid, project_id, sid, pno))
        mret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 1800)
        self.print_error_process(mret_dict)
        mret = 0 
        if any([e[0] for k, e in mret_dict.items()]):
           mret = 1  
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [mret, (t2-t1), mret_dict]
        return self.update_stage(docid, dbstring, mret, s1, s2, (t2-t1))


    def run_inc_ranking_unseen(self, docid, dbname, project_id, sid, s1, s2):
        t1 = time.time()
        #mrt_process = 'python inc_ranking_unseen.py -d %s -p %s -w %s  > a20'
        mrt_process = 'python tracer_wrapper.py -d %s -p %s -w %s  > a20'
        cwd = os.getcwd()
        print 'inc_ranking_unseen..........'
        os.chdir(self.wdbpath)
        mret, tb = multi_process.process_pdfs().run_single_process(mrt_process%(docid, project_id, sid))
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [mret, (t2-t1), {mrt_process%(docid, project_id, sid):[mret, tb]}]
        self.update_stage(docid, dbname, mret, s1, s2, (t2-t1))
        return mret

    def run_MRT(self, docid, dbstring, project_id, sid, s1, s2):
        mrt_process = 'python pdfmrt_v2.py -d %s -p %s -w %s > a10'
        cwd = os.getcwd()
        print 'MRT..........'
        self.update_batch_mgmt(docid, dbstring, '4P')
        os.chdir(self.wdbpath)
        mret, tb = multi_process.process_pdfs().run_single_process(mrt_process%(docid, project_id, sid))
        os.chdir(cwd)
        return self.update_stage(docid, dbstring, mret, s1, s2, (t2-t1))

    def run_document_zone_dtt_v2(self, project_id, docid):
        mrt_process = 'python document_zone_dtt_v2.py %s %s'
        cwd = os.getcwd()
        print 'document_zone_dtt_v2..........', self.wdbpath
        cmd = mrt_process%(project_id, docid)
        #print cmd
        os.chdir(self.wdbpath)
        os.system(cmd)
        #mret, tb = multi_process.process_pdfs().run_single_process(mrt_process%(project_id, docid))
        #os.chdir(cwd)

    def update_stage(self, docid, dbstring, mret, s1, s2, process_time=0):
        if mret:
            print 'status', docid, mret, '%sF' %s1
            self.update_batch_mgmt(docid, dbstring, '%sF' %s1, process_time)
        else:
            print 'status', docid, mret, '%sN' %s2
            self.update_batch_mgmt(docid, dbstring, '%sN' %s2, process_time)
        return mret

    def create_db_folder(self, docid):

        process = 'python cmd_run_exe.py %s %s'
        cwd = os.getcwd()
        print 'DB EXE..........'
        os.chdir(self.preist)
        dbret, tb = multi_process.process_pdfs().run_single_process(process%(self.out_dir, docid))
        os.chdir(cwd)
        return dbret

    def get_status(self, docid, dbname):

        con, cur = self.getDBconn(dbname)
        rsql = "select status from batch_mgmt_upload where doc_id=%s" 
        cur.execute(rsql%docid)
        sts = cur.fetchone()
        cur.close()
        con.close()
        return sts[0]
 
    def run_triplet_creation(self, project_id, sid, doc_str, dbname, project_type):

        t1 = time.time()
        cwd = os.getcwd()
        cmd_table_analysis = "python TA_main_v2.py %s %s %s %s %s"
        os.chdir(self.table_analysis)
        print "TABLE ANALYSIS::::", cmd_table_analysis%(project_id, sid, doc_str, dbname, project_type)
        tab_ret, tb = multi_process.process_pdfs().run_single_process(cmd_table_analysis%(project_id, sid, doc_str, dbname, project_type))
        os.chdir(cwd)
        print "tab_ret::", tab_ret
        t2 = time.time()
        self.stage_tym_dict[doc_str]['7'] = [tab_ret, (t2-t1), {cmd_table_analysis%(project_id, sid, doc_str, dbname, project_type):[tab_ret, tb]}]
        self.update_stage(doc_str, dbname, tab_ret, '7', '8', (t2-t1))
        if tab_ret: return 1
        return 0

    def run_gcomp(self, project_type, doc_str, dbname, opath):
        cwd = os.getcwd()
        cmd = "python gcomp_wrapper.py %s %s %s %s %s" 
        print cmd %(opath, opath, doc_str, project_type, dbname)
        os.chdir("/root/GCOMP/StandAloneGCOMP/")
        sret, tb = multi_process.process_pdfs().run_single_process(cmd %(opath, opath, doc_str, project_type, dbname))
        os.chdir(cwd)
        if sret: return 1
        return 0


    def run_redis_search(self, fye, cname, project_type, doc_str, project_id, sid, dbname):
        cwd = os.getcwd()
        cmd = "python document_data_redisearch.py /var/www/html/WorkSpaceBuilder_DB/%s/%s/pdata/docs/ %s"  
        print cmd %(project_id, sid, doc_str)
        os.chdir("/var/www/cgi-bin/NextGenTextApp/tasproject_1/src_dev/")
        sret, tb = multi_process.process_pdfs().run_single_process(cmd %(project_id, sid, doc_str))
        os.chdir(cwd)
        #self.update_stage(doc_str, dbname, sret, '9', '10')
        if sret: return 1
        return 0


    def run_worspace_feeder(self, fye, cname, project_type, doc_str, project_id, sid, dbname):

        t1 = time.time()
        cwd = os.getcwd()
        cmd = 'python create_selection_result_live_sp.py %s "%s" %s %s %s %s %s'
        print "Selection Result::::", cmd%(fye, cname, project_type, doc_str, project_id, sid, dbname)
        os.chdir(self.table_analysis)
        #sret, tb = multi_process.process_pdfs().run_single_process(cmd%(fye, cname, project_type, doc_str, project_id, sid, dbname))
        sret, tb = 0, ''
        sret = os.system(cmd%(fye, cname, project_type, doc_str, project_id, sid, dbname))
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[doc_str]['9'] = [sret, (t2-t1), {cmd%(fye, cname, project_type, doc_str, project_id, sid, dbname):[sret, tb]}]
        self.update_stage(doc_str, dbname, sret, '9', '10', (t2-t1))
        if sret: return 1
        return 0


    def create_selection_data(self, doc_str, meta_data, dbname, project_type, project_id, sid, opath, stg=[7,8,9], reprocess=0):

        if not meta_data: return 1
        if doc_str and reprocess==0:
           sts = self.get_status(doc_str, dbname)
           if sts and sts[-1] != 'N':
              return 1
        meta_data = ast.literal_eval(meta_data)
        cname = meta_data.get("Company", "")
        fye = meta_data.get("FYE", "") 
        ####TABLE ANALYSIS####
        if 7 in stg:
            tri_ret = self.run_triplet_creation_new(project_id, sid, doc_str, dbname, project_type, '7', '8')
            #tri_ret = self.run_triplet_creation_new(project_id, sid, doc_str, dbname, project_type, '9', '10')
            #tri_ret = self.run_triplet_creation(project_id, sid, doc_str, dbname, project_type)
            if tri_ret: return 1
        ####TRIPLET2####
        if 8 in stg:
            inc_ret = self.run_inc_ranking_unseen(doc_str, dbname, project_id, sid, '8', '9')
            #tri_ret = self.run_triplets2_Multi(doc_str, project_id, sid, dbname, opath, project_type)
            if inc_ret: return 1
            #print "tri_ret::", tri_ret
        ####SELECTION DATA####
        if 9 in stg:
            tri_ret = self.run_worspace_feeder(fye, cname, project_type, doc_str, project_id, sid, dbname)
            if tri_ret: return 1
            self.run_redis_search(fye, cname, project_type, doc_str, project_id, sid, dbname)
            #self.run_gcomp(project_type, doc_str, dbname, opath)
        return 0 

    def run_triplets2_Multi(self, doc_str, project_id, sid, dbname, opath, project_type):
       
        t1 = time.time() 
        print '...........triplate_classify2_wrapper_page..............'
        cwd = os.getcwd()
        total_pages = self.__get_total_pages(opath, doc_str)
        if project_type=='html': total_pages = 1  
        print "total_pages: ", total_pages 
        os.chdir(self.wdbpath)
        job_ar = []
        for pno in range(1, total_pages+1): 
            cmd_triplet = "python triplate_classify2_wrapper_page.py -d %s -n %s -p %s -w %s" %(doc_str, pno, project_id, sid)
            job_ar.append(cmd_triplet)
        tri_ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 1800)
        self.print_error_process(tri_ret_dict)
        os.chdir(cwd)
        mret = 0 
        if any([e[0] for k, e in tri_ret_dict.items()]):
           mret = 1  
        t2 = time.time()
        self.stage_tym_dict[doc_str]['8'] = [mret, (t2-t1), tri_ret_dict]
        self.update_stage(doc_str, dbname, mret, '8', '9', (t2-t1))
        if mret: return 1
        return 0 

    def run_triplet_creation_new(self, project_id, sid, doc_str, dbname, project_type, s1, s2):

        t1 = time.time()
        cwd = os.getcwd()
        #cmd = 'python TA_main_v3_page_TO.py "%s"'
        total_pages = self.__get_total_pages(self.out_dir, doc_str)
        if project_type=='html': total_pages = 1  
        print "total_pages: ", total_pages 
        job_ar = []
        os.chdir(self.table_analysis)
        for pno in range(1, total_pages+1):
            #args = {'tab_grid_id':'','db_string':dbname,'pid':project_id,'doctype':project_type,'trip2Path':self.wdbpath,'table_classification_flg':'1','pno':pno,'sid':sid,'doc_id':doc_str}
            #job_ar.append(cmd %(args))
            job_ar.append(['python', 'TA_main_v3_page_TO.py', '{"doc_id": "%s", "pid": "%s", "sid": "%s", "db_string": "%s", "doctype": "%s", "pno": "%s", "trip2Path": "%s", "tab_grid_id": "", "table_classification_flg": "0"}' %(doc_str, project_id, sid, dbname, project_type, pno, self.wdbpath)])
            #os.system(cmd %(args))
        tri_ret_dict = multi_process2.process_pdfs().process_basic(job_ar, 8, 1800)
        os.chdir(cwd)
        self.print_error_process(tri_ret_dict)
        mret = 0 
        if any([e[0] for k, e in tri_ret_dict.items()]):
           mret = 1  
        t2 = time.time()
        self.stage_tym_dict[doc_str][s1] = [mret, (t2-t1), tri_ret_dict]
        self.update_stage(doc_str, dbname, mret, s1, s2, (t2-t1))
        return mret

    def split_merge_pdf_file(self, outp, n2, docid):
        outpath = '%s/%s' %(outp, docid)
        if not os.path.exists(outpath):
            cmd = 'mkdir -p %s' %(outpath)
            os.system(cmd)
        outdir = '%s/%s' %(outpath, docid) + '_%d.pdf'
        cmd = "pdfseparate %s %s " % (n2, outdir)
        os.system(cmd)
        cmd = "pdfunite %s/* %s " % (outpath, n2)
        os.system(cmd)

    def run_pdf_cloud_service(self, docid, project_id, sid, dbstring, project_type, stage1, stage2, lang, ocr, pdftype, selected_pages, stagelst, ocr_chk, pd, lc):
        t1 = time.time()
        print "RUN run_pdf_cloud_service =========================SUNIL"
        dname = dbstring.split('#')[-1]
        import insert_query_new_cloud_lc as insert_query_new
        obj = insert_query_new.DocProcess(pd)

        ipath = os.path.join(self.out_dir, docid, 'sieve_input')
        n2 = os.path.join(ipath, docid+'.pdf')
        totalpages = obj.getPageCount(n2)
        if totalpages == -1:
            self.split_merge_pdf_file('/tmp', n2, docid)
            totalpages = obj.getPageCount(n2)
        n21 = n2.replace('/var/www/html/', '')
        iurl = 'http://%s/%s' %(self.ip, n21)
        print iurl
        stagelst_new = []
        stagedict_cloud = {}
        for each in stagelst:
            if each ==1:continue
            stagelst_new.append(str(each))
            stagedict_cloud[int(each)-1]= 1 
        stage_lst = '~'.join(stagelst_new)
        print stage_lst

        curl = 'http://%s:%s/get_method?oper_flag=97008&project_id=%s&ws_id=%s&db_name=%s&doc_id=%s&doc_type=%s&stage_lst=%s&lc=%s' %(self.ip, self.nport, project_id, sid, dname, docid, project_type, stage_lst, lc)
        print curl
        if lc == '0':
            sstr = '1#2'
        else:
            sstr = '1#2#3'
        mode = "P"
        if pd == "0":
            mode = "T"
        pdftype = int(pdftype)
        dbstring_new = '%s#'%(dbstring)
        kdictstr = json.dumps({"database":dbstring_new, "stages":stagedict_cloud})
        #records = [[project_id, sid, docid, self.ip, self.pwd, iurl, self.out_dir, totalpages, 99999, curl, ocr, pdftype, lang, selected_pages, ocr_chk, sstr, dbstring_new]]
        records = [[project_id, sid, docid, self.ip, self.pwd, iurl, self.out_dir, totalpages, 99999, curl, ocr, pdftype, lang, selected_pages, ocr_chk, sstr, kdictstr, mode]]
        print records
        obj.run(records)
        t2 = time.time()
        pret = 0
        self.stage_tym_dict[docid][stage1] = [pret, (t2-t1), {'pdf_parser':[pret, '']}]
        self.update_batch_mgmt(docid, dbstring, '%sN' %stage2, (t2-t1))
        print 'status', docid,  '%sN' %stage2

    def process_documents(self, doc_ar, project_id, sid, dbstring, meta_data, project_type, lang, ocr, pdftype, selected_pages, stage_lst, ocr_chk, pd, lc):
        for (docid, pdf_name) in doc_ar[:]:
            ipath = os.path.join(self.out_dir, docid, 'sieve_input')
            if not os.path.exists(ipath): os.makedirs(ipath)
            self.stage_tym_dict[docid] = {}
            n1 = os.path.join(self.inp_dir, pdf_name)
            n2 = os.path.join(ipath, docid+'.pdf')
            shutil.copyfile(n1, n2)
            ####PDF PARSER####
            #if pret: continue
            #new pdf parser 
            self.delete_doc_except_sieve_input(self.out_dir, docid)
            #replace cherry with cloud pdf service 
            self.run_pdf_cloud_service(docid, project_id, sid, dbstring, project_type, '1', '2', lang, ocr, pdftype, selected_pages, stage_lst, ocr_chk, pd, lc)
            '''
            ###pret = self.run_pdf_parser_as_serivce(docid, ipath, self.out_dir, dbstring, '1', '2')
            pret = self.pdf_processing_service(docid, ipath, self.out_dir, dbstring, '1', '2')
            if pret: continue
            ####CREATE DB FOLDER####
            #dbret = self.create_db_folder(docid)
            #if dbret: continue
            ####NUM GRID####
            ngret = self.run_NumGrid(project_id, sid, docid, dbstring, '2', '3')
            if ngret: continue
            ####MRT APPLR#### 
            mret = self.run_MRT_Multi(docid, dbstring, project_id, sid, '3', '4')
            if mret: continue
            #####ELIZA TABLE NORM
            ngaret = self.run_ng_grp_applicator(project_id, sid, docid, dbstring, '4', '5')
            if ngaret: continue
            ####PREIST####
            #iret = self.run_preist(docid, dbstring, '5', '6')
            iret = self.run_preist_pagewise(docid, dbstring, '5', '6')
            if iret: continue
            ####ASHWATH TABLE DICT####
            ret_value = self.runhtmlMain_pdf_Multi(docid, dbstring, project_id, sid, '6', '7')
            if ret_value: continue
            csd_ret = self.create_selection_data(docid, meta_data, dbstring, project_type, project_id, sid, self.out_dir)
            '''
        return

    def run_ng_grp_applicator(self, project_id, sid, docid, dbstring, s1, s2):
        
        t1 = time.time()
        cwd = os.getcwd()
        print '...........ng_grp_applicator..........'
        os.chdir(self.ng_norpath)
        proj_key = '%s_%s' %(project_id, sid)
        #python Wrap_ng_grp_applicator_release_fm_N.py 2640 /var/www/html/WorkSpaceBuilder_DB/8/1/pdata/docs/ 1
        #ng_grp_process = "python Wrap_ng_grp_applicator_release_fm_N.py %s %s %s 1 " %(proj_key, docid, self.out_dir)
        #ng_grp_process = "python Wrap_ng_grp_applicator_release_fm_N.py %s %s %s 1 1" %(proj_key, docid, self.out_dir)
        ng_grp_process = "python Wrap_ng_grp_applicator_release_fm_N.py %s %s 1 0" %(docid, self.out_dir)
        print ng_grp_process
        nret, tb = multi_process.process_pdfs().run_single_process(ng_grp_process)
        print "ng_grp_applicator",  "=====", nret
        os.chdir(cwd)
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [nret, (t2-t1), {ng_grp_process:[nret, tb]}]
        return self.update_stage(docid, dbstring, nret, s1, s2, (t2-t1))

    def runhtmlMain_pdf_Multi(self, docid, dbstring, project_id, sid, s1, s2):

        print '..........runhtmlMain.............'
        t1 = time.time()
        total_pages = self.__get_total_pages(self.out_dir, docid)
        cwd = os.getcwd()
        ret_value = 0
        os.chdir(self.wdbpath)
        #os.chdir(self.wdbpath_old)
        job_ar = []
        for pno in range(1, total_pages+1): 
            job_ar.append('time python2.7 runhtmlMain.py -p %s -r %s -d %s -n %s -t pdf' %(project_id, sid, docid, pno))
        hmrt_ret_dict = multi_process.process_pdfs().process_basic(job_ar, 8, 1800)
        #hmrt_ret_dict = {}  
        os.chdir(cwd)
        mret = 0 
        self.print_error_process(hmrt_ret_dict)
        if any([e[0] for k, e in hmrt_ret_dict.items()]):
           mret = 1  
        t2 = time.time()
        self.stage_tym_dict[docid][s1] = [mret, (t2-t1), hmrt_ret_dict]
        ret_value = self.update_stage(docid, dbstring, mret, s1, s2, (t2-t1))
        return ret_value



    def runhtmlMain_pdf(self, docid, dbstring, project_id, sid, s1, s2):
        total_pages = self.__get_total_pages(self.out_dir, docid)
        cwd = os.getcwd()
        ret_value = 0
        for pno in range(1, total_pages+1): 
            os.chdir(self.wdbpath)
            print 'time python2.7 runhtmlMain.py -p %s -r %s -d %s -n %s -t pdf' %(project_id, sid, docid, pno)
            hmrt_ret, tb = multi_process.process_pdfs().run_single_process('time python2.7 runhtmlMain.py -p %s -r %s -d %s -n %s -t pdf' %(project_id, sid, docid, pno))
            os.chdir(cwd)
            ret_value = self.update_stage(docid, dbstring, hmrt_ret, s1, s2, (t2-t1))
            if ret_value: break  
        return ret_value

    def get_meta_data_from_text_file(self, meta_file_name):

        f = open(meta_file_name, 'r')
        lines = f.readlines()
        f.close()
        header = lines[0].strip('\n').split('\t')
        data = lines[1].strip('\n').split('\t')
        meta_dict = {key : data[ind].strip('\r').strip('\n') for ind, key in enumerate(header)}
        meta_dict = json.dumps(meta_dict)
        return meta_dict
            
    def insert_into_batch_mgmt(self, doc_ar, con, cur, project_name, status, doc_type, meta_data, user_id):
        #this is for new code 
        sql = "insert into batch_mgmt_upload values (%s, '%s', '%s', '%s', '', '%s', '%s', '%s', '%s', 'N', '%s', '' , '');"
        #this is for old code 
        #sql = 'insert into batch_mgmt_upload values (%s, "%s", "%s", "System", "", "%s", "%s", "%s", "%s", "N", "%s", "", "");'
        tmpdoc_ar   = []
        for (docid, pdf_name) in doc_ar:
            txtfilename = '%s.txt' %(''.join(pdf_name.split('.')[:-1]))
            inp_txt_path = os.path.join(self.err_dir, txtfilename)
            if os.path.exists(inp_txt_path):
                meta_data = self.get_meta_data_from_text_file(inp_txt_path)
            dt = str(datetime.datetime.now())
            try:
                print sql%(docid, status, project_name, user_id, project_name, pdf_name, doc_type, dt, meta_data)
                cur.execute(sql%(docid, status, project_name, user_id, project_name, pdf_name, doc_type, dt, meta_data)) 
                tmpdoc_ar.append((docid, status, project_name, project_name, pdf_name, doc_type, dt, meta_data))
            except Exception as ex:
                print 'EXCEPTION batch_mgmt_upload :::::', docid, pdf_name, ex
                pass
        con.commit()
        return tmpdoc_ar
  
    def get_doc_list(self, cur1, all_docs):

        sql = "select max(CAST(doc_id AS UNSIGNED)) from batch_mgmt_upload"
        cur1.execute(sql)
        max_doc = cur1.fetchone()
        if (max_doc) and (max_doc[0]!=None): max_doc = max_doc[0]
        else: max_doc = 0
        doc_ar = [(str(max_doc+ind+1), each) for ind, each in enumerate(all_docs)]
        return doc_ar

    def render(self,json_data, server_ip, render_add, rener_port, doc_id, urlname, render_path):
        
        args = {"url": urlname, "doc_id": str(doc_id), "opath":render_path, "ip":server_ip, "json_data":json.dumps(json_data), "desc_ip":server_ip, "desc_folder":render_path, "desc_passwrd":"<*34$%+)>"}
        base_url = "http://%s:%s/submit_test?{}"%(render_add, rener_port)
        service_str = base_url.format(urllib.urlencode(args))
        f  = urllib.urlopen(service_str)
        res = f.read()
        return res

    def run_render(self, project_id, sid,  docid, urlname, dbstring, render_path):
        json_data = {}
        server_ip = self.ip
        render_add = self.ip
        rener_port = '7707'
        t1 = time.time()
        ret = self.render(json_data, server_ip, render_add, rener_port, docid, urlname, render_path)
        t2 = time.time()
        self.stage_tym_dict[docid]['1'] = [ret, (t2-t1), {'url_render':[ret, '']}]
        print 'RES ::', ret
        if ret != '0':
            self.update_batch_mgmt(docid, dbstring, '1F', (t2-t1))
        else:
            self.runClonehtml(dbstring, project_id, sid, docid, '2')
            self.update_batch_mgmt(docid, dbstring, '2N', (t2-t1))
        return ret

    def run_html_applicator(self, docid, project_id, sid, dbstring):

        cwd = os.getcwd()
        t1 = time.time()
        self.update_batch_mgmt(docid, dbstring, '2P')
        print 'Start App.....', docid, '2P'
        os.chdir(self.wdbpath)
        hcmd = 'time python2.7 runhtmlMain.py -p %s -r %s -d %s -n 1' %(project_id, sid, docid)
        hmrt_ret, hmrt_tb = multi_process.process_pdfs().run_single_process(hcmd)
        os.chdir(cwd)
        print 'run status::', hmrt_ret
        t2 = time.time()
        if hmrt_ret: 
            self.stage_tym_dict[docid]['2'] = [hmrt_ret, (t2-t1), {hcmd:[hmrt_ret, hmrt_tb]}]
            self.update_batch_mgmt(docid, dbstring, '2F', (t2-t1))
            print 'End App.....', docid, '2F'
        else:
            self.stage_tym_dict[docid]['2'] = [hmrt_ret, (t2-t1), {hcmd:[hmrt_ret, '']}]
            self.update_batch_mgmt(docid, dbstring, '3N', (t2-t1))
            print 'End App.....', docid, '3N'
        return hmrt_ret
 
    def process_html_documents(self, doc_ar, dbstring, project_id, sid, render_path, meta_data, project_type):

        for (docid, urlname) in doc_ar[:]:
            self.stage_tym_dict[docid] = {}
            self.update_batch_mgmt(docid, dbstring, '1P')
            print 'URL RUN ::', docid, urlname
            ret = self.run_render(project_id, sid, docid, urlname, dbstring, render_path)
            print "render", ret
            if ret != '0':continue
            h1ret = self.run_html_applicator(docid, project_id, sid, dbstring)
            csd_ret = self.create_selection_data(docid, meta_data, dbstring, project_type, project_id, sid, self.out_dir)
            ret_value = self.run_text_applicator(docid, dbstring, project_id, sid, project_type, '11', '10')
        return

    def load_url(self, url, timeout=60):
        import urllib
        import urllib2
        from urlparse import urlparse, urlunparse, parse_qsl
        txt1    = ''
        try:
            url = urllib.unquote(url);
            url_parts = list(urlparse(url))
            query = dict(parse_qsl(url_parts[4]))
            url_parts[4] = urllib.urlencode(query)
            new_url = urlunparse(url_parts)
            res = urllib2.urlopen(new_url, timeout=timeout)
            status_code = res.getcode()
            txt1 = res.read()
            txt = "OK"
        except Exception, e:
            if type(e).__name__ == 'HTTPError':
                status_code = e.getcode()
                txt = "httperr"
            elif type(e).__name__ == 'timeout':
                status_code = -1
                txt = "timeout"
            else:
                txt = "othererr"
                print dir(e)
        return txt, txt1


    def main_html(self, dbstring, batch_name, project_id, sid, all_urls, render_path, meta_data, project_type):

        if not all_urls: return all_urls, []
        con1, cur1 = self.getDBconn(dbstring)
        doc_ar = self.get_doc_list(cur1, all_urls)
        tmpdoc_ar   = self.insert_into_batch_mgmt(doc_ar, con1, cur1, batch_name, '1N', 'HTML', meta_data)
        cur1.close()
        con1.close()
        try:
            tmpmeta_data    = json.loads(meta_data)
            ntmpdoc_ar  = []
            for r in tmpdoc_ar:
                tmpmeta_data['doc_name']    = r[4]
                ntmpdoc_ar.append({'doc_id':r[0], 'meta_data':tmpmeta_data})
            tmpdoc_ar   = ntmpdoc_ar
            i_company_id = tmpmeta_data.get('i_company_id','')
            
        except:i_company_id  = ''
        #UPDATE DEMO DOCLIST
        if i_company_id:
            ijson   = {'cmd_id':3, 'project_id':project_id,'i_company_id':i_company_id, 'doclist':tmpdoc_ar, 'type':'HTML'}
            #url = 'http://172.16.20.229:7777/get_method?'+json.dumps(ijson)
            #print url
            #self.load_url(url, 60)
            print json.dumps(ijson)
            self.insert_into_demo(ijson)
        self.process_html_documents(doc_ar, dbstring, project_id, sid, render_path, meta_data, project_type)
        return all_urls, doc_ar

    def main_pdf(self, dbstring, batch_name, project_id, sid, uploaded_files, meta_data, project_type, lang, ocr, pdftype, selected_pages, stage_lst, ocr_chk, pd, lc, user_id):

        all_pdfs = self.readInputDirectory(uploaded_files)
        if not all_pdfs: return all_pdfs, []
        con1, cur1 = self.getDBconn(dbstring)
        doc_ar = self.get_doc_list(cur1, all_pdfs)
        tmpdoc_ar   = self.insert_into_batch_mgmt(doc_ar, con1, cur1, batch_name, '1N', 'PDF', meta_data)
        cur1.close()
        con1.close()
        try:
            tmpmeta_data    = json.loads(meta_data)
            ntmpdoc_ar  = []
            for r in tmpdoc_ar:
                tmpmeta_data['doc_name']    = r[4]
                ntmpdoc_ar.append({'doc_id':r[0], 'meta_data':tmpmeta_data, 'doc_name':r[4]})
            tmpdoc_ar   = ntmpdoc_ar
            i_company_id = json.loads(meta_data).get('i_company_id','')
        except:i_company_id  = ''
        #UPDATE DEMO DOCLIST
        self.process_documents(doc_ar, project_id, sid, dbstring, meta_data, project_type, lang, ocr, pdftype, selected_pages, stage_lst, ocr_chk, pd, lc)
        self.move_to_output_dir(all_pdfs)
        if i_company_id:
            ijson   = {'cmd_id':3,'project_id':project_id,'i_company_id':i_company_id, 'doclist':tmpdoc_ar, 'type':'PDF'}
            #url = 'http://172.16.20.229:7777/get_method?'+json.dumps(ijson)
            #print url
            #self.load_url(url, 60)
            print json.dumps(ijson)
            self.insert_into_demo(ijson)
        return all_pdfs, doc_ar


    def runClonehtml(self, dbstring, project_id, sid, docid, stg):

        t1 = time.time()
        cwd = os.getcwd()
        ret_value = 0
        if 1:
            #os.chdir('/root/node_socket_client')
            #print 'node --stack-size=6000 --max-old-space-size=30720 --max-semi-space-size=10240 --gc_global --expose-gc /root/node_socket_client/html_clone_pravat.js  %s %s %s %s' %(project_id, sid, docid, dbstring)
            print 'node --stack-size=6000 --max-old-space-size=30720 --max-semi-space-size=10240 --gc_global --expose-gc /var/www/html/webrendererv2/websrc/xul/apps/clone_updater/html_clone.js  %s %s %s %s' %(project_id, sid, docid, dbstring)
            #hmrt_ret = os.system('node --stack-size=6000 --max-old-space-size=30720 --max-semi-space-size=10240 --gc_global --expose-gc /root/node_socket_client/html_clone_pravat.js  %s %s %s %s' %(project_id, sid, docid, dbstring))
            hmrt_ret = os.system('node --stack-size=6000 --max-old-space-size=30720 --max-semi-space-size=10240 --gc_global --expose-gc /var/www/html/webrendererv2/websrc/xul/apps/clone_updater/html_clone.js  %s %s %s %s' %(project_id, sid, docid, dbstring))
            t2 = time.time()
            self.stage_tym_dict[docid][stg] = [hmrt_ret, (t2-t1), {'clone_html':[hmrt_ret, '']}]
            #os.chdir(cwd)
        return ret_value

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = mydb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def insert_into_demo(self, ijson):
        docid_lst        = ijson['doclist']
        project_id       = str(ijson['project_id'])
        company_row_id             = str(ijson['i_company_id'])
        doc_type         = str(ijson['type'])
        
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
            
        ''' 
        read_qry = """ SELECT project_id, company_row_id FROM company_project_mgmt WHERE project_id='%s'   """%(project_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        all_row_str = ', '.join([str(r[0]) for r in t_data])
        
        rd_qy = """ SELECT row_id FROM company_mgmt WHERE row_id in (%s) AND company_name=%s """%(all_row_str, company_name)
        m_cur.execute(rd_qy)
        company_row_id = m_cur.fetchone()
        '''
        
        chk_read = """ SELECT project_id, company_row_id, doc_id FROM document_mgmt WHERE project_id='%s' AND company_row_id='%s' """%(project_id, company_row_id)
        m_cur.execute(chk_read)
        t_info = m_cur.fetchall()       
            
        info_chk_dct = {}
        for rw in t_info:
            pid, crid, dcid = map(str, rw)
            info_chk_dct[(pid, crid, dcid)] = 1

        insert_rows = []
        update_rows = []
        
        for dc_info in docid_lst:
            dc, meta = str(dc_info['doc_id']),  str(dc_info['meta_data'])                   
            if (project_id, company_row_id, dc) in info_chk_dct:
                update_rows.append((meta, project_id, company_row_id, dc))                
            elif (project_id, company_row_id, dc) not in info_chk_dct:
                if doc_type == 'PDF':
                    ipath = os.path.join(self.out_dir, dc, 'sieve_input')
                    n2 = os.path.join(ipath, dc+'.pdf')
                    cmd = 'qpdf  --show-npages %s'%(n2)
                    process = subprocess.Popen(cmd , stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)    
                    op = process.communicate()
                    print '>>>', op
                    nop = op[0].strip()
                    #nop = str(self.read_txt_from_server(dc_page_path)[0])
                elif doc_type == 'HTML':
                    nop = '1' 
                insert_rows.append((project_id, company_row_id, meta, dc, 'N', nop))
        
        print 'ss', insert_rows
        #print 'tt', update_rows
        #sys.exit()
        if insert_rows:
            ins_stmt = """ INSERT INTO document_mgmt( project_id, company_row_id, meta_data, doc_id, status, no_of_pages) VALUES(%s, %s, %s, %s, %s, %s)  """
            
            m_cur.executemany(ins_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE document_mgmt SET meta_data=%s WHERE project_id=%s AND company_row_id=%s AND doc_id=%s   """
            m_cur.executemany(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
        
    def doc_info_dynamic(self, ijson):
        project_id        = ijson['project_id']
        company_row_id    = ijson['crid']
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry   = """ SELECT doc_id, meta_data FROM document_mgmt WHERE project_id=%s AND company_row_id=%s """%(project_id, company_row_id)
        m_cur.execute(read_qry)
        t_data     = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            doc_id, meta_data = map(str, row)
            d = {'dco_id':doc_id}
            d.update(meta_data)
            res_lst.append(d)
        return [{'message':'done', 'data':res_lst}]
         

    

