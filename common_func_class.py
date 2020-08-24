import os, sys
import shelve

import common.baseobj as bobj
import common.getconfig as getconfig
import common.datastore as datastore
import common.get_doc_data as get_doc_data
import common.filePathAdj as fileabspath
import common.decfile as decfile

#cfgfname = fileabspath.filePathAdj().get_file_path('/var/www/cgi-bin/tasinfosieveresearch/tirsrc2/tirintf2/modules/PDF_APP/config.ini')
#cfgfname = fileabspath.filePathAdj().get_file_path('/var/www/cgi-bin/tasfundamentals/tfmsrc/tfmintf2/config_fundamentals.ini')
cfgfname = fileabspath.filePathAdj().get_file_path('/var/www/cgi-bin/tasfundamentals/tfmsrc/tfmsrcpdf/src/config.ini')
cfgObj = getconfig.getconfig(cfgfname)
#idatapath = cfgObj.get_config('PageAnalysis', 'input')
#ipath, self.opath, self.isdb, self.isenc = bobj.BaseObj().get_project_info()
#self.isdb = int(self.isdb) 
#self.isenc = int(self.isenc) 
#ipath = '/var/www/html/INFOSIEVE_PROJECTS/AssetsDemo/data/output/'
#self.opath = '/var/www/html/INFOSIEVE_PROJECTS/AssetsDemo/data/output/'
#self.isdb = 1
#self.isenc = 1
class common_func:

    def __init__(self, ipath, opath, isdb, isenc):
        self.ipath = ipath
        self.opath = opath 
        self.isdb = isdb
        self.isenc = isenc
         
    def return_data(self, data_path):
        dd = datastore.read_data_fname(data_path, self.isdb, self.isenc)
        data_dict = dd.get('data', {})
        return data_dict

    def read_idata(self, fname):
        decObj = decfile.decfile()
        decObj.open(fname, 'r')
        lines = map(lambda x:x.strip("\n"), decObj.readlines())
        decObj.close()
        return lines


    def get_visual_group_proj_dict(self, doc_id, page_no): 
        ci_odir = cfgObj.get_config('MOD_DIRNAME', 'visprojdict')
        fname = '%s.sh' %(page_no)
        shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        print shname
        if not os.path.exists(shname): 
            print >> sys.stderr, 'Visual Group projected dict not found! ' 
            return {}

        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        res_dict = shv.get('vis_proj_dict', {})
        return res_dict

    def get_parametric_data_level(self, doc_id, page_no, level):
        ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
        fname = '%s_%s.sh'%(page_no, level) 
        #ph_data_path = os.path.join(ipath, str(doc_id), ph_odir, fname)
        ph_data_path = os.path.join(self.opath, str(doc_id), ph_odir, fname)
        if not os.path.exists(ph_data_path): 
            print >> sys.stderr, 'PH DATA not found! '
            return {}
        return return_data(ph_data_path)


    def get_parametric_data(self, doc_id, page_no):
        ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
        fname = '%s.sh'%(page_no) 
        #ph_data_path = os.path.join(ipath, str(doc_id), ph_odir, fname)
        ph_data_path = os.path.join(self.opath, str(doc_id), ph_odir, fname)
        if not os.path.exists(ph_data_path): 
            print >> sys.stderr, 'PH DATA not found! '
            return {}
        return return_data(ph_data_path)

    def get_triplets_shelve(self, doc_id, page_no, level):
        ct_odir = cfgObj.get_config('PageAnalysis', 'CoverTriplets_odir')
        fname = '%s_%s.sh'%(str(page_no), level)  
        #rm_path = os.path.join(ipath, str(doc_id), ct_odir, fname)
        rm_path = os.path.join(self.opath, str(doc_id), ct_odir, fname)
        if not os.path.exists(rm_path):
            print >> sys.stderr, 'Triplet Shelve not found! ' 
            return []
        return return_data(rm_path)

    def get_num_behave_shelve(self, doc_id, page_no):
        drm_odir = cfgObj.get_config('PageAnalysis', 'num_behave_odir')
        fname = '%s.sh'%(str(page_no))
        #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
        rm_path = os.path.join(self.opath, str(doc_id), drm_odir, fname)
        if not os.path.exists(rm_path):
            print >> sys.stderr, 'Number Behavior Shelve not found! ' 
            return []

        dd = datastore.read_data_fname(rm_path, self.isdb, self.isenc)
        data_dict = dd.get('data', {})
        return data_dict


    def get_media_box(self, doc_id, pno):
        rm_path = os.path.join(self.opath, str(doc_id), "db", str(pno), 'pdfdata.db')
        print rm_path 
        if not os.path.exists(rm_path):
            print >> sys.stderr, ' pdfdata not found' 
            return []

        dd = datastore.read_data_fname(rm_path, self.isdb, self.isenc, {}, 'pdfdata')
        data_dict = dd.get('page_master', [])
        if data_dict:  
            bbox_dict = data_dict[0].get('bbox', {})
            if bbox_dict:
              return "%s_%s_%s_%s" %(bbox_dict['x0'], bbox_dict['y0'], bbox_dict['w'], bbox_dict['h'])
        return ""



    def get_nonG_shelve(self, doc_id):
        drm_odir = cfgObj.get_config('applicator', 'TAS_Topic_Mapped_NonG')
        fname = '%s.sh'%(str(doc_id))
        #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
        rm_path = os.path.join(self.opath, str(doc_id), drm_odir, fname)
        if not os.path.exists(rm_path):
            print >> sys.stderr, 'Number Behavior Shelve not found! ' 
            return []

        dd = datastore.read_data_fname(rm_path, self.isdb, self.isenc)
        data_dict = dd.get('nong_data', {})
        return data_dict



    def get_proj_rm(self, doc_id, page_no, level):
        drm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
        fname = '%s_%s.sh'%(str(page_no), level)
        #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
        rm_path = os.path.join(self.opath, str(doc_id), drm_odir, fname)
        if not os.path.exists(rm_path):
            print >> sys.stderr, 'Projected RM not found! ' 
            return []

        return return_data(rm_path)

    def get_synthe_dict(self, doc_id, page_no, level): 

        slt_odir = cfgObj.get_config('PageAnalysis', 'coverpagesynthesizer_odir')
        fname = '%s_%s.sh' %(page_no, level)
        #CellDictpath = os.path.join(ipath, str(doc_id), slt_odir, fname)
        CellDictpath = os.path.join(self.opath, str(doc_id), slt_odir, fname)
        #print CellDictpath
        if not os.path.exists(CellDictpath): 
            print CellDictpath 
            print >> sys.stderr, 'SLT not found! ' 
            return {}

        return return_data(CellDictpath)
        dd = datastore.read_data_fname(CellDictpath, self.isdb, self.isenc)
        return dd

    def get_slt_dict(self, doc_id, page_no, level): 

        slt_odir = cfgObj.get_config('PageAnalysis', 'slt_data_odir')
        fname = '%s_%s.sh' %(page_no, level)
        #CellDictpath = os.path.join(ipath, str(doc_id), slt_odir, fname)
        CellDictpath = os.path.join(self.opath, str(doc_id), slt_odir, fname)
        #print CellDictpath
        if not os.path.exists(CellDictpath): 
            print CellDictpath 
            print >> sys.stderr, 'SLT not found! ' 
            return {}

        #return return_data(CellDictpath)
        dd = datastore.read_data_fname(CellDictpath, self.isdb, self.isenc)
        return dd


    def get_cell_dict(self, doc_id, page_no): 
        cpath = cfgObj.get_config('MOD_DIRNAME', 'celldict')
        fname = '%s.sh' %(page_no)
        CellDictpath = os.path.join(ipath, str(doc_id), cpath, fname)
        print 'CELL DICT PATH : ', CellDictpath
        #sys.exit()
        if not os.path.exists(CellDictpath): 
 #           print >> sys.stderr, 'Cell dict not found! ', CellDictpath 
            return {}
        #sys.exit()
        #print CellDictpath
        shv = datastore.read_data_fname(CellDictpath, self.isdb, self.isenc)
        print 'LLLLLLLLLLLLLL', shv.keys() 
    
        cell_dict = shv.get('cell_dict', {})
        #print cell_dict
        return cell_dict

    def get_clone_cell_dict(self, doc_id, page_no): 
        cpath = cfgObj.get_config('MOD_DIRNAME', 'CLONE_PATH')
        fname = '%s.sh' %(page_no)
        CellDictpath = os.path.join(ipath, str(doc_id), cpath, fname)
        print 'CELL DICT PATH : ', CellDictpath
        #sys.exit()
        if not os.path.exists(CellDictpath): 
#            print >> sys.stderr, 'Cell dict not found! ', CellDictpath 
            return {}
        #print CellDictpath
        #shv = datastore.read_data_fname(CellDictpath, self.isdb, self.isenc)
        shv = shelve.open(CellDictpath, 'r') 
        cell_dict = shv.get('data', {})
        #print cell_dict
        return cell_dict


    def get_font_dict(self, doc_id, page_no): 
        cpath = cfgObj.get_config('MOD_DIRNAME', 'fontdict')
        fname = '%s.sh' %(page_no)
        #FontDictpath = os.path.join(ipath, str(doc_id), cpath, fname)
        FontDictpath = os.path.join(self.opath, str(doc_id), cpath, fname)
        if not os.path.exists(FontDictpath): 
            print >> sys.stderr, 'Font dict not found! ', FontDictpath 
            return {}
        shv = datastore.read_data_fname(FontDictpath, self.isdb, self.isenc)
        font_dict = shv.get('font_dict', {})
        return font_dict



    def get_num_grid(self, doc_id, page_no):
        cpath = cfgObj.get_config('MOD_DIRNAME', 'numgrid')
        fname = '%s.sh' %(page_no)
        #numpath = os.path.join(ipath, str(doc_id), cpath, fname)
        numpath = os.path.join(self.opath, str(doc_id), cpath, fname)
        if not os.path.exists(numpath): 
            print >> sys.stderr, 'NUM GRID dict not found! ', numpath 
            return {}
        shv = datastore.read_data_fname(numpath, self.isdb, self.isenc)
        num_dict = shv.get('data', {})
        return num_dict

    def get_visual_group_dict(self, doc_id, page_no): 
        ci_odir = cfgObj.get_config('MOD_DIRNAME', 'visdict')
        fname = '%s.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        if not os.path.exists(shname): 
            print >> sys.stderr, 'Visual Group dict not found! ' 
            return {}

        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        cell_info_dict = shv.get('vis_dict', {})
        return cell_info_dict


    def write_cagr_shelve(self, doc_id, page_no, CellInfoDict, level): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cagr_result_odir')
        fname = '%s_%s.sh' %(page_no, level)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        datastore.make_dirs(os.path.join(self.opath, str(doc_id), ci_odir))

        d = {}
        d['data'] = CellInfoDict
        datastore.write_data_fname(shname, self.isdb, self.isenc, d)
        return


    def update_cell_info_dict_level(self, doc_id, page_no,  CellInfoDict, level): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s_%s.sh' %(page_no, level)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        #print shname, 'sh name'        
        #for k, vs in CellInfoDict.items():
        #        print k, vs
        d = {}
        d['cell_info_dict'] = CellInfoDict
        print "ISSS", self.isdb
        datastore.write_data_fname(shname, self.isdb, self.isenc, d)
        return




    def update_cell_info_dict(self, doc_id, page_no,  CellInfoDict): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)

        d = {}
        d['cell_info_dict'] = CellInfoDict
        datastore.write_data_fname(shname, self.isdb, self.isenc, d)
        return

    def is_cell_info_dict_exists(self, doc_id, page_no):
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        print 'shname: ', shname   
        if os.path.exists(shname):
           d = datastore.read_data_fname(shname, self.isdb, self.isenc)
           if d.get('cell_info_dict', {}):
              return 1
        return 0 

    def get_cell_info_dict_level(self, doc_id, page_no, level): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s_%s.sh' %(page_no, level)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
#        print 'sssss : ', shname
        if not os.path.exists(shname): 
            #print >> sys.stderr, 'Cell INFO dict not found! ', shname
            return {}

        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        cell_info_dict = shv.get('cell_info_dict', {})
        return cell_info_dict


    def get_cell_info_dict(self, doc_id, page_no): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        print 'sssss : ', shname
        if not os.path.exists(shname): 
#            print >> sys.stderr, 'Cell INFO dict not found! ' 
            return {}
        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        cell_info_dict = shv.get('cell_info_dict', {})
        #print "CELL INFO DICT ", cell_info_dict
        return cell_info_dict

    def get_cell_info_dict_1(self, doc_id, page_no): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s_HRA.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        print 'sssss : ', shname
        if not os.path.exists(shname): 
#            print >> sys.stderr, 'Cell INFO dict not found! ' 
            return {}

        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        cell_info_dict = shv.get('cell_info_dict', {})
        return cell_info_dict

    def get_cell_info_dict_2(self, doc_id, page_no): 
        ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        #fname = '%s_HRB.sh' %(page_no)
        fname = '%s.sh' %(page_no)
        #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
        shname = os.path.join(self.opath, str(doc_id), ci_odir, fname)
        #print shname
        if not os.path.exists(shname): 
#            print >> sys.stderr, 'Cell INFO dict not found! ' 
            return {}

        shv = datastore.read_data_fname(shname, self.isdb, self.isenc)
        cell_info_dict = shv.get('cell_info_dict', {})
        return cell_info_dict




    def write_fp_curr_result(self, doc_id, page_no, cell_info_dict):
        cid_odir = cfgObj.get_config('PageAnalysis', 'fp_curr_result')
        fname = '%s.sh' %(page_no)
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = {}
        d['data'] = cell_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)
        return 

    def get_fp_curr_result(self, doc_id, page_no):
        cid_odir = cfgObj.get_config('PageAnalysis', 'fp_curr_result')
        fname = '%s.sh' %(page_no)
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = datastore.read_data_fname(ofname, self.isdb, self.isenc)
        data = d.get('data', {})
        return data

    def read_number_curr_result(self, doc_id, page_no):
        cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
        fname = '%s.sh' %(page_no)
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = datastore.read_data_fname(ofname, self.isdb, self.isenc)
        cell_info_dict = d.get('data', {})
        return cell_info_dict


    def write_number_curr_result(self, doc_id, page_no, cell_info_dict):
        cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
        fname = '%s.sh' %(page_no)
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = {}
        d['data'] = cell_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)
        return 

    def get_number_curr_result(self, doc_id, page_no):
        cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
        #print "number_curr_result path:", cid_odir
        fname = '%s.sh' %(page_no)
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = datastore.read_data_fname(ofname, self.isdb, self.isenc)
        data = d.get('data', {})
        return data


    def write_cell_info_dict(self, doc_id, page_no, cell_info_dict):
        cid_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
        fname = '%s.sh' %(page_no)
        print "CID PATH : ", cid_odir, fname
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = {}
        d['cell_info_dict'] = cell_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)
        return 

    def write_font_data_dict(self, doc_id, page_no, cell_info_dict):
        cid_odir = cfgObj.get_config('MOD_DIRNAME', 'fontdict')
        fname = '%s.sh' %(page_no)
        print "CID PATH : ", cid_odir, fname
        ofname = os.path.join(self.opath, str(doc_id), cid_odir, fname)
        d = {}
        d['font_dict'] = cell_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)


    def get_metadata_dict(self, docid):
        cid_odir = cfgObj.get_config('ExtractEntity', 'EntityOutput_odir')
        fname = '%s.sh' %(docid)
        ofname = os.path.join(self.opath, str(docid), cid_odir, fname)
        shv = datastore.read_data_fname(ofname, self.isdb, self.isenc)
        metadat_dict = shv.get('data', {})
        return metadat_dict

    def write_ph_info_dict_level(self, doc_id, pno, ph_info_dict, level):
        ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
        fname = '%s_%s.sh' %(pno, level)

        #ofname = os.path.join(ipath, str(doc_id), ph_odir, fname)
        ofname = os.path.join(self.opath, str(doc_id), ph_odir, fname)
        datastore.rmfile(ofname)
        d = {}
        d['data'] = ph_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)
        return


 
    
    def write_ph_info_dict(self, doc_id, pno, ph_info_dict):
        ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
        fname = '%s.sh' %(pno)

        #ofname = os.path.join(ipath, str(doc_id), ph_odir, fname)
        ofname = os.path.join(self.opath, str(doc_id), ph_odir, fname)
        datastore.rmfile(ofname)
        d = {}
        d['data'] = ph_info_dict
        datastore.write_data_fname(ofname, self.isdb, self.isenc, d)
        return


    def write_projected_rm(self, doc_id, page_no, level, rm_lines):
        prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
        fpath = os.path.join(self.opath, str(doc_id), prm_odir, '')
        #os.system('mkdir -p %s' %fpath)
        datastore.make_dirs(fpath)
        filename = '%s_%s.sh' %(str(page_no), level)
        fname = os.path.join(fpath, filename)
        d = {}
        d['data'] = rm_lines[:]
        datastore.write_data_fname(fname, self.isdb, self.isenc, d)

        return

    def write_projected_rm_file(self, doc_id, page_no, level, rm_lines):
        prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
        fpath = os.path.join(self.opath, str(doc_id), prm_odir, '')
        #os.system('mkdir -p %s' %fpath)
        datastore.make_dirs(fpath)
        filename = '%s_%s.txt' %(str(page_no), level)
        fname = os.path.join(fpath, filename)

        f1 = open(fname, "w")
        for e in rm_lines:
            es = '\t'.join(e)
            f1.write('%s\n' %es)
        f1.close()

        return


    def get_relation_dict(self, doc_id, page_no):
        rel_odir = cfgObj.get_config('PageAnalysis', 'RelationResults_odir')
        fname = '%s.sh'%(str(page_no))
        #sh_path = os.path.join(ipath, str(doc_id), rel_odir, fname)
        sh_path = os.path.join(self.opath, str(doc_id), rel_odir, fname)
        if not os.path.exists(sh_path): 
            print >> sys.stderr, 'relation dict not found! ' 
            return {}

        dd = datastore.read_data_fname(sh_path, self.isdb, self.isenc)
        rd = dd.get('data', {})
        return rd

##################################################################

    def get_projected_columns(self, level, r_type):
        fname = os.path.join(idatapath, 'projected_columns.txt')
        #lines = open(fname, 'r').readlines()
        lines = read_idata(fname)
        for line in lines:
            line_sp = line.strip('\n').split('\t')
            if line_sp[0] == level+":"+r_type:
                return map(lambda x:x.strip(), line_sp[1:])
        return []  
 
    def read_token_simplify_file(self, level):
        fname = os.path.join(idatapath, 'token_simplify_%s.txt' %level)
        fname = fileabspath.filePathAdj().get_file_path(fname)
        #lines = open(fname, 'r').readlines()
        lines = read_idata(fname)
    
        d_dict = {} 
        for line in lines:
            line_sp = line.split('\t')
            d_dict[line_sp[0].strip()] = line_sp[1].strip()
        return d_dict     


    def read_rule_selection_file(self, level):
        fname = os.path.join(idatapath, 'rule_igs_%s.txt' %level)
        fname = fileabspath.filePathAdj().get_file_path(fname)
        #lines = open(fname, 'r').readlines()
        lines = read_idata(fname)

        d_dict = {} 
        for line in lines:
            if not line.strip(): continue
            lsp = line.strip('\n').split('\t')
            d_dict[lsp[0].strip()] = []
            for l in lsp[1:]:
                if not l.strip(): continue
                d_dict[lsp[0].strip()].append(l.strip())

        return d_dict


    def get_level_igs_projrm(self, level):
        level_igs = []
        fname = os.path.join(idatapath, 'token_controller_%s.txt' %level)
        fname = fileabspath.filePathAdj().get_file_path(fname)
        #lines = open(fname, 'r').readlines()
        lines = read_idata(fname)
        if not lines: return level_igs
        for line in lines[1:]:
            line = line.strip('\n')
            ls = line.split('\t')
            if ls[3].strip() == 'None': continue
            if str(level) == ls[0].strip(): 
                level_igs.append(ls[1:])
        return level_igs


    def get_level_igs(self, level):
        level_igs = []
        fname = os.path.join(idatapath, 'token_controller_%s.txt' %level)
        fname = fileabspath.filePathAdj().get_file_path(fname)
        #print fname, 'igs file name'
        #lines = open(fname, 'r').readlines()
        lines = read_idata(fname)
        if not lines: return level_igs
        for line in lines[1:]:
            line = line.strip('\n')
            ls = line.split('\t')
            if ls[3].strip() == 'None': continue
            if 1:#str(level) == ls[0].strip(): 
                level_igs.append(ls[1:])
        #print level_igs, 'level igs'        
        return level_igs

