import os, sys, json, copy, sets, lmdb, sqlite3
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import data_builder.db_data as db_data
db_dataobj  = db_data.PYAPI()
import utils.meta_info as meta_info
m_obj   = meta_info.MetaInfo()
import report_year_sort
import utils.convert as scale_convert
sconvert_obj   = scale_convert.Convert()
import numpy as np
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import perm_comb
percomb = perm_comb.perm_comb()
import tavinash
import pyapi
p_obj   = pyapi.PYAPI()
import gcomp

class Validate():
    def parse_ijson(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        return company_name, model_number, deal_id, project_id, company_id

    def get_order_phs(self, phs, dphs, report_map_d, ijson):
        doc_ph_d    = {}
        pk_map_d    = {}
        for ii, ph in enumerate(phs):
            pk_map_d[ph['k']]    = ph['dph']
            doc_id, table_id = ph['g'].split()[0].split('-')
            if ph['n']:
                doc_ph_d.setdefault(doc_id, {}).setdefault(ph['n'], {})[ph['k']]   = 1
        dphs_ar         = report_year_sort.year_sort(dphs.keys())
        #print dphs_ar
        reported_phs    = {}
        for dph in dphs_ar:
            doc_ids = dphs[dph].keys()
            doc_ids.sort()
            d_ph    = dph[:-4]
            d_year  = dph[-4:]
            #print '\n+++++++++++++++++++++++++++++++++++'
            #print dph, ijson
            for doc_id in doc_ids:
                if doc_id not in doc_ph_d:continue
                doc_phs = doc_ph_d[doc_id]
                doc_phs_ar = report_year_sort.year_sort(doc_phs.keys())
                f   = 0
                for ph in doc_phs_ar:
                    t_ph    = ph[:-4]
                    t_year  = ph[-4:]
                    #print '\t', ph
                    if dph == ph:
                        reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                        f   = 1
                        #break
                if 1:#f == 0:
                    for ph in doc_phs_ar:
                        t_ph    = ph[:-4]
                        t_year  = ph[-4:]
                        if d_year == t_year:
                            if d_ph in report_map_d[t_ph]:
                                if t_ph == 'H1':
                                    tmpph   = 'Q2'+t_year
                                    if tmpph not in doc_ph_d[doc_id] and (ph not in reported_phs):
                                        reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                elif t_ph == 'H2':
                                    tmpph   = 'Q4'+t_year
                                    if tmpph not in doc_ph_d[doc_id] and (ph not in reported_phs):
                                        reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                else:
                                    reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                #break
        fphs    = report_year_sort.year_sort(reported_phs.keys())
        phs = []
        for ph in fphs:
            allks   = reported_phs[ph].keys()
            allks.sort(key=lambda x:0 if ph[:-4] == pk_map_d[x][0] else 1)
            tph = pk_map_d[allks[0]]
            phs.append({'k':allks[0], 'ph':ph, 'dph':tph})
        return phs, reported_phs

    def get_reported_phs(self, phlist):
        f_phs   = []
        f_ar    = []
        for i, r in enumerate(phlist):
            f_ar.append(0)
            dph, cph    = r
            try:
                dyrear  = int(dph[-4:])
            except:continue
            try:
                cyrear  = int(cph[-4:])
            except:continue
            if dph[:-4] not in ['Q1', 'Q1ESD', 'Q2', 'Q2ESD', 'H1', 'H1ESD', 'Q3', 'Q3ESD', 'M9', 'M9ESD', 'Q4', 'Q4ESD', 'H2', 'H2ESD', 'FY', 'FYESD']:continue
            if cph[:-4] not in ['Q1', 'Q1ESD', 'Q2', 'Q2ESD', 'H1', 'H1ESD', 'Q3', 'Q3ESD', 'M9', 'M9ESD', 'Q4', 'Q4ESD', 'H2', 'H2ESD', 'FY', 'FYESD']:continue
            dd  = {'dph':(dph[:-4], dyrear), 'k':str(i), 'n':cph, 'g':'%s-%s DOC'%(i+1, i+1)}
            f_phs.append(dd)
        reported_phs    = self.form_ph_cols(f_phs, {})
        pk_d    = {}
        for ph, pk_ar in reported_phs.items():
            for k in pk_ar:
                f_ar[int(k)]    = 1
        return f_ar
        
            
        
        


    def form_ph_cols(self, phs, ijson, ignore_dphs={}, rsp_phs={}):
        self.report_map_d    = {
                            'Q1'    : {'Q1':1},
                            'FY'    : {'FY':1,'Q4':1},
                            'Q2'    : {'Q2':1,'H1':1},
                            'Q3'    : {'Q3':1, 'M9':1},
                            'Q4'    : {'Q4':1,'FY':1},
                            'H1'    : {'H1':1, 'Q2':1},
                            'H2'    : {'H2':1, 'Q4':1,'FY':1},
                            'M9'    : {'M9':1, 'Q3':1},
                            }
        dphs    = {}
        for ph in phs:
            doc_id, table_id = ph['g'].split()[0].split('-')
            dphs.setdefault('%s%s'%ph['dph'], {})[doc_id]    = 1
        report_map_rev_d    = {}
        for k, v_d in self.report_map_d.items():
            for v in v_d.keys():
                report_map_rev_d.setdefault(v, {})[k]   = 1
        f_phs, reported_phs               = self.get_order_phs(phs, dphs, self.report_map_d, ijson)
        done_ph = {}
        all_years   = {}
        for ph in f_phs:
            done_ph[ph['ph']]   = 1
            try:
                all_years[int(ph['ph'][-4:])]   = 1
            except:pass
        pt_arr         = report_year_sort.year_sort(rsp_phs.keys())
        d_new_phs       = []
        for ph in pt_arr:
            if ph in ignore_dphs:continue
            if ph in done_ph:continue
            dd  = {'ph':ph, 'new':"Y", 'k':rsp_phs[ph][-1], 'g':ph, 'dph':(ph[:-4], int(ph[-4:])), 'reported':"Y"}
            f_phs   = [dd]+f_phs
            reported_phs[ph]    = {}
            for tk in rsp_phs[ph]:
                reported_phs[ph][tk]    = 1
                
            d_new_phs.append(copy.deepcopy(dd))
        dphs_all         = report_year_sort.year_sort(dphs.keys()+pt_arr)
        #for k, v in reported_phs.items():
        #    print k, v
        #sys.exit()
        done_ph             = {}
        f_phs_d             = {}
        ph_map_d    = {}
        for ph in f_phs:
            f_phs_d[ph['ph']]   = ph['k']
            done_ph[ph['ph']]  = 1
            ph_map_d[ph['ph']]    = ph['k']
        exists_ph       = {}
        for ph in f_phs:
            exists_ph[ph['ph']] = 1
        n_ph    = {}
        ph_pkmap    = {}
        if ijson.get('project_name', '').lower() != 'schroders':
            for ph in phs:
                ph_pkmap[ph['k']]    = ph['dph']
                if ph['n'] in exists_ph:continue
                if not ph['n']:continue
                try:
                    ttyear  = int(ph['n'][-4:])
                except:continue
                if ph['n'] in ignore_dphs:continue
                n_ph.setdefault(ph['n'], []).append( ph['k'])

        if n_ph:
            ph_map_d    = {}
            f_phs_d     = {}
            for ph in f_phs:
                ph_map_d[ph['ph']]   = ph
                f_phs_d[ph['ph']]   = ph['k']
            for tph, pks in n_ph.items():
                doc_ph_d        = ph_pkmap[pks[-1]]
                dph             = (tph[:-4], int(tph[-4:]))
                rePflg          = 'Y'
                k1              = tph
                k1          = pks[-1]
                #print tph, pks, [doc_ph_d]
                if tph[:-4] in report_map_rev_d.get(doc_ph_d[0], {}):
                    #print '\t', report_map_rev_d.get(doc_ph_d[0], {})
                    k1          = pks[-1]
                    rePflg      = ''
                    #dph         = doc_ph_d
                
                    
                ph  = {'ph':tph, 'new':"Y", 'k':k1, 'g':tph, 'dph':dph, 'reported':rePflg} #ph_pkmap[pks[-1]]} #(tph[:-4], int(tph[-4:]))}
                ph_map_d[ph['ph']]   = ph
                f_phs_d[ph['ph']]   = ph['k']
                #reported_phs[tph]    = {}
                if rePflg == '':
                    for tk in pks:
                        tmpdoc_ph_d        = ph_pkmap[tk]
                        if tmpdoc_ph_d == doc_ph_d:
                            reported_phs.setdefault(tph, {})[tk]    = 1
                d_new_phs.append(copy.deepcopy(ph))
            dphs_ar             = report_year_sort.year_sort(f_phs_d.keys())
            dphs_all            = copy.deepcopy(dphs_ar)
            dphs_ar.reverse()
            f_phs               = map(lambda x:ph_map_d[x], dphs_ar)
        #for k, v in reported_phs.items():
        #    print k, v
        #sys.exit()
        done_ph             = {}
        f_phs_d             = {}
        ph_map_d    = {}
        for ph in f_phs:
            f_phs_d[ph['ph']]   = ph['k']
            done_ph[ph['ph']]  = 1
            ph_map_d[ph['ph']]    = ph['k']
        pk_d    = {}
        for ph in f_phs:
            ks  = reported_phs.get(ph['ph'], {}).keys()
            if ph['k'] not in ks:
                ks.append(ph['k'])
            ks.sort(key=lambda x:0 if x == ph['k'] else 1)
            pk_d[ph['ph']]   = ks
        return pk_d

    def get_g_chain_number_grid(self, data, pk_d):
        indexs  = {}
        for ktup, idxs in pk_d.items():
            for ind in idxs:
                indexs[ind] = 1
        tmpd    = {}
        taxo_d  = {}
        g_grp   = {}
        for ii, rr in enumerate(data):
            taxo_d[str(rr['t_id'])] = (ii, rr)
        for ii, rr in enumerate(data):
            if rr.get('f') == 'Y':
                for k, v in rr.items():
                    if isinstance(v, dict) and v.get('f_col'):
                        op_d    = {}
                        t_ar    = []
                        findx   = 0
                        for ft in v['f_col'][0]:
                            if ft.get('operator'):
                                op_d[ft['operator']]    = 1
                            if ft['type'] == 't':
                                if ft['description'] == 'Not Exists' or str(ft['taxo_id']) not in taxo_d:
                                        op_d    = {}
                                        t_ar    = {}
                                        break
                                t_ar.append(taxo_d[str(ft['taxo_id'])][0])
                                if taxo_d[str(ft['taxo_id'])][0] in indexs:
                                    findx   = 1
                        if t_ar and op_d and  findx == 1:
                            g_grp[(tuple(t_ar))]   = ii
                            break
        ks  = g_grp.keys()
        ks.sort()
        map_d   = {}
        final_ar    = []
        for grp in ks:
            for ktup in pk_d.keys():
                grp_ar  = []
                for tid in grp: 
                    rr  = data[tid]
                    tmp_ar  = []
                    for i, pk in enumerate(ktup):
                        if pk in rr:
                            v_d     = rr[pk]
                            clean_value = numbercleanup_obj.get_value_cleanup(v_d['v'])
                            if clean_value:
                                clean_value = float(clean_value)
                                if clean_value:
                                    s   = v_d.get('phcsv', {}).get('s', '')
                                    if s:
                                        tv, factor  = sconvert_obj.convert_frm_to_1(s, '1', clean_value)
                                        if factor and tv:
                                            clean_value = float(tv.replace(',', ''))
                                map_d.setdefault((tid, clean_value), {})[pk]   = 1
                                #if clean_value not in tmp_ar:
                                tmp_ar.append(clean_value)
                    if tmp_ar:
                        tmp_ar  = list(sets.Set(tmp_ar))
                        grp_ar.append((copy.deepcopy(tmp_ar), tid, 'Y' if tid == g_grp[grp] else 'N', ktup))
                if len(grp_ar) > 1:
                    final_ar.append(grp_ar)
        return final_ar, map_d



    def clean_compare(self, v1, s1, v2, s2):
         # s1 and s2 are not equal

         if v1 == '': return 0
         if v2 == '': return 0

         # convert - s1 
         v2_mod = self.clean_conv_value(v2, s2, s1)
         v1_mod  = self.clean_conv_value(v1, s1, s1)
          
         if abs(abs(v1_mod) - abs(v2_mod)) <= 1:
            return 1


         # convert - s2
         v2_mod = self.clean_conv_value(v2, s2, s2)
         v1_mod  = self.clean_conv_value(v1, s1, s2)

         if abs(abs(v1_mod) - abs(v2_mod)) <= 1:
            return 1


         return 0     
         

    def clean_conv_value(self, v1, s1, con_s):
         clean_value = numbercleanup_obj.get_value_cleanup(v1)
         if clean_value:
            clean_value = float(clean_value)
            if clean_value:
                s   = s1
                if s:
                    tv, factor  = sconvert_obj.convert_frm_to_1(s, con_s, clean_value)
                    if factor and tv:
                        clean_value = float(tv.replace(',', ''))
         #print clean_value  
         return float(clean_value)  

    def get_checksum_data(self, res):
        taxo_d  = {}
        for i, r in enumerate(res[0]['data']):
            taxo_d[str(r['t_id'])]  = (i, r)
        def read_formula_value(v, level, f_derived):
            op_d    = {}
            findx   = 0
            v_ar    = []
            #print
            for ft in v['f_col'][0]:
                #print '\t', ft.get('description')
                if ft.get('operator'):
                    if ft.get('PH_D') == 'Y':
                        f_derived['Y']  = 1
                    op_d[ft['operator']]    = 1
                    if ft.get('type', '') == 't':
                        if level > 1:
                            if ft.get('operator') == '=':continue
                        if ft['description'] == 'Not Exists' or str(ft['taxo_id']) not in taxo_d or not ft.get('k'):
                            #print '\nBREAK ', ft
                            op_d    = {}
                            v_ar    = {}
                            break
                        rr  = taxo_d[str(ft['taxo_id'])][1]
                        if ft['k'] in rr :
                            pk      = ft['k']
                            v_d     = rr[pk]
                            got_value   = 0
                            if v_d.get('f_col') and  level == 1 and ft.get('operator') != '=':
                                tmp_ar  = read_formula_value(v_d, level+1, f_derived)
                                nval    = []
                                pkar    = []
                                for vv in tmp_ar:
                                    pkar    += [vv[3]]
                                    nval    += [vv[0]]
                                if nval:
                                    got_value   = 1
                                    if len(nval) > 1:
                                        f_derived['More than one']  = 1
                                    v_ar.append((nval, taxo_d[str(ft['taxo_id'])][0], 'Y' if ft['operator'] == '=' else 'N', tuple(pkar)))
                            if got_value == 0:
                                clean_value = numbercleanup_obj.get_value_cleanup(v_d['v'])
                                if clean_value:
                                    clean_value = float(clean_value)
                                    if clean_value:
                                        s   = v_d.get('phcsv', {}).get('s', '')
                                        if s:
                                            tv, factor  = sconvert_obj.convert_frm_to_1(s, '1', clean_value)
                                            if factor:
                                                clean_value = float(tv.replace(',', ''))
                                        v_ar.append(([clean_value], taxo_d[str(ft['taxo_id'])][0], 'Y' if ft['operator'] == '=' else 'N', ((taxo_d[str(ft['taxo_id'])][0], pk), )))
            return v_ar
        grp_ar  = []
        for i, r in enumerate(res[0]['data']):
            for ph in res[0]['phs']:
                if ph['k'] in r and r[ph['k']].get('c_s'):
                    #print '\n', [r['t_l'], ph , r[ph['k']].get('c_s'), r[ph['k']]['v']]
                    f_derived   = {}
                    v_ar    = read_formula_value(r[ph['k']], 1, f_derived)
                    #print v_ar
                    if len(v_ar) > 1 and 'Y' in f_derived:# and 'More than one' in f_derived:   
                        #grp_ar.append(v_ar)
                        v_ar.sort(key=lambda x:0 if x[1] == 'Y' else 1)
                        num_ar  = map(lambda x:x[0], v_ar)
                    
                        #CALL HERE
        return grp_ar


    def get_xml_groups_sp_path(self):
        lmdb_path   = '/var/www/html/akshay/test_results/1_156/CPH_GROUPDATA/MGDOC_GROUP/'
        env      = lmdb.open(lmdb_path, max_dbs=27)
        db_name     = env.open_db('others')
        transaction =  env.begin(write=False, db=db_name)
        cursor      =  transaction.cursor()
        output_d    = {}
        for key, value in cursor:
            #print key, value.split('|')
            output_d[key] = value.split('|') #list(eval(value))
        transaction.commit()
        cursor.close()
        return output_d



    def get_xml_groups_cf(self, output_path):
        lmdb_path   = '%s/CPH_GROUPDATA/MGDOC_GROUP/'%(output_path)
        env      = lmdb.open(lmdb_path, max_dbs=27)
        db_name     = env.open_db('others')
        transaction =  env.begin(write=False, db=db_name)
        cursor      =  transaction.cursor()
        output_d    = {}
        for key, value in cursor:
            #print key, value.split('|')
            output_d[key] = value.split('|') #list(eval(value))
        transaction.commit()
        cursor.close()
        return output_d




    def get_xml_groups(self, output_path):
        lmdb_path   = '%s/GROUPDATA/MGDOC_GROUP/'%(output_path)
        env      = lmdb.open(lmdb_path, max_dbs=27)
        db_name     = env.open_db('others')
        transaction =  env.begin(write=False, db=db_name)
        cursor      =  transaction.cursor()
        output_d    = {}
        for key, value in cursor:
            #print key, value.split('|')
            output_d[key] = value.split('|') #list(eval(value))
        transaction.commit()
        cursor.close()
        return output_d



    def validate_across(self, ijson):
        #print 'In validate_across: '
        #sys.exit()
        given_table_ids = []
        #print given_table_ids 
        #sys.exit() 
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        #projects, p_d    = p_obj.read_gbl_companyConfig(db_file, 0, [])
        #for p in projects:
        #    for tt in p['tt_list']:
        #        p.setdefault('configured', {})[(tt['mh'], tt.get('grpid', ''))] = 1
        #if not projects:
        #    projects    = [{'st':'Both', 'n':'New Project'}]
        projects, p_d    = [], {}
        ijson['project_name']   = ''
        ijson['year'] = 50
        output_path =  '/mnt/rMB_db/%s/'%(company_id)
        os.system('rm -rf '+output_path)
        cmd = 'export LD_LIBRARY_PATH=.'
        print cmd
        os.system(cmd)
        m_tables, rev_m_tables, doc_m_d,table_type_m = m_obj.get_main_table_info(company_name, model_number)
        j_ar    = []
        db_data_d   = {}
        grp_map_d   = {}
        for ii, table_id in enumerate(doc_m_d.keys()):
            #print 'Running ', ii, ' / ', len(rev_m_tables.keys()), [table_type, ijson['company_name']]
            ijson_c = copy.deepcopy(ijson)
            ijson_c['table_id'] = table_id
            ijson_c['doc_id']   = doc_m_d[table_id]
            j_ar.append(copy.deepcopy(ijson_c))
        ttype_d = {}
        ph_d    = {}
      
        my_data = [ ]
      
        table_ref_dict = {}   
        review_db   = {}
        for ii, ijson_c in enumerate(j_ar):
            #if (ijson_c['table_id'] not in given_table_ids):  continue
            #print 'Running ', ii, ' / ', len(j_ar), [ijson_c['table_id'], ijson.get('doc_id')]

            ijson_c['table_type'] = ijson_c['table_id']
            #if (ijson_c['table_type'] != 'IS'): continue
            #print ijson_c.keys()
            #if 'grpid' in ijson_c:
            #   print ' grpid ', ijson_c['grpid']
           
            res         = db_dataobj.read_table_builder(ijson_c)
            if not res:continue
            if res[0]['message'] != 'done':continue
            keytup          = (ijson_c['table_id'], ijson_c.get('doc_id', ''))
            data    = res[0]['data']
            phs    = res[0]['phs']
            db_data_d[(ijson_c['table_type'], ijson_c.get('grpid', ''))]   = res[0]


            for i2, data_elm in enumerate(data):
                #print 'taxonomy id: ', data_elm['t_id'] 
                #print 'taxonomy: ', data_elm['t_l'] 
                for ph in phs:
                    pkey = ph['k']
                    if pkey in data_elm:
                       ptp = data_elm[pkey]['phcsv']['pt']+data_elm[pkey]['phcsv']['p'] 
                       val = data_elm[pkey]['v']                   
                       #print 'table id: ', data_elm[pkey]['t']
                       #print pkey, val, ptp
                       ref = data_elm[pkey]['x']
                       if (data_elm[pkey]['t'], ref) not in table_ref_dict:
                          table_ref_dict[(data_elm[pkey]['t'], ref)] = {}
                       if (ijson_c['table_type'], ijson_c.get('grpid', '')) not in table_ref_dict[(data_elm[pkey]['t'], ref)]:
                          table_ref_dict[(data_elm[pkey]['t'], ref)][(ijson_c['table_type'], ijson_c.get('grpid', ''))] = (data_elm['t_id'], pkey, ptp, val, data_elm[pkey]['phcsv']['s']) 




            my_data.append((data[:], phs[:], copy.deepcopy(ijson_c), review_db))


        #print table_ref_dict.keys()
        #sys.exit()
        pairwise_gp_dict, pairwise_gp_cf_dict, table_across_comp, table_across_comp_ac, schedule_across_group, schedule_across_group_mul = self.computation_classes(company_name, company_id, output_path, db_data_d, table_ref_dict)
        self.index_group_data(pairwise_gp_dict, company_id, 'EQUAL', i_ar, grp_map_d) # first one is the resultant
        self.index_group_data(pairwise_gp_cf_dict, company_id, 'CF-EQUAL', i_ar, grp_map_d) # first one is the resultant
        self.index_group_data_acr(schedule_across_group, company_id, 'SCHEDULE', i_ar, grp_map_d)
        self.index_group_data_acr(schedule_across_group_mul, company_id, 'SCHEDULE-MUL', i_ar, grp_map_d)
        self.index_group_data_acr(table_across_comp, company_id, 'GCOMP2', i_ar, grp_map_d)
        self.index_group_data_acr(table_across_comp_ac, company_id, 'GCOMP3', i_ar, grp_map_d)

        
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql         = "CREATE TABLE IF NOT EXISTS error_class(row_id INTEGER PRIMARY KEY AUTOINCREMENT, error_type TEXT, table_type TEXT, error_msg TEXT, group_id TEXT, group_name TEXT, data TEXT, ph varchar(256), color TEXT, user_name TEXT, datetime TEXT, edit_flag varchar(1), update_flg varchar(256), done_status)"
        cur.execute(sql)
        sql = "select row_id from error_class where error_type like 'TALLY-%'"
        cur.execute(sql)
        res = cur.fetchall()
        row_ids = {}
        for r in res:
            row_ids[str(r[0])]  = 1
        sql = "delete from error_class where row_id in (%s)"%(', '.join(row_ids.keys()))
        cur.execute(sql)
        cur.executemany('insert into error_class(error_type, error_msg, table_type, group_id, group_name, data, color, update_flg)values(?,?,?, ?, ?, ?, ?, ?)', i_ar)
        conn.commit()
        cur.close() 
        conn.close()
        
        print 'DONE'






    def validate(self, ijson, given_table_ids=[]):
        #print given_table_ids 
        #sys.exit() 
        company_name, model_number, deal_id, project_id, company_id = self.parse_ijson(ijson)
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        #projects, p_d    = p_obj.read_gbl_companyConfig(db_file, 0, [])
        #for p in projects:
        #    for tt in p['tt_list']:
        #        p.setdefault('configured', {})[(tt['mh'], tt.get('grpid', ''))] = 1
        #if not projects:
        #    projects    = [{'st':'Both', 'n':'New Project'}]
        projects, p_d    = [], {}
        ijson['project_name']   = ''
        ijson['year'] = 50
        output_path =  '/mnt/rMB_db/%s/'%(company_id)
        os.system('rm -rf '+output_path)
        cmd = 'export LD_LIBRARY_PATH=.'
        print cmd
        os.system(cmd)
        m_tables, rev_m_tables, doc_m_d,table_type_m = m_obj.get_main_table_info(company_name, model_number)
        j_ar    = []
        db_data_d   = {}
        grp_map_d   = {}
        for ii, table_id in enumerate(doc_m_d.keys()):
            #print 'Running ', ii, ' / ', len(rev_m_tables.keys()), [table_type, ijson['company_name']]
            ijson_c = copy.deepcopy(ijson)
            ijson_c['table_id'] = table_id
            ijson_c['doc_id']   = doc_m_d[table_id]
            j_ar.append(copy.deepcopy(ijson_c))
        ttype_d = {}
        ph_d    = {}
      
        my_data = [ ]
       
        table_ref_dict = {}   
        review_db   = {}
        for ii, ijson_c in enumerate(j_ar):
            if given_table_ids and (ijson_c['table_id'] not in given_table_ids):  continue
            print 'Running ', ii, ' / ', len(j_ar), [ijson_c['table_id'], ijson.get('doc_id')]

            
            #if (ijson_c['table_type'] != 'IS'): continue
            #print ijson_c.keys()
            #if 'grpid' in ijson_c:
            #   print ' grpid ', ijson_c['grpid']
           
            res         = db_dataobj.read_table_builder(ijson_c)
            if not res:continue
            if res[0]['message'] != 'done':continue
            keytup          = (ijson_c['table_id'], ijson_c.get('doc_id', ''))
            data    = res[0]['data']
            phs    = res[0]['phs']


            for i2, data_elm in enumerate(data):
                print 'taxonomy id: ', data_elm['t_id'] 
                print 'taxonomy: ', data_elm['t_l'] 
                for ph in phs:
                    pkey = ph['k']
                    if pkey in data_elm:
                       ptp = data_elm[pkey]['phcsv']['pt']+data_elm[pkey]['phcsv']['p'] 
                       val = data_elm[pkey]['v']                   
                       print 'table id: ', data_elm[pkey]['t']
                       print pkey, val, ptp
            my_data.append((data[:], phs[:], copy.deepcopy(ijson_c), review_db))

        

        self.compute_gcomputation(my_data, given_table_ids)
        return
        sys.exit()
        #def computation_classes(self, company_name, company_id, output_path, db_data_d, table_ref_dict):
        i_ar    = []
        self.index_group_data(pairwise_gp_dict, company_id, 'EQUAL', i_ar, grp_map_d) # first one is the resultant
        self.index_group_data(pairwise_gp_cf_dict, company_id, 'CF-EQUAL', i_ar, grp_map_d) # first one is the resultant
        self.index_group_data_acr(schedule_across_group, company_id, 'SCHEDULE', i_ar, grp_map_d)
        self.index_group_data_acr(schedule_across_group_mul, company_id, 'SCHEDULE-MUL', i_ar, grp_map_d)
        self.index_group_data_acr(table_across_comp, company_id, 'GCOMP2', i_ar, grp_map_d)
        self.index_group_data_acr(table_across_comp_ac, company_id, 'GCOMP3', i_ar, grp_map_d)
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql         = "CREATE TABLE IF NOT EXISTS error_class(row_id INTEGER PRIMARY KEY AUTOINCREMENT, error_type TEXT, table_type TEXT, error_msg TEXT, group_id TEXT, group_name TEXT, data TEXT, ph varchar(256), color TEXT, user_name TEXT, datetime TEXT, edit_flag varchar(1), update_flg varchar(256), done_status)"
        cur.execute(sql)
        sql = "select row_id from error_class where error_type like 'TALLY-%'"
        cur.execute(sql)
        res = cur.fetchall()
        row_ids = {}
        for r in res:
            row_ids[str(r[0])]  = 1
        sql = "delete from error_class where row_id in (%s)"%(', '.join(row_ids.keys()))
        cur.execute(sql)
        cur.executemany('insert into error_class(error_type, error_msg, table_type, group_id, group_name, data, color, update_flg)values(?,?,?, ?, ?, ?, ?, ?)', i_ar)
        conn.commit()
        cur.close() 
        conn.close()
        
        print 'DONE'


    def index_group_data(self, pairwise_dict, company_id, flag, i_ar, grp_map_d):
        # first one is the resultant 
        #print pairwise_gp_dicti
        kstr_d  = {}
        for k, vs_dict in pairwise_dict.items():
            #i_ar.append(('FORMULA_OVERLAP', 'Formula Overlap', ttype, grpid, grpname, str(kstr_d), ''))
            #print '============================================='
            #print 'Pair: ', k
            for line_pair, vs in vs_dict.items():
                #print '   line_pair: ', line_pair
                for v_pair in vs:
                    print v_pair, flag
                    #sys.exit()
                    #flg_ar = [ v_pair[2], v_pair[3] ]
                    #sys.exit()
                    res_flg = v_pair[2]
                    opr_flg = v_pair[3]
                    op_ar   = []
                    for i, v in enumerate([ v_pair[0], v_pair[1] ]):
                        #print '        ++++ ', v 
                        #flg = flg_ar[i]
                        op  = '='
                        if i != 0:
                            op  = '+'
                        else:continue
                        ktup    = v
                        tttup   = k[i]
                        taxoid  = line_pair[i] 
                        #print (tttup, ktup)
                        op_ar.append('%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s'%(taxoid, op,tttup[0], tttup[1], 'CELLFORMULA','', '', '', ktup[0], ktup[1], opr_flg)) # flg
                    tttup   = k[0]
                    k1   = '%s^%s^%s^%s'%(line_pair[0], v_pair[0][0], v_pair[0][1], res_flg)
                    kstr_d.setdefault((tttup[0], k[1][0]), {}).setdefault((k[0],  grp_map_d.get(k[0], k[0][0]), k[1],  grp_map_d.get(k[1], k[1][0])), {})[k1]  = '$$'.join(op_ar)
        for ttype, grpd_d in kstr_d.items():
            for grpid, vd in grpd_d.items():
                i_ar.append(('TALLY-'+flag, flag, '('+', '.join(ttype)+')', str(grpid), '%s-%s'%(grpid[1], grpid[3]), str(vd), '', 'N'))
        #sys.exit()

    def index_group_data_acr(self, gcomp_ddict_dict, company_id, flag, i_ar, grp_map_d):
        # first one is the resultant 
        #print pairwise_gp_dicti
        #ncodeURIComponent(row['taxo_id'])+'@@'+encodeURIComponent(row['operator'])+'@@'+row["tt"]+'@@'+row["grpid"]+'@@'+op_type+'@@'+row["c"]+'@@'+row["s"]+'@@'+row["vt"]+'@@'+row['ph'];        
        kstr_d  = {}
        for ttype, gcomp_ddict in gcomp_ddict_dict.items():
            #print ' ACROSS MUL', ttype 
            for gkey, gval_ar in gcomp_ddict.items():
                #print '================'
                #print ' gkey: ', gkey
                #('=', ('6404-2-', 'Q12011', '(2,234)', 'Mn'), ('CF', ''), 22)
                for ar in gval_ar[:]:
                    #print '  ***********************'
                    #print ar
                    #sys.exit()
                    op_ar   = []
                    for r1 in ar[1:]:
                        #print '       --> ', r1
                        op, ktup, tttup, taxoid = r1
                        op_ar.append('%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s@@%s'%(taxoid, op,tttup[0], tttup[1], 'CELLFORMULA','', '', '', ktup[0], ktup[1]))
                    op, ktup, tttup, taxoid = ar[0]
                    k   = '%s^%s^%s'%(taxoid, ktup[0], ktup[1])
                    kstr_d.setdefault( tttup[0], {}).setdefault(tttup[1], {})[k]    = '$$'.join(op_ar)
        for ttype, grpd_d in kstr_d.items():
            for grpid, vd in grpd_d.items():
                grpname = grp_map_d.get((ttype, grpid), ttype)
                i_ar.append(('TALLY-'+flag, flag, ttype, grpid, grpname, str(vd), '', 'N'))
        #sys.exit()                              

        
    def computation_classes(self, company_name, company_id, output_path, db_data_d, table_ref_dict):
        #print table_ref_dict 
        #sys.exit()
        import grid_computation
        gobj    = grid_computation.GridCompute()
        gobj.create_post_analysis_data_value_groups(company_name, company_id, output_path,None, None, db_data_d) # compute grouping
        gobj.create_cf_ph_value_groups(company_name, company_id, output_path,None, None, db_data_d) # compute grouping
        #tmp_ar  = ['CF', 'IS', 'BS']
        #tmp_tt  = {}
        table_across_comp_ac = {}
        if 1:
            ks  = db_data_d.keys()
            for i1, ktup1 in enumerate(ks):
                for i2, ktup2 in enumerate(ks[:]):
                    for i3, ktup3 in enumerate(ks[:]):
                        if (i1 == i2): continue
                        if (i2 == i3): continue
                        if (i1 == i3): continue
                        if not ((ktup1[0] == 'IS') and (ktup2[0] == 'BS') and (ktup3[0] == 'CF')): continue # ktup1 is resultant
                        mul_across_group    = gobj.multi_table_computation(company_name, company_id, map(lambda x:str(x), ktup1), map(lambda x:str(x), ktup2), map(lambda x:str(x), ktup3), str(output_path), db_data_d)
                        if mul_across_group:
                           gdict = self.data_preparation_across_gps(mul_across_group, table_ref_dict)
                           if (ktup1, ktup2, ktup3) not in table_across_comp_ac:
                              table_across_comp_ac[(ktup1, ktup2, ktup3)] = [] 
                           table_across_comp_ac[(ktup1, ktup2, ktup3)] = copy.deepcopy(gdict)
            print 'DONE 3 GComp'
            #sys.exit()               
                      

        table_across_comp = {}
        if 1:
            ks  = db_data_d.keys()
            for i, ktup in enumerate(ks):
                for i, ktup1 in enumerate(ks[:]):
                    if not ((ktup[0] == 'IS') and (ktup1[0] == 'CF')): continue # ktup1 is resultant
                    #print 'Across: ', ktup, ktup1
                    #sys.exit()  
                    across_group    = gobj.inter_table_ph_wise_computation_new(company_name, company_id, map(lambda x:str(x), ktup), map(lambda x:str(x), ktup1), str(output_path), db_data_d)
                    if across_group:
                       gdict = self.data_preparation_across_gps(across_group, table_ref_dict)
                       if (ktup, ktup1) not in table_across_comp:
                          table_across_comp[(ktup, ktup1)] = [] 
                       table_across_comp[(ktup, ktup1)] = copy.deepcopy(gdict)
            print 'DONE 2 GComp'


        schedule_across_group = {}
        if 1:
            ks  = db_data_d.keys()
            for i, ktup in enumerate(ks):
                for i, ktup1 in enumerate(ks[:]):
                    if not ((ktup[0] == 'CF') and (ktup1[0] == 'BS')): continue # ktup1 is resultant (CY-PY)BS = CF
                    #print 'Across: ', ktup, ktup1
                    #sys.exit()  
                    phdiff_group   = gobj.multi_table_computation_with_phdiff(company_name, company_id, map(lambda x:str(x), ktup), map(lambda x:str(x), ktup1), output_path, db_data_d)
                    if phdiff_group:
                       gdict = self.data_preparation_across_gps(phdiff_group, table_ref_dict)
                       if (ktup, ktup1) not in schedule_across_group:
                          schedule_across_group[(ktup, ktup1)] = [] 
                       schedule_across_group[(ktup, ktup1)] = copy.deepcopy(gdict)
            print 'DONE Schd GComp'



        schedule_across_group_mul = {}
        if 1:
            ks  = db_data_d.keys()
            for i1, ktup in enumerate(ks):
              for i2, ktup1 in enumerate(ks[:]):
                for i3, ktup2 in enumerate(ks[:]):
                    if not ((ktup[0] == 'CF') and (ktup[1] == 'IS') and (ktup2[0] == 'BS')): continue # ktup2 is resultant (CY-PY)BS = IS + BS
                    #print 'Across: ', ktup, ktup1
                    #sys.exit()  
                    phdiff_group   = gobj.multi_table_computation_with_phdiff_2(company_name, company_id, map(lambda x:str(x), ktup), map(lambda x:str(x), ktup1), map(lambda x:str(x), ktup2),  output_path, db_data_d)
                    if phdiff_group:
                       gdict = self.data_preparation_across_gps(phdiff_group, table_ref_dict)
                       if (ktup, ktup1, ktup2) not in schedule_across_group_mul:
                          schedule_across_group_mul[(ktup, ktup1, ktup2)] = [] 
                       schedule_across_group_mul[(ktup, ktup1, ktup2)] = copy.deepcopy(gdict)
            #print schedule_across_group_mul 
            #print 'DONE Schd GComp JSHJDHDHDHDHD'
            #sys.exit()
 
        pairwise_gp_dict = {}
        if 1:
            group_d = self.get_xml_groups(output_path)
            pairwise_gp_dict = self.data_preparation_group(group_d, table_ref_dict)
            print 'DONE EQ'

 
        pairwise_gp_cf_dict = {}
        if 1:
            group_d_cf = self.get_xml_groups_cf(output_path)
            pairwise_gp_cf_dict = self.data_preparation_group_cf(group_d_cf, table_ref_dict)
            print 'DONE EQ CF'

        return pairwise_gp_dict, pairwise_gp_cf_dict, table_across_comp, table_across_comp_ac  , schedule_across_group , schedule_across_group_mul  


    def data_preparation_across_gps(self, across_group, table_ref_dict):

        gcomp_ddict = {}
        for gp, vs  in across_group.items():
            for ar in vs:
                #print '=========================' 
                #print gp 
                mykey = (gp[0], gp[1])             
                ttype = [ mykey ]
                #print gp, ' == ', table_ref_dict[mykey] 
                line_key = [ ]
                res = []
                for k, g in table_ref_dict[mykey].items():
                    line_key.append(g[0])   
                    res.append(('=', g[1:], k, g[0])) 
                #print 'RES: ', res 
                #print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'   
                for elm in ar:
                    mykey = (elm[0], elm[1])
                    #print ' ==> ', elm, ' ** ', table_ref_dict[mykey] 
                    for k, g in table_ref_dict[mykey].items():
                        line_key.append(g[0])
                        res.append((elm[2], g[1:], k, g[0])) 
                #print '=========================' 
                #print line_key
                #print res
                #sys.exit()  
                line_key = tuple(line_key)
                if line_key not in gcomp_ddict:
                   gcomp_ddict[line_key] = [] 
                gcomp_ddict[line_key].append(res)
        return gcomp_ddict 
        print ' ACROSS MUL'
        for gkey, gval_ar in gcomp_ddict.items():
            print '================'
            print ' gkey: ', gkey
            for ar in gval_ar:
                print '  ***********************'
                for r in ar:
                    print '       --> ', r
        sys.exit()                              
   
           

    def compare_ph_pair(self, ph1, ph2):

        if (ph1[0:2] == ph2[0:2]):
           yy1 = int(ph1[2:])
           yy2 = int(ph2[2:])
           if (yy1 < yy2):
              return [0, 1]
           else:
              return [1, 0]
        else:
           yy1 = int(ph1[2:])
           yy2 = int(ph2[2:])
           if (yy1 < yy2):
              return [0, 1]
           elif (yy1 > yy2):
              return [1, 0]
           else:
              pt1 = ph1[:2]
              pt2 = ph2[:2]
              if (pt1 == 'FY') and (pt2 == 'Q4'):
                 return [1, 0] 
              if (pt1 == 'Q4') and (pt2 == 'FY'):
                 return [0, 1] 
              if (pt1 == 'Q3') and (pt2 == 'Q2'):
                 return [1, 0] 
              if (pt1 == 'Q2') and (pt2 == 'Q3'):
                 return [0, 1] 
              if (pt1 == 'Q2') and (pt2 == 'Q1'):
                 return [1, 0] 
              if (pt1 == 'Q1') and (pt2 == 'Q2'):
                 return [0, 1] 
              if (pt1 == 'H2') and (pt2 == 'FY'):
                 return [0, 1] 
              if (pt1 == 'FY') and (pt2 == 'H2'):
                 return [1, 0] 
              if (pt1 == 'H1') and (pt2 == 'H2'):
                 return [0, 1] 
              if (pt1 == 'H2') and (pt2 == 'H1'):
                 return [1, 0] 
              print 'Unseen Case'
              print pt1, pt2
              sys.exit()


    def data_preparation_group_cf(self, group_d, table_ref_dict):

        pairwise_dict = {}
        gp_keys = group_d.keys()
        for gp_key in gp_keys:
            ttype_ar = [] 
            values = group_d[gp_key]
            for value in values:
                table_id = value.split('@@')[0]
                ref_id = value.split('@@')[1]
                flg = value.split('@@')[2]
                mykey = (table_id, ref_id)
                #print '    >>> ', value 
                for tkey in table_ref_dict[mykey].keys():
                    #print '    ---------> ', tkey, ' == ',  table_ref_dict[mykey][tkey]
                    ttype_ar.append((tkey, table_ref_dict[mykey][tkey], flg))
            #print ttype_ar     
            for i1, elm1 in enumerate(ttype_ar):
                for i2, elm2 in enumerate(ttype_ar):
                    if (i1 == i2): continue
                    if (elm1[0][0] == elm2[0][0]) and (elm1[0][1] == elm2[0][1]) and (elm1[1][0] == elm2[1][0]):
                       continue
                    if (elm1[1][2] != elm2[1][2]):  
                       #print '===========================' 
                       #print elm1
                       #print elm2
                       ph_order = self.compare_ph_pair(elm1[1][2], elm2[1][2])
                       #print 'PH Order: ', ph_order
                       if (ph_order[0] == 0) and (ph_order[1] == 1):
                          temp_elm2 = elm2
                          elm2 = elm1
                          elm1 = temp_elm2 
                          #print ' Swapped: ' 
                          #print elm1
                          #print elm2   
                       ttype_key = (elm1[0], elm2[0])
                       if ttype_key not in pairwise_dict:
                          pairwise_dict[ttype_key] = {}   
                       taxo_id_key = (elm1[1][0], elm2[1][0])
                       res1 = elm1[1][1:]
                       res2 = elm2[1][1:]
                       
                       if taxo_id_key not in pairwise_dict[ttype_key]:
                          pairwise_dict[ttype_key][taxo_id_key] = []
                       pairwise_dict[ttype_key][taxo_id_key].append((res1, res2, elm1[2], elm2[2]))
                           

        return pairwise_dict 
        for k, vs_dict in pairwise_dict.items():
            print '============CF================================='
            print 'Pair: ', k
            for line_pair, vs in vs_dict.items():
                print '   line_pair: ', line_pair
                for v in vs:
                    print '        ++++ ', v 
        sys.exit()
        return pairwise_dict     



        

    def data_preparation_group(self, group_d, table_ref_dict):

        pairwise_dict = {}
        gp_keys = group_d.keys()
        for gp_key in gp_keys:

            #print ' GP_key: ', gp_key 
            #print group_d[gp_key]

            #sys.exit()  

            ttype_ar = [] 
            values = group_d[gp_key]
            for value in values:
                table_id = value.split('@@')[0]
                ref_id = value.split('@@')[1]
                flg = value.split('@@')[2]
                mykey = (table_id, ref_id)
                #print '    >>> ', value 
                for tkey in table_ref_dict[mykey].keys():
                    #print '    ---------> ', tkey, ' == ',  table_ref_dict[mykey][tkey]
                    ttype_ar.append((tkey, table_ref_dict[mykey][tkey], flg))
            #sys.exit() 
            #print ttype_ar     
            for i1, elm1 in enumerate(ttype_ar):
                for i2, elm2 in enumerate(ttype_ar):
                    if (i1 == i2): continue
                    if ((elm1[0][0] == elm2[0][0]) and (elm1[0][1] == elm2[0][1])): continue
                    #print elm1, elm2
                    mykey = (elm1[0], elm2[0])
                    if mykey not in pairwise_dict:
                       pairwise_dict[mykey] = {}
                    line_id1 = elm1[1][0]
                    res1 = elm1[1][1:]
                    line_id2 = elm2[1][0]
                    res2 = elm2[1][1:]
             
                            
                    if (line_id1, line_id2) not in pairwise_dict[mykey]:
                       pairwise_dict[mykey][(line_id1, line_id2)] = [] 
                    pairwise_dict[mykey][(line_id1, line_id2)].append((res1, res2, elm1[2], elm2[2]))
                    #print elm1
                    #print elm2
                    #print pairwise_dict  
                    #sys.exit()  
        
        return pairwise_dict     
        for k, vs_dict in pairwise_dict.items():
            print '============================================='
            print 'Pair: ', k
            for line_pair, vs in vs_dict.items():
                print '   line_pair: ', line_pair
                for v in vs:
                    print '        ++++ ', v 
        sys.exit()





              

    def compute_gcomputation(self, my_data, only_these_table_ids=[]):

        error_table_ids = {} 
        table_data_dict = {}
        for ii, elm_g in enumerate(my_data):      
            data = elm_g[0]
            phs = elm_g[1]
            ijson_c = elm_g[2]
            #print ijson_c, phs
            table_id = ijson_c['table_id']
            project_id  = ijson_c['project_id']
            model_number= ijson_c['model_number']
            deal_id     = ijson_c['deal_id']
            company_name= ijson_c['company_name']
            if only_these_table_ids and (str(table_id) not in only_these_table_ids): continue
            print '======TABLE============' , table_id  
            table_data_dict[table_id] = {}
            error_flg = 0  
            xmlmap  = {}
            for i2, data_elm in enumerate(data):
                for ph in phs:
                    pkey = ph['k']
                    if pkey in data_elm:
                       #clean_value = numbercleanup_obj.get_value_cleanup(elm[1]['v'])
                       #print '     ===> ', i2, data_elm[pkey], pkey, data_elm[pkey]['v']
                       val = data_elm[pkey]['v']
                       if '%' in val: continue 
                       xml_id = data_elm[pkey]['x']
                       xmlmap[(table_id, xml_id)]   = data_elm['x'] 
                       clean_value = numbercleanup_obj.get_value_cleanup(val)
                       if clean_value == '':
                          clean_value = 0
                       else:
                          clean_value = float(clean_value)
                       
                       if pkey not in table_data_dict[table_id]:
                          table_data_dict[table_id][pkey] = []  
                       if val == '': continue 
                       if clean_value == '': 
                          continue
                       #if clean_value == 0: continue
                       #if abs(clean_value) < 1: continue
                       table_data_dict[table_id][pkey].append((clean_value, val, xml_id, i2))
                    else:
                       error_table_ids[table_id] = 1
                       del table_data_dict[table_id]  
                       error_flg = 1  
                       break
                       #table_data_dict[table_id][pkey].append((0, '', ''))
                if error_flg: break
                     
        count = -1        
        for table_id, col_data_dict in table_data_dict.items():
            #if table_id != '129': continue  
            count += 1 
            #print ' Computation for table_id: ', table_id
            #fw = open('/tmp/avinash_test_ampl.txt', 'a')
            #fw.write(str(table_id)+' -- ' + str(count)+'\n')
            #fw.close() 
       
            col_key_data_dict = {}
           
            computation_table_id_wise = {}
            all_pkey_ref_dict = {}
            all_pkey_ref_dict2 = {}
            #print '/tmp/avinash_test_ampl.txt'
            num_ar = []
            col_keys = [] 
            for col_key, vals in col_data_dict.items():
                #print col_key
                o_vals = [] 
                tmp_num_ar =[]
                zero_count = 0
                for val in vals:
                    #print '  == ', val
                    if col_key not in col_key_data_dict:
                       col_key_data_dict[col_key] = {}
                    col_key_data_dict[col_key][val[3]] = val[0]
                       
                    all_pkey_ref_dict[val[2]] = val[3] 

                    if (col_key, val[3]) not in all_pkey_ref_dict2:
                       all_pkey_ref_dict2[(col_key, val[3])] = val[2]
                    #all_pkey_ref_dict2[(col_key, val[3])].append(val[2])
                       
                    tmp_num_ar.append(val[0])
                    if val[0] == 0:
                       zero_count += 1 
                    o_vals.append(val[1])
                o_vals_unq = list(sets.Set(o_vals))
                #print 'zero_count: ', zero_count 
                if zero_count >= 20: continue 
                if (len(o_vals_unq) == 1) and (o_vals_unq[0] == ''): continue # we will not do computation for such columns   
                num_ar.append(tmp_num_ar[:])
                col_keys.append(col_key)

            #print all_pkey_ref_dict 
            #sys.exit()
            computation_results = gcomp.gcomp().process_table(num_ar)     
            col_computation_dict = {}  
            for col_ind, results in computation_results.items(): 
                #print col_ind
                #print results[0]
                pkey = col_keys[col_ind]
                #print pkey 
                col_computation_dict[col_keys[col_ind]] = results[0][:]
                col_key = col_keys[col_ind]
                for res in results[0][:]:
                    #print res
                    indxs = res[1]
                    ref_ar = []
                    ref_ind_ar = []  
                    for indx in indxs:
                        #print ' == ', col_data_dict[col_key][indx]    
                        ref = col_data_dict[col_key][indx][2]
                         
                        ref_ar.append(ref)
                        ref_ind_ar.append(all_pkey_ref_dict[ref])
                    #print 'ref_ar: ', ref_ar

                    #print 'ref_ind_ar: ', ref_ind_ar    
                    if res[2] == True:
                       resultant = ref_ind_ar[0]
                       opers = ref_ind_ar[1:]
                    else:
                       resultant = ref_ind_ar[-1]
                       opers = ref_ind_ar[:-1]
                    #print resultant, ' == ', opers
                    #print 'Sign: ', res[0]
                    #computation_table_id_wise = {}
                    if resultant not in computation_table_id_wise:
                       computation_table_id_wise[resultant] = {}
                    mkey = tuple(ref_ind_ar)
                    if mkey not in computation_table_id_wise[resultant]:
                       computation_table_id_wise[resultant][mkey] = {}

                    #print mkey, opers, res[0]   , res
                    if (len(opers) == len(res[0])):          
                       computation_table_id_wise[resultant][mkey][col_key] = [ opers, res[0] ]

            add_ar = []
            for resultant, mkeydict in computation_table_id_wise.items():
                for cycle, r_col_key_dict in mkeydict.items():
                    for col_key in col_key_data_dict.keys():
                        if col_key not in r_col_key_dict:
                           #print cycle
                           #print resultant
                           #print 'col_key: ', col_key    
                           #print ' TODO'
                           cycle_li = list(cycle)
                           if resultant == cycle_li[0]:
                              cycle_opers = cycle_li[1:]
                           else:
                              cycle_opers = cycle_li[:-1]
                           #print col_key_data_dict[col_key]
                           if resultant not in col_key_data_dict[col_key]: continue 
                           resultant_num = col_key_data_dict[col_key][resultant]
                            
                           #oper_nums = map(lambda x:col_key_data_dict[col_key][x], cycle_opers[:])

                           oper_nums = []
                           for cyc_oper in cycle_opers:
                               if cyc_oper not in col_key_data_dict[col_key]:
                                  oper_nums = []
                                  break
                               else:
                                  oper_nums.append(col_key_data_dict[col_key][cyc_oper])
                           if not oper_nums: continue
                            
                           #print resultant_num
                           #print oper_nums    
                           num_indx = range(0, len(oper_nums))
                           sp_results = gcomp.gcomp().process_specific_computation(oper_nums, num_indx, resultant_num)
                           if sp_results and (len(sp_results) == len(oper_nums)) :
                              sign_ar = []
                              for sp_res in sp_results:
                                  if (sp_res[0] == '+'):
                                     sign_ar.append(0)
                                  else:
                                     sign_ar.append(1)
                              add_ar.append(( resultant, cycle, col_key, cycle_opers, sign_ar))
                              #print 'Add: ', cycle  
            for elm in add_ar:
                resultant = elm[0]
                mkey = elm[1]
                col_key = elm[2]
                opers = elm[3]
                sign_ar = elm[4]
                if len(opers) == len(sign_ar):  
                   computation_table_id_wise[resultant][mkey][col_key] = [ opers, sign_ar ]


            ''' 
            for rid, grpinfo in computation_table_id_wise.items():
                print rid, grpinfo
                for grp, pkinfo in grpinfo.items():
                    print '>>>>>', grp, pkinfo
                    #for pk, signinfo in pkinfo.items():

            sys.exit()  
            ''' 

            
            #print ' Table id: ', table_id  
            #print 'Store: ', computation_table_id_wise 
            #print 'Also store: ', all_pkey_ref_dict2
            #print xmlmap
            final_d = {}
            final_pk_d = {}
            sign_map    = {0:'+',1:'-'}
            for rid, grpinfo in computation_table_id_wise.items():
                #print rid, grpinfo
                for grp, pkinfo in grpinfo.items():
                    #print '>>>>>', grp, pkinfo
                    for pk, signinfo in pkinfo.items():
                        r_xml   =   all_pkey_ref_dict2[(pk, rid)]
                        #print '\tR ', (xmlmap[(table_id, r_xml)], len(grp))
                        op      = []
                        sign_ar = []
                        for oid in grp:
                            if oid == rid:continue
                            #print '\t\t',(xmlmap[(table_id, all_pkey_ref_dict2[(pk,oid)])], sign_map[signinfo[1][len(op)]])
                            sign_ar.append(sign_map[signinfo[1][len(op)]])
                            op.append(xmlmap[(table_id, all_pkey_ref_dict2[(pk,oid)])])
                        final_d.setdefault(xmlmap[(table_id, r_xml)], {}).setdefault('$$'.join(op), {})[pk+':$$:'+'$$'.join(sign_ar)] = 1
            i_ar    = []
            for k, v in final_d.items():
                for op, pks in v.items():
                    i_ar.append((table_id, k, op, ':^^:'.join(pks)))
                
            
            db_path = '/mnt/eMB_db/%s/%s/table_computation_dbs/'%(company_name, model_number)   
            os.system("mkdir -p "+db_path)
            db_path = '/mnt/eMB_db/%s/%s/table_computation_dbs/%s.db'%(company_name, model_number, table_id)   
            os.system('rm -rf %s'%(db_path))
            print db_path
            conn = sqlite3.connect(db_path)
            cur  = conn.cursor()
            
            del_stmt = "drop table if  exists TableFormulaInfo"
            cur.execute(del_stmt)
            crt_stmt = """CREATE TABLE IF NOT EXISTS TableFormulaInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(20), xml_id TEXT, operand TEXT, PH TEXT)"""
            cur.execute(crt_stmt)
            insert_stmt = """INSERT INTO TableFormulaInfo(table_id, xml_id, operand, PH) VALUES(?, ?, ?, ?)"""
            cur.executemany(insert_stmt, i_ar)
            conn.commit()
            conn.close()       
            #sys.exit()

                
    def order_wise_equal(self, ar1, ar2):
        dar1 = sets.Set(ar1)
        dar2 = sets.Set(ar2)

        if (dar1 == dar2) and (ar1[0] == '') and (len(dar1) == 1):
            return 0
  
        for i in range(0, len(ar1)):
            if ar1[i] == ar2[i]:
               continue
            return 0
        return 1    
                 
    def process(self, cmd, ijson, table_ids):
        res = []
        if cmd == 1:
            self.validate(ijson, table_ids)
            #print 'ph_error: ', ph_error
            #print 'vgh_error: ', vgh_error   
        return res

if __name__ == '__main__':
    obj = Validate()
    #print obj.get_reported_phs([('FY2015', 'FY2015'), ('FY2015', 'FY2014'), ('FY2014', 'FY2014'), ('FY2014', 'FY2013'), ('Q42013', 'FY2013')])
    #sys.exit()
    try:
        ijson   = json.loads(sys.argv[1])
        cmd_id  = int(ijson['cmd_id'])
    except:
        cmd_id  = int(sys.argv[1])
        deal_id = sys.argv[2]
        try:
            table_ids = sys.argv[3]
            table_ids = table_ids.split('~')
        except:
            table_ids = [] 
        if '_' in deal_id:
            ijson   = m_obj.read_company_info({"cids":[deal_id.split('_')[1]]})[deal_id]
        else:
            #print deal_id
            deal_id = int(deal_id)
            ijson   = m_obj.read_company_info({"cids":[deal_id]})[deal_id]
        #if len(sys.argv) > 3:
        #    tmpjson = json.loads(sys.argv[3])
        #    ijson.update(tmpjson)
    res = obj.process(cmd_id, ijson, table_ids)
    print res
        
