#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime,sqlite3
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
from utils.meta_info import MetaInfo
import report_year_sort
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import gcom_operator
import tavinash
from collections import defaultdict as DD, OrderedDict as OD
#try:
#    import ParaAPI.Para_Comp_API_Back_New as Para_Comp_API_Back_New 
#    date_obj    = Para_Comp_API_Back_New.Para_Comp_API()
#except:pass

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

from libgcomputation_cpp import compute as gcompute_cpp
from collections import defaultdict as dd
from nong_new_akshay import NGComp

def remap_index(orig_index_dict, old_results):

    for res in old_results:
        res[1] = [orig_index_dict[i] for i in res[1]]

    return old_results


def calculate_comp_results(ph_values):

    ng_obj = NGComp()
    ph, values = ph_values
    values, xml_ids, act_values = values['values'], values['xml_ids'], values['actual_values']

    i1 = 0
    i2 = len(act_values)

    for i in range(0, len(act_values)):
        if (act_values[i] == ''):
           i1 = i+1
        else:
           break
    
    #print len(act_values)  

    if (i1 == i2):
       return (ph, [], [])

    while act_values[i2-1] == '':
          i2 = i2 - 1         

    #print i1, i2
    #sys.exit()  

    truncate_values = []
    if (i1 < i2):
       truncate_values = values[i1:i2]

    orig_index_dict = {}
    valid_values = []
    new_index = 0

    '''
    for orig_index, (x, v) in enumerate(zip(xml_ids, values)):
        if x.strip():
            orig_index_dict[new_index] = orig_index
            valid_values.append(v)
            new_index += 1
    #'''
    valid_values = values[:]

    gcomp_results = []
    nong_results = []

    '''
    if not any([x.strip() for x in xml_ids]):
        return (ph, gcomp_results, nong_results)
    '''


    '''
    if not any([x.strip() for x in xml_ids]):
        return (ph, gcomp_results, nong_results)
    '''

    pattern_file = '/mnt/eMB_db/GComp_txt_files/1/ExtnGCPatterns.txt'
    formula_file = '/mnt/eMB_db/GComp_txt_files/1/Extnadd_formula.txt'

    # Process only if there are any valid xmlids/values
    if truncate_values:
        gcompute_cpp(truncate_values, pattern_file, formula_file, gcomp_results)
        #nong_results = ng_obj.compute_nong_eqs(valid_values)

     

    
    #print valid_values
    #print gcomp_results # [[0, 0], [0, 1, 2], False]
    '''
          if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                    else:
                       # last one is resultant
                       res_operands = result[0][:-1]
    '''

    add_res = []
    # Result multiplier
    for res in gcomp_results:
        if len(res[1]) >= 4:
           #print res
           #print truncate_values
           mid_indx = res[1][1:-1]
           if res[2] == False:
              # Last one is resultant
              #print 'Last one is the resultant 1:' 
              mid_opers = res[0][1:] 
              rem_opr = res[0][0] 
           elif res[2] == True:
              # First one is resultant
              #print 'First one is the resultant :-1'  
              mid_opers = res[0][:-1] 
              rem_opr = res[0][-1] 
                 
           #print mid_indx   
           val_ar = map(lambda x:truncate_values[x], mid_indx)
           #print val_ar    
           #print mid_opers 
           non_zero_indx = []
           i = -1
           for x in mid_indx:
               
               i = i + 1
               if (truncate_values[x] == 0) or (truncate_values[x] == 0.0):
                   continue
               else:
                   non_zero_indx.append((x, mid_opers[i]))
           if non_zero_indx:   
              #print '======================='
              #print res
              #print non_zero_indx  
              #new_result = []
              n_indx = map(lambda x:x[0], non_zero_indx[:])
              n_oprs = map(lambda x:x[1], non_zero_indx[:])
               
              new_opr = [ res[1][0] ] + n_indx[:] + [ res[1][-1] ]
              #print 'new_opr: ', new_opr  
              if res[2] == False:
                 new_sgn = [ rem_opr ] + n_oprs[:]
              else: 
                 new_sgn = n_oprs[:]  + [ rem_opr ]
              new_res = [ new_sgn, new_opr, res[2] ]
              #print new_res   
              add_res.append(new_res)
    gcomp_results = gcomp_results + add_res[:]
               
    #sys.exit()   


    sign_dict = {}
    for res in gcomp_results:
        mykey = tuple(res[1] + [res[2]])
        if mykey not in sign_dict:
           sign_dict[mykey] = [] 
        sign_dict[mykey].append(res[0])

    new_gcomp_results = [] 
    for k, vs in sign_dict.items():
        operands = list(k)[:-1]
        parity = k[-1]
        if len(vs) > 1:
           #print truncate_values 
           min_ar = []
           vs.sort()
           for v in vs:
               #print '++++++++++++++++++++++++++'  
               if parity == False:
                  # Last one is resultant
                  resultant = truncate_values[operands[-1]]
                  opers = map(lambda x:truncate_values[x], operands[:-1])

               else:
                  # first one is the resultant
                  resultant = truncate_values[operands[0]]
                  opers = map(lambda x:truncate_values[x], operands[1:])
               if 1:        
                  #print v
                  #print opers  
                  #print resultant 
                  comp_value = 0
                  for i, v_elm in enumerate(v):
                      if v_elm == 0:
                         comp_value = comp_value + opers[i]
                      else:
                         comp_value = comp_value - opers[i]
                  #print 'computed_value: ', comp_value 
                  diff = abs(resultant - comp_value) 
                  #print 'diff: ', diff               
                  min_ar.append((diff, v))
           min_ar.sort()
           best_v = min_ar[0][1]
           new_gcomp_results.append([ best_v, operands, parity]) 
        else:
           new_gcomp_results.append([ vs[0], operands, parity]) 

    gcomp_results = new_gcomp_results[:] 
    #print 'Final'
    #sys.exit()  
    new_results = []
    for res in gcomp_results:
        sgn = res[0]
        #print res
        #continue 
        #for sgn1 in res:
        #    print sgn1
          
        #sys.exit() 
        opr = map(lambda x:x+i1, res[1][:])
        s = res[2]
        new_results.append((sgn, opr, s))  

        #print '======================', i1
        #print res
        #print (sgn, opr, s)

    #sys.exit() 
    #print 'Old: ', gcomp_results
    #print 'New: ', new_results
    gcomp_results = new_results[:]     
    #sys.exit() 

    # If there is change in index
    #if len(valid_values) != len(values):
    #    gcomp_results = remap_index(orig_index_dict, gcomp_results)
        #nong_results = remap_index(orig_index_dict, nong_results)

    return (ph, gcomp_results, nong_results)


class PYAPI(MetaInfo):
    def __init__(self, pypath="/root/databuilder_train_ui/tenkTraining/Data_Builder_Training/pysrc"):
        MetaInfo.__init__(self, pypath)
        self.doc_path          = self.config['doc_path'] 
        self.bkup_name       = '' #_user_10_dec'
        self.db_path         =  '/mnt/yMB_db/%s_%s'+ self.bkup_name+'/'
        self.taxo_path       = '/mnt/eMB_db/%s/%s/'
        self.output_path    = self.config['table_path']
        self.bbox_path      = '/var/www/html/fill_table/'
        self.value_type_map  = {
                            'Percentage'        : '%',
                            'Percentage-1'      : '%-1',
                            'Percentage-2'      : '%-2',
                            'Percentage-3'      : '%-3',
                            'Percentage-4'      : '%-4',
                            'Percentage-5'      : '%-5',
                            'Percentage-6'      : '%-6',
                            'Percentage-7'      : '%-7',
                            'Percentage-8'      : '%-8',
                            'Percentage-9'      : '%-9',
                            'Percentage-10'      : '%-10',
                            'Percentage-11'      : '%-11',
                            'Percentage-12'      : '%-12',
                        }
        self.order_d     = {
                        'IS'     : 1,
                        'BS'     : 2,
                        'CF'     : 3,
                        'RBS'    : 4,
                        'RBG'    : 5,
                        'STD'    : 6,
                        'LTD'    : 7,
                        'OLSE'   : 8,
                        'CLSE'   : 9,
                        'IEXP'   : 10,
                        'IINC'   : 11,
                        'COGSS'  : 12,
                        'FE'     : 13,
                        'FI'     : 14,
                        'RNTL'   : 15,
                        'COGS'   : 16,
                        'DEBT'   : 17,
                        'COS'    : 18,
                        'LTP'    : 19,
                        'STP'    : 20,
                        'RnD'    : 21,
                        'PRV'    : 22,
                        'PSH'    : 23,
                        'GWIL'   : 24,
                        'CEQ'    : 25,
                        'EBITDA' : 26,
                        'EBIT'   : 27,
                        }
        self.scale_map_d = {
            '1':'One', 'TH':'Thousand', 'TENTHOUSAND':'TenThousand', 'Mn':'Million', 'Bn':'Billion', 'KILO':'Thousand','Ton':'Million','Tn':'Million', 'Mn/Ton':"Million",
            'TH/KILO':"Thousand",
        }
        self.scale_map_rev_d = {
            'One':'1', 'Thousand':"TH", 'TenThousand':'TENTHOUSAND', 'Million':'Mn', 'Billion':'Mn'
        }
        self.report_map_d    = {
                            'Q1'    : {'Q1':1},
                            'FY'    : {'FY':1,'Q4':1},
                            'Q2'    : {'Q2':1,'H1':1},
                            'Q3'    : {'Q3':1, 'M9':1},
                            'Q4'    : {'Q4':1},
                            'H1'    : {'H1':1, 'Q2':1},
                            'H2'    : {'H2':1, 'Q4':1,'FY':1},
                            'M9'    : {'M9':1, 'Q3':1},
                            }
        self.ignore_phs = {
                            '219':{
                                    '79-72':{
                                            'Q22017':1,
                                            'Q32017':1
                                        }
                                },
                            }
        self.gen_users      = {'captain':1
                                
                                }
        pass
    def create_seq(self, ijson):
        import create_table_seq
        obj = create_table_seq.TableSeq()
        return obj.create_seq(ijson)

    def create_seq_across(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        if ijson.get('vids', {}) and isinstance(ijson.get('vids', {}), list):
            ijson['vids']   = ijson['vids'][0]
        else:
            ijson['vids']   = {}
        #m_tables, rev_m_tables, doc_m_d = self.get_main_table_info(company_name, model_number)
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        def form_ph_csv_taxo(ph, c_ph):
            return ph
        def form_ph_csv_mt(ph, c_ph):
            return c_ph
        db_name = ''
        if ijson.get('taxo_flg', '') == 1:
            form_ph_csv = form_ph_csv_taxo
            db_name = 'taxo_data_builder'
        else:
            form_ph_csv = form_ph_csv_mt
            db_name = 'mt_data_builder'
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            try:
                year    = int(ph[-4:])
            except:continue
            if ph and start_year < year:
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])
        main_header     = table_type_m['main_header']
        #m_path          = self.taxo_path%(company_name, model_number)
        #path            = m_path+'/TAXO_RESULT/'
        #env1        = lmdb.open(path, max_dbs=27)
        #db_name1    = env1.open_db('other')
        #txn         = env1.begin(db=db_name1)

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()
        table_ph_d  = {}
        all_ph_d    = {}
        table_type  = str(i_table_type)
        f_taxo_arr  = [] #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        table_ids   = {}
        g_ar        = []
        table_col_phs   = {}
        consider_tables = {}
        taxo_complete_map = {}
        vgh_id_d, vgh_id_d_all, docinfo_d   = {}, {}, {}
        if 'HGHGROUP' in ijson.get('grpid', ''):
            table_type  = ijson['grpid']
        if not taxo_d:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)

            ph_der_stmt = 'select ph, formula from ph_derivation where table_type = "%s" and group_id = "%s" and formula_type="TH"'%(table_type, ijson.get('grpid', ''))
            ph_der_rows = []
            try:
                cur.execute(ph_der_stmt)
                ph_der_rows = cur.fetchall()
            except:pass
            ph_der_map_dict = {}
            for ph_der_row in ph_der_rows:
                ph, formula = ph_der_row
                ph = int(ph)
                formula = str(formula)
                ph_der_map_dict[ph] = formula 

            if not ijson.get('vids', {}):
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            else:
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(ijson['vids'].keys()))
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            g_vids  = ijson.get('vids', {})
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type    = rr
                th_flg = ''

                if th_flg == None:
                    th_flg = ''
                doc_id      = str(doc_id)
                if doc_id not in doc_d:continue
                table_id    = str(table_id)
                if table_id not in m_tables or (vgh_group and 'COPY_' not in vgh_group):continue
                tk          = table_id+'_'+self.get_quid(xml_id)
                c_id        = txn_m.get('TXMLID_MAP_'+tk)
                if not c_id:
                    tk   = self.get_quid(table_id+'_'+xml_id)
                    c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                #if str(deal_id) == '214' and str(table_id) == '1184':
                #    print [taxo_id, xml_id, c_id]
                c   = int(c_id.split('_')[2])
                if g_vids.get(vgh_text, {}) and table_id+'-'+str(c) not in g_vids.get(vgh_text, {}):continue
                key     = table_id+'_'+self.get_quid(xml_id)
                ph_map  = txn.get('PH_MAP_'+str(key))
                if ph_map:
                    tperiod_type, tperiod, tcurrency, tscale, tvalue_type    = ph_map.split('^')
                else:
                    tperiod_type, tperiod, tcurrency, tscale, tvalue_type   = '', '', '', '', ''
                if not period:
                    period      = tperiod
                if not period_type:
                    period_type      = tperiod_type
                if not scale:
                    scale      = tscale
                if not currency:
                    currency     = tcurrency
                if  not value_type:
                    value_type      = tvalue_type
                if period and period_type:
                    ph  = period_type+period
                gv_xml  = c_id
                consider_tables[table_id] = 1
                table_col_phs.setdefault((table_id, c), {})[ph]   = table_col_phs.setdefault((table_id, c), {}).get(ph, 0) +1
                table_ids[table_id]   = 1
                comp    = ''
                #if gcom == 'Y' or ngcom == 'Y':
                #    comp    = 'Y'
                vgh_id_d.setdefault(vgh_text, {})[(table_id, c_id, row_id, doc_id)]        = 1
                vgh_id_d_all[vgh_text]  = 1
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                docinfo_d.setdefault(doc_id, {})[(table_id, c_id, vgh_text)]   = 1
                taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'rid':row_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows, 'th_flg':th_flg, 'ds':'N'})['ks'].append((table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id, xml_id, row_id, period, period_type, scale, currency, value_type, vgh_text))
                #if ijson.get('taxo_flg', '') == 1 and vgh_text:
                #    tk   = self.get_quid(table_id+'_'+vgh_text)
                #    c_id        = txn_m.get('XMLID_MAP_'+tk)
                #    taxo_d[taxo_id]['ks'].append((table_id, c_id, 'Parent', ph_label,gcom, ngcom, doc_id, vgh_text, row_id, period, period_type, scale, currency, value_type))
                if vgh_group == 'N':
                    taxo_d[taxo_id]['l_change'][xml_id]  = 1
            conn.close()

        #print table_ids
        #print taxo_d
        ph_csv_error_map_dict, table_gv_info = {}, {}
        if ijson.get('taxo_flg', '') == 1:
            ph_csv_error_map_dict, table_gv_info = self.validate_taxo_ph_scv_info(company_name, model_number, company_id, txn_m, txn, table_ids, taxo_d)

        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                row_d   = {}
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        #r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
                        #print [table_id, r+tr, c_id, t]
                        row_d.setdefault(r+tr, []).append((c, t, x))
                r_ld[table_id]  = {}
                for r, c_ar in row_d.items():
                    c_ar.sort()
                    txt = []
                    xml = []
                    for tr in c_ar:
                        txt.append(tr[1])
                        xml.append(tr[2])
                    bbox        = self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml))
                    r_ld[table_id][r]  = (' '.join(txt), ':@:'.join(xml), bbox)
        rc_ld    = {}
        if ijson.get('vids', []) and 0:
            for table_id in table_ids.keys():
                k       = 'VGH_'+str(table_id)
                ids     = txn_m.get(k)
                if ids:
                    col_d   = {}
                    ids     = ids.split('#')
                    for c_id in ids:
                        r       = int(c_id.split('_')[1])
                        c       = int(c_id.split('_')[2])
                        cs      = int(txn_m.get('colspan_'+c_id))
                        for tr in range(cs): 
                            col_d.setdefault(c+tr, {})[r]   = c_id
                    for c, rows in col_d.items():
                        rs= rows.keys()
                        rs.sort(reverse=True)
                        for r in rs:
                            c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                            if not c_id or not txn_m.get('TEXT_'+c_id):continue
                            x       = txn_m.get('XMLID_'+c_id)
                            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                            t       = ' '.join(t.split())
                            rc_ld.setdefault(table_id, {})[c]   = (x, t, self.get_bbox_frm_xml(txn1, table_id, x))
                            break
                        
        f_ar        = []
        done_d      = {}
        #for dd in f_taxo_arr:
        tmptable_col_phs    = {}
        not_found_ph    = {}
        if 0:
            for k, v in table_col_phs.items():
                phs = v.keys()
                phs.sort(key=lambda x:v[x], reverse=True)
                if len(phs) == 1 and phs[0] == '':
                    table_id    = k[0]
                    tk       = 'VGH_'+str(table_id)
                    ids     = txn_m.get(tk)
                    if ids:
                        col_d   = {}
                        ids     = ids.split('#')
                        for c_id in ids:
                            r       = int(c_id.split('_')[1])
                            c       = int(c_id.split('_')[2])
                            cs      = int(txn_m.get('colspan_'+c_id))
                            for tr in range(cs): 
                                col_d.setdefault(c+tr, {})[r]   = c_id
                        for c, rows in col_d.items():
                            if c != k[1]:continue
                            rs= rows.keys()
                            rs.sort(reverse=True)
                            for r in rs:
                                c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                                if not c_id or not txn_m.get('TEXT_'+c_id):continue
                                x       = txn_m.get('XMLID_'+c_id)
                                t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                                t       = ' '.join(t.split())
                                tmptable_col_phs[k]   = t
                                not_found_ph.setdefault(k[0], {})[k[1]] = 1
                                break
                    pass
                if k not in tmptable_col_phs:
                    tmptable_col_phs[k] = phs[0]
        row_ids = taxo_d.keys()
        row_ids.sort(key=lambda x:len(taxo_d[x]['ks']), reverse=True)
        #for row_id, dd in taxo_d.items():
        if ijson.get('gen_output', '') == 'Y':
            tmprows = []
            done_d  = {}
            for row_id in row_ids:
                dd      = taxo_d[row_id]
                ks      = dd['ks']
                tmp_arr = []
                for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id, trid, period, period_type, scale, currency, value_type, vgh_text in ks:
                    if (table_id, xml_id) in done_d:continue
                    tmp_arr.append((table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id, trid))
                    done_d[(table_id, xml_id)]=1
                if not tmp_arr:continue
                tmprows.append(row_id)
            row_ids = tmprows[:]
                    
        
        ph_formula_d                            = {}
        if ijson.get('NO_FORM', '') != 'Y' and ijson.get('taxo_flg', '') != 1:
            ph_formula_d                            = self.read_ph_user_formula(ijson, '')
            if ijson.get('grpid', ''):
                ph_formula_d                            = self.read_ph_user_formula(ijson, ijson.get('grpid', ''), ph_formula_d)
        table_xml_d     = {}
        taxo_id_dict    = {}
        fs_d            = {}
        for row_id in row_ids:
            dd      = taxo_d[row_id]
            db_comp_flg = 0
            ks      = dd['ks']
            th_flg  = ph_der_map_dict.get(row_id, '')
            taxos   = dd['t_l'].split(' / ')[0]
            row     = {'t_id':row_id} #'t_l':taxo}
            f_dup   = ''
            label_d = {}
            label_r_d = {}
            f_phs   = []
            if len(ks) < len(table_ids.keys()):
                db_comp_flg = 1
            ks.sort(key=lambda x:tuple(map(lambda x1:int(x1), x[1].split('_'))))
            ph_ind  = {}
            scale_d = {}
            for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id, trid, period, period_type, scale, currency, value_type, vgh_text in ks:
                #tk   = self.get_quid(table_id+'_'+xml_id)
                #c_id        = txn_m.get('XMLID_MAP_'+tk)
                #if not c_id:continue
                row.setdefault('tids', {})[table_id]    = 1
                table_id    = str(table_id)
                c_id        = str(c_id)
                r           = int(c_id.split('_')[1])
                c           = int(c_id.split('_')[2])
                x           = txn_m.get('XMLID_'+c_id)
                t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                if t: 
                    row['da']='Y'
                if tlabel:
                    tlabel  = self.convert_html_entity(tlabel)
                
                #print [row_id, table_id, c_id, t, ph, tlabel, xml_id]
                if (deal_id == '221' and table_type=='RBG') or (deal_id == '44' and table_type == 'OS') or 'HGHGROUP' in table_type:
                    c_ph        = ph
                    c_tlabel    = ''
                else:
                    c_ph        = str(c)
                    c_tlabel    = ''
                c_ph    = form_ph_csv(ph, c_ph)
                pc_error_flg = 0
                if c_ph != 'Parent':
                    pc_error_flg = ph_csv_error_map_dict.get((table_id, x), 0)
                gv_ph_csv_info  = table_gv_info.get((table_id, x), [])
                bbox, x_c       = self.get_bbox_frm_xml(txn1, table_id, x, 'Y')
                #f_ph            =  tmptable_col_phs[(table_id, c)]
                if(table_id, c_ph) in ph_ind:
                    c_tlabel    = str(ph_ind[(table_id, c_ph)])
                    ph_ind[(table_id, c_ph)]    += 1
                else:
                    ph_ind[(table_id, c_ph)]    = 1
                c_key           = table_id+'-'+c_ph+'-'+c_tlabel
                if t: 
                    scale_d.setdefault(scale, {})[(c_key, trid)] = 1
                tmptable_col_phs.setdefault(c_key, {})
                tmptable_col_phs[c_key][ph] = tmptable_col_phs[c_key].get(ph, 0)+1
                #print table_id, c_id
                txts, xml_ar, bbox1    = r_ld[table_id].get(r, ('', '', ''))
                txts        = self.convert_html_entity(txts)
                grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                if ijson.get('taxo_flg', '') != 1 or ph != 'Parent' :
                    label_r_d[grm_txts] = 1
                    label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':bbox1, 'x':xml_ar, 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':bbox1, 'x':xml_ar, 'd':doc_id, 't':table_id}

                    if xml_id not in dd.get('l_change', {}):
                        label_d[grm_txts]['s']  = 'Y'
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):
                    continue
                row[c_key]      = {'v':t, 'x':x, 'bbox':bbox, 'd':doc_id, 't':table_id, 'r':r, 'rid':trid, 'phcsv':{'p':period, 'pt':period_type, 's':scale, 'c':currency, 'vt':value_type}, 'pce':pc_error_flg, 'vt':gv_ph_csv_info, 'ph_f':c_tlabel}
                
                
                table_xml_d.setdefault(table_id, {})[(int(x_c.split('_')[1]), int(x_c.split('_')[0].strip('x')))]   = 1
                #if c_id in done_d:
                #    f_dup   = 'Y'
                #    row[table_id+'-'+c_ph+'-'+c_tlabel]['m']    = 'Y'
                if gcom == 'Y':
                    row[c_key]['g_f']    = 'Y'
                    row['f']    = 'Y'
                if ngcom == 'Y':
                    row[c_key]['ng_f']    = 'Y'
                    row['f']    = 'Y'
                done_d[c_id]    = 1
                if (deal_id == '221' and table_type=='RBG') or (deal_id == '44' and table_type == 'OS') or ('HGHGROUP' in table_type):
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, ph)
                else:
                    if ijson.get('taxo_flg', '') == 1:
                        table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (0 if not c_tlabel else int(c_tlabel), ph)
                    else:
                        table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = ((c, 0 if not c_tlabel else int(c_tlabel)), ph)
                col_txt = rc_ld.get(table_id, {}).get(c, ())
                if col_txt:
                    txts        = self.convert_html_entity(col_txt[1])
                    grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                    label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id}
                    if xml_id not in dd.get('l_change', {}):
                        label_d[grm_txts]['s']  = 'Y'
                if vgh_text and ijson.get('taxo_flg', '') == 1:
                    x   = vgh_text
                    tk   = self.get_quid(table_id+'_'+x)
                    c_id        = txn_m.get('XMLID_MAP_'+tk)
                    if c_id:
                        c_id        = str(c_id)
                        r           = int(c_id.split('_')[1])
                        c           = int(c_id.split('_')[2])
                        x           = txn_m.get('XMLID_'+c_id)
                        t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                        c_ph        = 'Parent'
                        c_key           = table_id+'-'+c_ph+'-'+c_tlabel
                        bbox            = self.get_bbox_frm_xml(txn1, table_id, x)
                        row[c_key]  = {'v':t, 'x':x, 'bbox':bbox, 'd':doc_id, 't':table_id, 'r':r, 'rid':trid, 'phcsv':{'p':period, 'pt':period_type, 's':scale, 'c':currency, 'vt':value_type}}
                        table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (0 if not c_tlabel else int(c_tlabel), 'Parent')
                        ph  = 'Parent'
                        tmptable_col_phs.setdefault(c_key, {})
                        tmptable_col_phs[c_key][ph] = tmptable_col_phs[c_key].get(ph, 0)+1
                    
                        
            if len(label_d.keys()) > 1:
                row['lchange']  = 'Y'
                row['ldata']    = label_d.values()
            lble    = label_r_d.keys()
            lble.sort(key=lambda x:len(x), reverse=True)
            row['taxo']  = dd['t_l']
            if dd.get('u_label', ''):
                row['t_l']  = dd.get('u_label', '')
            else:
                row['t_l']  = label_d[lble[0]]['txt']
            xml_ar  = label_d[ lble[0]]['x'].split(':@:')
            if ijson.get('taxo_flg', '') != 1 and xml_ar and xml_ar[0] and (not row.get('parent_txt', '')):
                    table_id    =  label_d[ lble[0]]['t']
                    doc_id      = label_d[ lble[0]]['d']
                    p_key   = txn.get('TRPLET_HGH_PINFO_'+table_id+'_'+self.get_quid(xml_ar[0]))
                    if p_key:
                        tmp_xar  = []
                        t_ar    = []
                        for pkinfo in p_key.split(':!:'):
                            pxml, ptext = pkinfo.split('^')
                            tmp_xar.append(pxml)
                            t_ar.append(binascii.a2b_hex(ptext))
                        pxml    = ':@:'.join(tmp_xar)
                        row['parent_txt']   = ' '.join(t_ar) # {'txt':' '.join(t_ar), 'bbox':self.get_bbox_frm_xml(txn1, table_id, pxml), 'x':pxml, 'd':doc_id, 't':table_id}
                        if not row['t_l']:
                            row['t_l']  = row['parent_txt']+' (Null)'

            if len(scale_d.keys()) > 1:
                scale_grps  = scale_d.keys()
                scale_grps.sort()
                scale_grps  = tuple(scale_grps)
                fs_d.setdefault(scale_grps, {})[row_id] = scale_d
                row['scale_error']  = scale_grps
                pass

            row['x']    = label_d[ lble[0]]['x']
            row['bbox'] = label_d[ lble[0]]['bbox']
            row['t']    = label_d[ lble[0]]['t']
            row['d']    = label_d[ lble[0]]['d']
            row['l']    = len(ks)
            row['fd']   = f_dup
            row['order']   = dd['order_id']
            row['rid']     = dd['rid']
            row['th_flg']  = th_flg
            row['dbf']     = db_comp_flg
            if dd.get('m_rows', ''):
                row['merge']   = 'Y'
            if dd.get('missing', ''):
                row['missing']  = dd['missing']
            taxo_id_dict[str(row_id)]    = row
            row['t_l']  = row['t_l'].strip('-/+')
            row['t_l']  = row['t_l'].strip('+/-')
            row['t_l']  = row['t_l'].strip('=')
            f_ar.append(row)
        if ijson.get('NO_FORM', '') == 'Y':
            fs_d    = {}
        s_ar    = []
        for stup, taxo_d  in fs_d.items():
            k   = '_'.join(stup)
            scale_d = {}
            for t_id, sc_d in taxo_d.items():
                for scale, rid_d in sc_d.items():
                    scale_d.setdefault(scale, {'t':0, 'a':0, 'rids':{}})
                    for (c_key, rid) in rid_d.keys():
                        scale_d[scale]['rids'].setdefault(t_id, {})[rid] = ph_formula_d.get(('SCALE', c_key), 'N')
                        scale_d[scale]['t'] += 1
                        if ph_formula_d.get(('SCALE', c_key), 'N') == 'Y':
                            scale_d[scale]['a'] += 1
            sgrp    = []
            for k, v in scale_d.items():
                dd  = {'s':k, 't':v['t'],'a':v['a'], 'sids':v['rids']}
                dd['done']  = 'N'
                if v['a'] == v['t']:
                    dd['done']  = 'Y'
                elif v['a'] < v['t']:
                    dd['done']  = 'P'
                sgrp.append(dd)
            dd  = {'info':sgrp}
            s_ar.append(dd)
            
                    

        dphs = report_year_sort.year_sort(dphs.keys())
        dphs.reverse()
        #alphs = report_year_sort.year_sort(all_ph_d.keys())
        #alphs.reverse()
        table_ids   = table_ph_d.keys()
        #try:
        #table_ids.sort(key=lambda x:(dphs.index(doc_d[x[0]][0]), x[1]))
        table_ids.sort(key=lambda x:(dphs.index(doc_d[x[0]][0]), sorted(table_xml_d[x[1]].keys())[0]))
        #except:
        #    table_ids.sort(key=lambda x:(x[0], x[1]))
        #    pass
        phs = []
        t_ar    = []
        for table_id in table_ids:
            #print table_id, sorted(table_xml_d[table_id[1]].keys())
            t_ar.append({'t':table_id[1], 'l':len(table_ph_d[table_id].keys()), 'd':table_id[0], 'dt':doc_d.get(table_id[0], '')})
            all_phs = table_ph_d[table_id].keys()
            all_phs.sort(key=lambda x:table_ph_d[table_id][x][0])
            for ph, tlabel in all_phs:
                #tph = table_ph_d[table_id][(ph, tlabel)][1]
                c_key   = table_id[1]+'-'+ph+'-'+tlabel
                t_phs   = tmptable_col_phs[c_key].keys()
                t_phs.sort(key=lambda x:tmptable_col_phs[c_key][x], reverse=True)
                tph     = t_phs[0] 
                d_tph   = doc_d[table_id[0]][0]
                phs.append({'k':table_id[1]+'-'+ph+'-'+tlabel, 'n':tph, 'g':table_id[0]+'-'+table_id[1]+' ( '+'_'.join(doc_d.get(table_id[0], []))+' )', 'ph':ph, 'dph':(d_tph[:-4], int(d_tph[-4:]))})
        not_done = list(sets.Set(m_tables.keys()) - sets.Set(consider_tables.keys()))
        t_rem_ar    = []
        for t in not_done:
            if table_type != m_tables[t]:continue
            table_id    = (doc_m_d[t], t)
            if table_id[0] not in doc_d:continue
            t_rem_ar.append({'t':table_id[1], 'd':table_id[0], 'dt':doc_d.get(table_id[0], ['',''])})
        
        # f_ar.sort(key=lambda x:(0 if x.get('missing', '') != 'Y' else 1, x.get(phs[0]['k'], {'r':999999999})['r'], 99999 - x['l']))
        f_ar.sort(key=lambda x:(x['order'], x['rid']))
        if  ijson.get('taxo_flg', '') != 1  and ijson.get('NO_FORM', '') != 'Y':
            for row in f_ar:
                t_row_f     = ph_formula_d.get(('F', str(row['t_id'])), ())
                op_d    = {}
                ttype   = ''
                if not t_row_f:
                    t_row_f     = ph_formula_d.get(('SYS F', str(row['t_id'])), ())
                    if t_row_f:
                        ttype   = 'SYSTEM'
                        op_d    = ph_formula_d.get(("OP", t_row_f[0]), {})
                else:
                    ttype   = 'USER'
                if t_row_f:
                    #print [row['t_l'], ttype]
                    self.read_sys_form(t_row_f, row, phs, taxo_id_dict, table_type, op_d)
        res = [{'message':'done', 'data':f_ar, 'phs':phs, 'table_ar':t_ar, 'table_ar_rem':t_rem_ar, 'total':len(rev_m_tables.get(table_type, {}).keys()), 'nph':not_found_ph, 'main_header':main_header,'s_arr':s_ar}] #, 'm_tables':m_tables, 'm':rev_m_tables}]
        if not ijson.get('vids', []) and 'HGHGROUP' not in table_type:
            if ijson.get('taxo_flg', '') != 1 and ijson.get('NO_FORM', '') != 'Y':
                res[0]['g_ar']  = self.read_all_vgh_groups(table_type, company_name, model_number,vgh_id_d, vgh_id_d_all, docinfo_d)
                res[0]['load_grps']  = 'Y'
                
        else:
            res[0]['FROM_GRP']  = 'Y'
        if ijson.get('taxo_flg', '') == 1:
            res[0]['taxo_flg']  = 1
            
        return res

    def read_sys_form(self, t_row_f, rr, f_phs, taxo_id_dict, table_type, op_d):
        for ph in f_phs[:]:
            if ph['k'] in rr:
                k   = ph['k']
                v   = rr[ph['k']]
                if v.get('f_col', []):continue
                #print '\tIN', k, v
                
                f_ar    = []
                f   = 1
                op_inds = op_d.get((str(rr['t_id']), ph['k']), [])
                op_ind  = 0
                for ft in t_row_f[1]:
                    if ft['type'] == 'v':
                        dd  = {}
                        dd['clean_value']   = ft['txid']
                    else:
                        if ft['txid'] not in taxo_id_dict:# and table_type == ft['t_type']:
                            dd  = {}
                            f   = 0
                            dd['clean_value']   = ''
                            dd['description']   = 'Not Exists'
                            dd['ph']            = ph['n']
                            dd['taxo_id']       = ft['txid']
                            dd['tt']            = ft['t_type']
                            dd['operator']          = ft['op']
                            if dd['operator'] != '=':
                                if op_inds:
                                    dd['operator']          = op_inds[op_ind]
                                op_ind  += 1
                            f_ar.append(dd)
                            continue
                            #break
                        dd  = copy.deepcopy(taxo_id_dict.get(ft['txid'], {}).get(k, {}))
                        dd['clean_value']   = dd.get('v', '')
                        dd['description']   = taxo_id_dict.get(ft['txid'], {'t_l':''})['t_l']
                        dd['ph']            = ph['n']
                        dd['taxo_id']       = ft['txid']
                        dd['tt']            = ft['t_type']
                    dd['operator']          = ft['op']
                    if dd['operator'] != '=':
                        if op_inds:
                            dd['operator']          = op_inds[op_ind]
                        op_ind  += 1
                        
                    dd['k']      = k
                    if ft['op'] == '=':
                        dd['R']    = 'Y'
                        dd['rid']    = t_row_f[0].split('-')[-1]
                    f_ar.append(dd)
                #if f == 0:
                #    break
                v['f_col']  = [f_ar]
                v['fv']  = 'Y'
                rr['f']  = 'Y'

    def read_all_vgh_groups(self, table_type, company_name, model_number,vgh_id_d, vgh_id_d_all, docinfo_d):
        group_d     = {}
        revgroup_d  = {}
        d_group_d     = {}
        d_revgroup_d  = {}
        ijson   = {'company_name':company_name, 'model_number':model_number}
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        grp_doc_map_d   = {}
        rev_doc_map_d   = {}
        sql = "select vgh_group_id, doc_group_id, group_txt from vgh_doc_map where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        doc_vgh_map = {}
        for rr in res:
            vgh_group_id, doc_group_id, group_txt   = rr
            doc_vgh_map[(vgh_group_id, doc_group_id)]   = group_txt

        sql = "select group_id, table_type, group_txt from vgh_group_info where table_type like '%s'"%('HGHGROUP-'+table_type+'%')
        #if company_name == 'ADIENT PLC':
        #    print sql
        try:
            cur.execute(sql)
            res1 = cur.fetchall()
        except:
            res1 = []
        hgh_grp = []
        for rr in res1:
            group_id, ttable_type, group_txt = rr
            hgh_grp.append({'n':group_txt, 'vids':[{}], 'grpid':ttable_type, 'doc_ids':{}, 'doc_grpid':'', 'hgh':"Y"})
        #if company_name == 'ADIENT PLC':
        #    sys.exit()
        sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            #print rr
            group_id, table_type, group_txt = rr
            grp_info[str(group_id)]   = group_txt
        

        sql         = "select row_id, vgh_id, group_txt, table_str, doc_vgh from vgh_group_map where table_type='%s'"%(table_type)
        res         = []
        try:
            cur.execute(sql)
            res         = cur.fetchall()
        except:pass
        for rr in res:
            row_id, vgh_id, group_txt, table_str, doc_vgh   = rr
            if doc_vgh == 'DOC':
                d_group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                d_revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)
                pass
            else:
                group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)

        vghs = vgh_id_d.keys()
        vghs.sort(key=lambda x:len( vgh_id_d[x].keys()), reverse=True)
        f_arr       = []
        max_tlen    = 0
        grp_doc_ids = {}
        for ii, vgh_id in enumerate(vghs):
            vids        = vgh_id_d[vgh_id].keys()
            #vgh_id, doc_grp = vgh_id
            tmparr  = []
            t_d     = {}
            tc_d     = {}
            doc_id_d    = {}
            for rtup in vids:
                table_id, c_id, row_id, doc_id  = rtup
                c                       = c_id.split('_')[2]
                t_d.setdefault((table_id+'-'+c, doc_id), {})[(row_id, c_id)] = 1
                tc_d[table_id+'-'+c]    = doc_id
                doc_id_d.setdefault(doc_id, {})[table_id+'-'+c]    = 1
            if vgh_id in revgroup_d:
                dd_g         = revgroup_d.get(vgh_id, {}).keys()
                for grp_id in dd_g:
                    tmpd            = {}
                    doc_tcd = {}
                    if not revgroup_d[vgh_id][grp_id][1]:
                        tmpd    = tc_d
                    else:
                        for tsr in revgroup_d[vgh_id][grp_id][1].split('#'):
                            if tsr not in tc_d:continue
                            tmpd[tsr]   = tc_d[tsr]
                    for tkey, doc_k in tc_d.items():
                        doc_tcd.setdefault(doc_k, {})[tkey]     = 1
                    for doc_id in doc_id_d.keys():
                        grp_doc_ids.setdefault(grp_id, {}).setdefault(d_revgroup_d.get(doc_id, {'':''}).keys()[0], {}).setdefault(vgh_id, {}).update(doc_tcd.get(doc_id, {}))
                    group_d[grp_id][vgh_id] = (ii, revgroup_d[vgh_id][grp_id][0], tmpd, revgroup_d[vgh_id][grp_id][1])
        g_ar    = []
        for k, v in group_d.items():
            #print [k]
            doc_grp = grp_doc_ids.get(k, {})
            for tk, tvids in doc_grp.items():
                tmp_grp_name    = ''
                if tk in grp_info:
                    tmp_grp_name    = ' - '+grp_info[tk]
                tmp_grp_name    = grp_info.get(k, k)+tmp_grp_name
                tmp_grp_name    = doc_vgh_map.get((k, tk), tmp_grp_name)    
                tmp_grpid       = k
                if tk:
                    tmp_grpid   = k+'-'+tk
                dd  = {'n':tmp_grp_name, 'vids':[{}], 'grpid':tmp_grpid, 'doc_ids':{}, 'doc_grpid':tk}
                table_ids   = {}
                for vid, i in v.items():
                    if vid not in tvids:continue
                    if len(i) <3:
                        continue
                    table_ids[vid]  = tvids[vid]
                dd['vids'] = [table_ids]
                g_ar.append(dd)
        g_ar    += hgh_grp
        return g_ar


    def get_computation(self, key, txn_m, txn, r, p_xml_d, pxml, tks):
        for gflg in ['G', 'NG']:
            gcom        = txn.get('COM_'+gflg+'_ROWMAP_'+key)
            if gcom:
                for eq in gcom.split('|'):
                    formula, eq_str = eq.split(':$$:')
                    f_taxo_arr[ii].setdefault('gcom', {})[c_id] = formula
                    for xml in eq_str.split('^'):
                        k   = self.get_quid(table_id+'_'+xml)
                        tmpcid  = txn_m.get('XMLID_MAP_'+k)
                        if tmpcid.split('_')[1] == r and tmpcid in tks:
                            p_xml_d[xml]  = pxml

    def cal_bobox_data_new(self, ijson):
        project_id  = ijson['project_id']
        deal_id     = ijson['deal_id']
        company_id  = str(project_id) + '_'+str(deal_id)
        #self.output_path = '/var/www/html/fundamentals_intf/output/'
        lmdb_folder      = os.path.join(self.output_path, company_id, 'doc_page_adj_cords')
        doc_page_dict    = {}
        if os.path.exists(lmdb_folder):
            env = lmdb.open(lmdb_folder)
            with env.begin() as txn:
                cursor = txn.cursor()
                for doc_id, res_str in cursor:
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
                        doc_page_dict[doc_id] = page_dict
        return [{'message':'done', 'data':doc_page_dict}]

    def read_doc_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)


        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_ar  = []
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:
                doc_ar.append({'d':line[0].strip()})
                continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            doc_id  = line[0]
            doc_ar.append({'d':doc_id, 'ph':ph, 'type':line[2]})
        doc_ar.sort(key=lambda x:int(x['d']))
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
        map_dic = {
                    'IS'    :0,
                    'BS'    :1,
                    'CF'    :2,
                    'RBS'    :3,
                    'RBG'    :4,
                    }
        
        ks = rev_m_tables.keys()
        ks.sort(key=lambda x:map_dic.get(x, 9999))
        list_mt = []
        for k in ks:
            dic = {'l': k, 'k': k, 'f': False}
            list_mt.append(dic)
        proj_info = self.getProjectId_details_comp(ijson)
        return [{'message':'done', 'data':doc_ar, 'mt_list': list_mt, 'proj_info':proj_info}]


    def add_new_taxonomy(self, ijson):
        import create_table_seq
        obj = create_table_seq.TableSeq()
        disableprint()
        obj.insert_new_taxonomy(ijson)
        enableprint()
        return self.create_seq_across(ijson)
    def update_label(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        table_type      = ijson['table_type']

        label           = ijson['lbl']
        t_id            = ijson['t_id']
        sql             = 'update mt_data_builder set user_taxonomy="%s" where taxo_id=%s and table_type="%s"'%(label, t_id, table_type)
        cur.execute(sql)
        conn.commit()
        res             = [{'message':'done'}]
        return res 

    def sh_merge(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select distinct(user_taxonomy) from mt_data_builder where table_type='%s'"%(table_type)
        cur.execute(sql)
        res             = cur.fetchall()
        e_taxo  = {}
        for rr in res:
            e_taxo[rr[0].lower()]  = 1
        f_ar    = []
        for t_ids, user_taxonomy in ijson['merge_taxo']:   
            ijson['t_ids']  = t_ids 
            ijson['target_taxo']  = user_taxonomy
            t_ids            = map(lambda x:int(x), ijson['t_ids'])
            f_taxo_id       = t_ids[0]
            taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
            table_ids   = {}
            g_ar        = []
            table_col_phs   = {}
            consider_tables = {}
            f_ph        = {}
            lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
            env         = lmdb.open(lmdb_path1)
            txn_m       = env.begin()

            sql             = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id from mt_data_builder where table_type='%s' and isvisible='Y' and taxo_id in (%s)"%(table_type, ', '.join(map(lambda x:str(x), ijson['t_ids'])))
            cur.execute(sql)
            res             = cur.fetchall()
            new_row_id  = {}
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id    = rr
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                gv_xml  = c_id
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    f_ph.setdefault((table_id, ph, ph_label), {})[(gv_xml, ph, ph_label)]    = 1
                elif ijson.get('taxo_flg', '') == 1:
                    f_ph.setdefault(table_id, {})[(taxo_id, xml_id, row_id)] = 1
                else:
                    f_ph.setdefault((table_id, c_id.split('_')[2], ''), {}).setdefault((gv_xml, ph, ph_label), {})[(taxo_id, xml_id)] = 1
                new_row_id[str(row_id)] = 1
            if ijson.get('taxo_flg', '') == 1 and str(deal_id) != '43':
                lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
                env         = lmdb.open(lmdb_path1)
                txn         = env.begin()
                res = self.check_ph_overlap(f_ph, txn_m, txn)
                if res:
                    return res
            #print f_ph
            error_tables    = []
            if ijson.get('taxo_flg', '') != 1:
                for ph, cnt in f_ph.items():
                    if len(cnt.keys()) > 1:
                         error_tables   += cnt.keys()
            if error_tables:
                tmpar   = []
                for rr in error_tables:
                    gv_xml, ph, ph_label    = rr
                    table_id    = gv_xml.split('_')[0]
                    tmpar.append(table_id+'-'+gv_xml.split('_')[2]+'-'+ph_label)
                error_tables    = tmpar[:]
                    
            
            if error_tables:
                error_tables    = list(sets.Set(error_tables))
                res             = [{'message':'PH Overlap', 'data':error_tables, 'taxo_d':[]}]
                return res
            f_ar.append((ijson.get('target_taxo', ''), f_taxo_id, '_'.join(map(lambda x:str(x), ijson['t_ids'])), ','.join(map(lambda x:str(x), ijson['t_ids']))))
        for (target_taxo, f_taxo_id, tids, rids) in f_ar:
            if target_taxo.lower() in e_taxo:
                tid = e_taxo[target_taxo.lower()]
                target_taxo += '-N-'+str(tid)
                e_taxo[target_taxo.lower()] = tid+1
            else:
                e_taxo[target_taxo.lower()] = 1
            sql     = "update mt_data_builder set user_taxonomy='%s', taxo_id=%s, m_rows='%s' where taxo_id in (%s)"%(target_taxo, f_taxo_id, tids, rids)
            #print sql
            cur.execute(sql)
        conn.commit()
        conn.close()
        return self.create_seq_across(ijson)

    def merge_taxo(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = map(lambda x:int(x), ijson['t_ids'])
        f_taxo_id       = t_ids[0]
        taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        table_ids   = {}
        g_ar        = []
        table_col_phs   = {}
        consider_tables = {}
        f_ph        = {}
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        sql             = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id from mt_data_builder where table_type='%s' and isvisible='Y' and taxo_id in (%s)"%(table_type, ', '.join(map(lambda x:str(x), ijson['t_ids'])))
        cur.execute(sql)
        res             = cur.fetchall()
        new_row_id  = {}
        for rr in res:
            row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id    = rr
            doc_id      = str(doc_id)
            table_id    = str(table_id)
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            gv_xml  = c_id
            if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                f_ph.setdefault((table_id, ph, ph_label), {})[(gv_xml, ph, ph_label)]    = 1
            elif ijson.get('taxo_flg', '') == 1:
                f_ph.setdefault(table_id, {})[(taxo_id, xml_id, row_id)] = 1
            else:
                f_ph.setdefault((table_id, c_id.split('_')[2], ''), {}).setdefault((gv_xml, ph, ph_label), {})[(taxo_id, xml_id)] = 1
            new_row_id[str(row_id)] = 1
        if ijson.get('taxo_flg', '') == 1 and str(deal_id) != '43':
            lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
            env         = lmdb.open(lmdb_path1)
            txn         = env.begin()
            res = self.check_ph_overlap(f_ph, txn_m, txn)
            if res:
                return res
        #print f_ph
        error_tables    = []
        if ijson.get('taxo_flg', '') != 1:
            for ph, cnt in f_ph.items():
                if len(cnt.keys()) > 1:
                     error_tables   += cnt.keys()
        if error_tables:
            tmpar   = []
            for rr in error_tables:
                gv_xml, ph, ph_label    = rr
                table_id    = gv_xml.split('_')[0]
                tmpar.append(table_id+'-'+gv_xml.split('_')[2]+'-'+ph_label)
            error_tables    = tmpar[:]
                
        
        if error_tables:
            error_tables    = list(sets.Set(error_tables))
            res             = [{'message':'PH Overlap', 'data':error_tables, 'taxo_d':[]}]
            return res
        if ijson.get('target_taxo', ''):
            sql     = "update mt_data_builder set user_taxonomy='%s', taxo_id=%s, m_rows='%s' where row_id in (%s)"%(ijson.get('target_taxo', ''), f_taxo_id, '_'.join(map(lambda x:str(x), ijson['t_ids'])), ', '.join(new_row_id.keys()))
        elif ijson.get("taxo_flg", '') == 1:
            res = [{"message":"Error Not allowed without target taxo"}]
            conn.close()
            return res
        else:
            sql     = "update mt_data_builder set taxo_id=%s, m_rows='%s' where row_id in (%s)"%(f_taxo_id, '_'.join(map(lambda x:str(x), ijson['t_ids'])), ', '.join(new_row_id.keys()))
        cur.execute(sql)
        conn.commit()
        d_ids   = {}
        for t in t_ids[1:]:
            d_ids[t]    = 1
        
        res = [{'message':'done', 's':f_taxo_id, 'd_ids':d_ids, 'merge':'New', 't_taxo':ijson.get('target_taxo', '')}]
        return res


    def check_ph_overlap(self, f_ph, txn_m, txn):
        t_d     = {}
        table_d = {}
        for ph, cnt in f_ph.items():
            if len(cnt.keys()) > 1:
                t_d[ph]   = 1
                for taxo_id, xml_id, rid in cnt.keys():
                    table_d.setdefault(ph, {})[xml_id]   = (taxo_id, rid)
        t_rc_d  = {}
        for table_id in t_d.keys():
            #if table_id != '1215':continue
            k       = 'HGH_'+table_id
            ids     = txn_m.get(k)
            r_ld    = {}
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    if x in table_d.get(table_id, {}):
                        rs      = int(txn_m.get('rowspan_'+c_id))
                        for tr in range(rs):
                            r_ld.setdefault(r+tr, {})[x]  = 1
            k   = 'GV_'+table_id
            ids = txn_m.get(k)
            if not ids:continue
            ids         = ids.split('#')
            for c_id in ids:
                r       = int(c_id.split('_')[1])
                c       = int(c_id.split('_')[2])
                #print c_id
                #print 'YESY'
                x       = txn_m.get('XMLID_'+c_id)
                key     = table_id+'_'+self.get_quid(x)
                x_txts    = r_ld.get(r, {}).keys()
                for x_p in x_txts:
                    t_rc_d.setdefault((table_id, x_p), {})[x]   = 1
        error_d = {}
        for table_id, px_d in table_d.items():
            e_flg   = 0
            prev_phs    = ''
            for px in px_d.keys():
                phs = []
                xd  = t_rc_d.get((table_id, px), {})
                #print (table_id, px, xd)
                for xml_id in xd.keys():
                    key     = table_id+'_'+self.get_quid(xml_id)
                    ph_map  = txn.get('PH_MAP_'+str(key))
                    if ph_map:
                        tperiod_type, tperiod, tcurrency, tscale, tvalue_type    = ph_map.split('^')
                    else:
                        tperiod_type, tperiod, tcurrency, tscale, tvalue_type   = '', '', '', '', ''
                    if tperiod_type and tperiod:
                        phs.append(tperiod_type+tperiod)
                if prev_phs != '' and (sets.Set(prev_phs).intersection(sets.Set(phs)) or not phs):
                    e_flg   = 1
                    #break
                #print phs
                prev_phs    = phs
                if e_flg == 1:
                    for px, (tid, rid) in px_d.items():
                        error_d[rid] = 1
        if error_d:
            res             = [{'message':'PH Overlap', 'taxo_d':error_d.keys(), 'data':[]}]
            return res
        return []

    def form_row(self, table_type, txn_m, txn, table_ids, taxo_d):
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                row_d   = {}
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        #r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
                        row_d.setdefault(r+tr, []).append((c, t, x))
                r_ld[table_id]  = {}
                for r, c_ar in row_d.items():
                    c_ar.sort()
                    txt = []
                    xml = []
                    for tr in c_ar:
                        txt.append(tr[1])
                        xml.append(tr[2])
                    bbox        = self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml))
                    r_ld[table_id][r]  = (' '.join(txt), ':@:'.join(xml), bbox)
        rc_ld    = {}
        if ijson.get('vids', []):
            for table_id in table_ids.keys():
                k       = 'VGH_'+str(table_id)
                ids     = txn_m.get(k)
                if ids:
                    col_d   = {}
                    ids     = ids.split('#')
                    for c_id in ids:
                        r       = int(c_id.split('_')[1])
                        c       = int(c_id.split('_')[2])
                        cs      = int(txn_m.get('colspan_'+c_id))
                        for tr in range(cs): 
                            col_d.setdefault(c+tr, {})[r]   = c_id
                    for c, rows in col_d.items():
                        rs= rows.keys()
                        rs.sort(reverse=True)
                        for r in rs:
                            c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                            if not c_id or not txn_m.get('TEXT_'+c_id):continue
                            x       = txn_m.get('XMLID_'+c_id)
                            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                            t       = ' '.join(t.split())
                            rc_ld.setdefault(table_id, {})[c]   = (x, t, self.get_bbox_frm_xml(txn1, table_id, x))
                            break
                        
        f_ar        = []
        done_d      = {}
        #for dd in f_taxo_arr:
        tmptable_col_phs    = {}
        for k, v in table_col_phs.items():
            phs = v.keys()
            phs.sort(key=lambda x:v[x], reverse=True)
            tmptable_col_phs[k] = phs[0]
        row_ids = taxo_d.keys()
        row_ids.sort(key=lambda x:len(taxo_d[x]['ks']), reverse=True)
        #for row_id, dd in taxo_d.items():
        if ijson.get('gen_output', '') == 'Y':
            tmprows = []
            done_d  = {}
            for row_id in row_ids:
                dd      = taxo_d[row_id]
                ks      = dd['ks']
                tmp_arr = []
                for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id in ks:
                    if (table_id, xml_id) in done_d:continue
                    tmp_arr.append((table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id))
                    done_d[(table_id, xml_id)]=1
                if not tmp_arr:continue
                tmprows.append(row_id)
            row_ids = tmprows[:]
                    
        
        for row_id in row_ids:
            dd      = taxo_d[row_id]
            ks      = dd['ks']
            taxos   = dd['t_l'].split(' / ')[0]
            row     = {'t_id':row_id} #'t_l':taxo}
            f_dup   = ''
            label_d = {}
            label_r_d = {}
            for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id in ks:
                #tk   = self.get_quid(table_id+'_'+xml_id)
                #c_id        = txn_m.get('XMLID_MAP_'+tk)
                #if not c_id:continue
                row.setdefault('tids', {})[table_id]    = 1
                table_id    = str(table_id)
                c_id        = str(c_id)
                r           = int(c_id.split('_')[1])
                c           = int(c_id.split('_')[2])
                x           = txn_m.get('XMLID_'+c_id)
                t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                if tlabel:
                    tlabel  = self.convert_html_entity(tlabel)
                #print [taxo, table_id, c_id, t, ph, tlabel]
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    c_ph        = ph
                    c_tlabel    = tlabel
                else:
                    c_ph        = str(c)
                    c_tlabel    = ''
                row[table_id+'-'+c_ph+'-'+c_tlabel]    = {'v':t, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 't':table_id, 'r':r}
                #if c_id in done_d:
                #    f_dup   = 'Y'
                #    row[table_id+'-'+c_ph+'-'+c_tlabel]['m']    = 'Y'
                if gcom == 'Y':
                    row[table_id+'-'+c_ph+'-'+c_tlabel]['g_f']    = 'Y'
                    row['f']    = 'Y'
                if ngcom == 'Y':
                    row[table_id+'-'+c_ph+'-'+c_tlabel]['ng_f']    = 'Y'
                    row['f']    = 'Y'
                done_d[c_id]    = 1
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, ph)
                else:
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, tmptable_col_phs[(table_id, c)])
                #print table_id, c_id
                txts, xml_ar, bbox    = r_ld[table_id].get(r, ('', '', ''))
                txts        = self.convert_html_entity(txts)
                grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                label_r_d[grm_txts] = 1
                label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id}

                if xml_id not in dd.get('l_change', {}):
                    label_d[grm_txts]['s']  = 'Y'
                col_txt = rc_ld.get(table_id, {}).get(c, ())
                if col_txt:
                    txts        = self.convert_html_entity(col_txt[1])
                    grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                    label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id}
                    if xml_id not in dd.get('l_change', {}):
                        label_d[grm_txts]['s']  = 'Y'
            if len(label_d.keys()) > 1:
                row['lchange']  = 'Y'
                row['ldata']    = label_d.values()
            lble    = label_r_d.keys()
            lble.sort(key=lambda x:len(x), reverse=True)
            row['taxo']  = dd['t_l']
            if dd.get('u_label', ''):
                row['t_l']  = dd.get('u_label', '')
            else:
                row['t_l']  = label_d[lble[0]]['txt']
            xml_ar  = label_d[ lble[0]]['x'].split(':@:')
            if xml_ar and xml_ar[0]:
                    table_id    =  label_d[ lble[0]]['t']
                    doc_id      = label_d[ lble[0]]['d']
                    p_key   = txn.get('TRPLET_HGH_PINFO_'+table_id+'_'+self.get_quid(xml_ar[0]))
                    if p_key:
                        tmp_xar  = []
                        t_ar    = []
                        for pkinfo in p_key.split(':!:'):
                            pxml, ptext = pkinfo.split('^')
                            tmp_xar.append(pxml)
                            t_ar.append(binascii.a2b_hex(ptext))
                        pxml    = ':@:'.join(tmp_xar)
                        row['parent_txt']   = ' '.join(t_ar) # {'txt':' '.join(t_ar), 'bbox':self.get_bbox_frm_xml(txn1, table_id, pxml), 'x':pxml, 'd':doc_id, 't':table_id}

            row['x']    = label_d[ lble[0]]['x']
            row['bbox'] = label_d[ lble[0]]['bbox']
            row['t']    = label_d[ lble[0]]['t']
            row['d'] = label_d[ lble[0]]['d']
            row['l']    = len(ks)
            row['fd']   = f_dup
            row['order']   = dd['order_id']
            row['rid']   = dd['rid']
            if dd.get('m_rows', ''):
                row['merge']   = 'Y'
            if dd.get('missing', ''):
                row['missing']  = dd['missing']
            f_ar.append(row)
        return f_ar



    def merge_label(self, ijson):
        if ijson.get('st_ids', []):
            return self.merge_table_to_row(ijson)
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        if 1:#str(deal_id) == '214':
            return self.merge_taxo(ijson)
            


        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids           = ijson['t_ids']
        table_type      = ijson['table_type']
        sql             = "select taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id from mt_data_builder where table_type='%s' and taxo_id in (%s) and isvisible='Y'"%(table_type , ', '.join(map(lambda x:str(x), t_ids)))
        cur.execute(sql)
        res             = cur.fetchall()
        ks_d            = {}
        taxo_ar         = []
        uset_taxo       = {}
        order_ids       = {}
        table_ids       = {}
        f_ph            = {}
        for rr in res:
            taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id    = rr
            doc_id      = str(doc_id)
            table_id    = str(table_id)
            table_ids[table_id]       = 1
            vgh_text     = '' if not vgh_text else vgh_text
            vgh_group    = '' if not vgh_group else vgh_group
            order_ids[order_id] = 1
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            gv_xml  = c_id
            if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                f_ph.setdefault((table_id, ph, ph_label), {})[(gv_xml, ph, ph_label)]    = 1
            else:
                f_ph.setdefault((table_id, c_id.split('_')[2], ''), {}).setdefault((gv_xml, ph, ph_label), {})[(taxo_id, xml_id)] = 1
            ks_d[(table_id, gv_xml, ph, ph_label, gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id)]            = 1
            taxo_ar     += taxonomy.split(' / ')
            if user_taxonomy:
                uset_taxo[user_taxonomy]    = 1
        #print f_ph
        error_tables    = []
        for ph, cnt in f_ph.items():
            if len(cnt.keys()) > 1:
                 error_tables   += cnt.keys()
        if ((deal_id == '221' and table_type=='RBG') or ((deal_id == '44' and table_type == 'OS'))) and error_tables:
            tmpar   = []
            for rr in error_tables:
                gv_xml, ph, ph_label    = rr
                table_id    = gv_xml.split('_')[0]
                tmpar.append(table_id+'-'+ph+'-'+ph_label)
            error_tables    = tmpar[:]
        elif error_tables:
            tmpar   = []
            for rr in error_tables:
                gv_xml, ph, ph_label    = rr
                table_id    = gv_xml.split('_')[0]
                tmpar.append(table_id+'-'+gv_xml.split('_')[2]+'-'+ph_label)
            error_tables    = tmpar[:]
                
        
        if error_tables:
            error_tables    = list(sets.Set(error_tables))
            res             = [{'message':'PH Overlap', 'data':error_tables}]
            return res
        if not order_ids:
            sql = "select max(order_id) from mt_data_builder"
            cur.execute(sql)
            r   = cur.fetchone()
            if r:
                order_ids[r[0]] = 1 
        order_ids    = order_ids.keys()
        order_ids.sort()
        order_id    = order_ids[0]
        #t_ids.sort()
        sql             = "update mt_data_builder set isvisible='N' where table_type='%s' and taxo_id in (%s)"%(table_type , ', '.join(map(lambda x:str(x), t_ids)))
        cur.execute(sql)


        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id)).replace('\xc2\xa0', '')
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))

        i_ar    = []
        taxo_ar = list(sets.Set(taxo_ar))
        taxo_ar.sort()
        taxo    = ' / '.join(taxo_ar)
        try:
            taxo    = taxo.decode('utf-8')
        except:pass
        uset_taxo   = uset_taxo.keys()
        uset_taxo.sort(key=lambda x:len(x))
        if uset_taxo:
            uset_taxo   = uset_taxo[-1]
        else:
            uset_taxo   = ''

        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            doc_id  = line[0]
            doc_d[doc_id]   = (ph, line[2])
            dphs[ph]        = 1
        dtime   = str(datetime.datetime.now()).split('.')[0]
        row     = {'t_id':t_ids[0]} #'t_l':taxo}
        label_d = {}
        for (table_id, c_id, ph, tlabel, gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id) in ks_d.keys():
            c_id    = str(c_id)
            try:
                tlabel    = tlabel.decode('utf-8')
            except:pass
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id    = txn_m.get('XMLID_MAP_'+tk)
            i_ar.append((order_id, table_type, taxo, '', '', table_id, c_id, ph, tlabel, gcom, ngcom, 'Y', vgh_text, vgh_group, '_'.join(map(lambda x:str(x), t_ids)), doc_id, xml_id, ijson['user'], dtime))
            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
            x       = txn_m.get('XMLID_'+c_id)
            r       = int(c_id.split('_')[1])
            c       = int(c_id.split('_')[2])
            if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                c_ph        = ph
                c_tlabel    = tlabel
            else:
                c_ph    = str(c)
                c_tlabel    = ''
            row[table_id+'-'+c_ph+'-'+c_tlabel]    = {'v':t, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 't':table_id, 'r':r}
            row.setdefault('tids', {})[table_id]    = 1
            if gcom == 'Y':
                row[table_id+'-'+c_ph+'-'+c_tlabel]['g_f']    = 'Y'
            if ngcom == 'Y':
                row[table_id+'-'+c_ph+'-'+c_tlabel]['ng_f']    = 'Y'
            txts    = r_ld.get(table_id, {}).get(r, [])
            txts.sort(key=lambda x:x[0])
            txts_ar = []
            xml_ar  = []
            for x in txts:
                txts_ar.append(x[1])
                pkey    = table_id+'_'+str(r)+'_'+str(x[0])
                t_x = txn_m.get('XMLID_'+pkey)
                if t_x:
                    xml_ar.append(t_x)
            txts    = ' '.join(txts_ar)
            grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
            label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml_ar)), 'x':':@:'.join(xml_ar), 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml_ar)), 'x':':@:'.join(xml_ar), 'd':doc_id, 't':table_id}
            if vgh_group != 'N':
                label_d[grm_txts]['s']  = 'Y'
        if len(label_d.keys()) > 1:
            row['lchange']  = 'Y'
            row['ldata']    = label_d.values()
        lble    = label_d.keys()
        lble.sort(key=lambda x:len(x), reverse=True)
        if uset_taxo:
            row['t_l']  = uset_taxo
        else:
            #row['t_l']  = self.convert_html_entity(lble[0])
            row['t_l']  = self.convert_html_entity(label_d[lble[0]]['txt'])
        row['x']    = label_d[ lble[0]]['x']
        row['bbox'] = label_d[ lble[0]]['bbox']
        row['t']    = label_d[ lble[0]]['t']
        row['d'] = label_d[ lble[0]]['d']
        row['merge'] = 'Y'
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            row['t_id'] = g_id
            i_ar    = map(lambda x:(g_id, )+x, i_ar)
            cur.executemany("insert into mt_data_builder(taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label, gcom, ngcom, isvisible, vgh_text, vgh_group, m_rows, doc_id, xml_id, user_name, datetime)values(?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?, ?,?,?, ?)", i_ar)
            conn.commit()
        conn.close()
        dd  = {}
        dd[t_ids[0]]    = row
        del_ids = {}
        for t in t_ids[1:]:
            del_ids[t]  = 1
        res             = [{'message':'done', 'data':dd, 'd_ids':del_ids}]
        return res

    def read_all_vgh_texts(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        i_table_type    = ijson['table_type']
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()


        table_type  = str(i_table_type)
        group_d     = {}
        revgroup_d  = {}

        d_group_d     = {}
        d_revgroup_d  = {}

        vgh_id_d    = {}
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        #sql         = "select vgh_id, doc_id, table_id, group_txt from doc_group_map where table_type='%s'"%(table_type)
        #try:
        #    cur.execute(sql)
        #    res = cur.fetchall()
        #except:
        #    res = []
        grp_doc_map_d   = {}
        rev_doc_map_d   = {}
        #for rr in res:
        #    vgh_id, doc_id, table_id, group_txt = rr
        #    grp_doc_map_d.setdefault(group_txt, {})[table_id]   = doc_id #.setdefault(doc_id, {})[table_id]    = 1
        #    rev_doc_map_d.setdefault((doc_id, table_id), {})[group_txt] = 1
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        sql = "select vgh_group_id, doc_group_id, group_txt from vgh_doc_map where table_type='%s'"%(table_type)
        doc_vgh_map = {}
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        for rr in res:
            vgh_group_id, doc_group_id, group_txt   = rr
            doc_vgh_map[(vgh_group_id, doc_group_id)]   = group_txt

        sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[str(group_id)]   = group_txt

        sql         = "select row_id, vgh_id, group_txt, table_str, doc_vgh from vgh_group_map where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        for rr in res:
            row_id, vgh_id, group_txt, table_str, doc_vgh   = rr
            if doc_vgh == 'DOC':
                d_group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                d_revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)
                pass
            else:
                group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)

        sql         = "select row_id, table_id, c_id, vgh_text, doc_id, xml_id from mt_data_builder where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        docinfo_d   = {}
        vgh_id_d_all    = {}
        for rr in res:
            row_id, table_id, c_id, vgh_text, doc_id, xml_id    = rr
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            doc_id  = str(doc_id)
            if vgh_text != None and str(doc_id) in doc_d and str(table_id) in m_tables:
                vgh_id_d.setdefault(vgh_text, {})[(table_id, c_id, row_id, doc_id)]        = 1
                vgh_id_d_all[vgh_text]  = 1
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                docinfo_d.setdefault(doc_id, {})[(table_id, c_id, vgh_text)]   = 1

        
        sql         = "select vgh, vgh_id from vgh_info where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        vgh_d       = {}
        for rr in res:
            vgh_text, vgh_id    = rr
            if vgh_id in vgh_id_d_all:
                try:
                    vgh_text    = binascii.a2b_hex(vgh_text)
                except:pass
                vgh_d[vgh_id]       = self.convert_html_entity(vgh_text)

        dghs = docinfo_d.keys()
        dghs.sort(key=lambda x:len( docinfo_d[x].keys()), reverse=True)
        df_arr       = []
        dmax_tlen    = 0
        for ii, dgh_id in enumerate(dghs[:]):
            dgh         = docinfo_d[dgh_id]
            dd          = {}
            dd['txt']   = '_'.join(doc_d[dgh_id])
            dd['v_id']  = dgh_id
            vids        = docinfo_d[dgh_id].keys()
            vids.sort()
            doc_id      = dgh_id
            table_id, c_id, row_id        = vids[0]
            x           = txn_m.get('COLXMLID_'+str(table_id)+'_'+str(c_id.split('_')[2]))
            if not x:
                x       = txn_m.get('XMLID_'+str(c_id))
            dd['x']     = x
            dd['d']     = doc_id
            dd['t']     = table_id
            dd['bbox']  = self.get_bbox_frm_xml(txn1, table_id, x)
            tmparr  = []
            t_d     = {}
            vgh_ids = {}
            tc_d    = {}
            for rtup in vids:
                table_id, c_id, row_id  = rtup
                c                       = c_id.split('_')[2]
                t_d.setdefault(table_id+'-'+c, {})[(row_id, c_id)] = 1
                vgh_ids[row_id] = 1
                tc_d[table_id+'-'+c]    = doc_id
            dmax_tlen    = max(dmax_tlen, len(t_d.keys()))
            for table_id, rows in t_d.items():
                rows    = rows.keys()
                c_id    = rows[0][1]
                x       = txn_m.get('COLXMLID_'+str(table_id)+'_'+str(c_id.split('_')[2]))
                if not x:
                    x       = txn_m.get('XMLID_'+str(c_id))
                rd      = {'t':table_id, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 'rid':map(lambda x:x[0], rows)}
                tmparr.append(rd)
                #if (doc_id, table_id) in rev_doc_map_d:
                #    dd['g']         = rev_doc_map_d[(doc_id, table_id)].keys()
                #    for grp_id in dd['g']:
                #        dgroup_d[grp_id][(doc_id, table_id)] = ii
            dd['table_str']     = ', '.join(t_d.keys())
            dd['table_ids']     = tmparr  
            dd['i']             = ii
            dd['vgh_ids']       = vgh_ids

            if doc_id in d_revgroup_d:
                dd['g']         = d_revgroup_d.get(doc_id, {}).keys()
                for grp_id in dd['g']:
                    tmpd            = {}
                    if not d_revgroup_d[doc_id][grp_id][1]:
                        tmpd    = tc_d
                    else:
                        for tsr in d_revgroup_d[doc_id][grp_id][1].split('#'):
                            if tsr not in tc_d:continue
                            tmpd[tsr]   = tc_d[tsr]
                    d_group_d[grp_id][doc_id] = (ii, d_revgroup_d[doc_id][grp_id][0], tmpd, d_revgroup_d[doc_id][grp_id][1])
            df_arr.append(dd)

        vghs = vgh_id_d.keys()
        vghs.sort(key=lambda x:len( vgh_id_d[x].keys()), reverse=True)
        f_arr       = []
        max_tlen    = 0
        grp_doc_ids = {}
        for ii, vgh_id in enumerate(vghs):
            vids        = vgh_id_d[vgh_id].keys()
            #vgh_id, doc_grp = vgh_id
            vgh         = vgh_d[vgh_id]
            dd          = {}
            dd['txt']   = vgh
            dd['v_id']  = vgh_id
            vids.sort()
            table_id, c_id, row_id, doc_id        = vids[0]
            x           = txn_m.get('COLXMLID_'+str(table_id)+'_'+str(c_id.split('_')[2]))
            if not x:
                x       = txn_m.get('XMLID_'+str(c_id))
            dd['x']     = x
            dd['d']     = doc_id
            dd['t']     = table_id
            dd['bbox']  = self.get_bbox_frm_xml(txn1, table_id, x)
            tmparr  = []
            t_d     = {}
            tc_d     = {}
            doc_id_d    = {}
            for rtup in vids:
                table_id, c_id, row_id, doc_id  = rtup
                c                       = c_id.split('_')[2]
                t_d.setdefault((table_id+'-'+c, doc_id), {})[(row_id, c_id)] = 1
                tc_d[table_id+'-'+c]    = doc_id
                doc_id_d.setdefault(doc_id, {})[table_id+'-'+c]    = 1
            max_tlen    = max(max_tlen, len(t_d.keys()))
            for (table_id, doc_id), rows in t_d.items():
                rows    = rows.keys()
                c_id    = rows[0][1]
                x       = txn_m.get('COLXMLID_'+str('_'.join(table_id.split('-'))))
                if not x:
                    x       = txn_m.get('XMLID_'+str(c_id))
                rd      = {'t':table_id, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id.split('-')[0], x), 'd':doc_id, 'rid':map(lambda x:x[0], rows)}
                tmparr.append(rd)
            dd['table_ids']     = tmparr  
            dd['table_str']     = ', '.join(tc_d.keys())
            dd['i']             = ii

            if vgh_id in revgroup_d:
                dd['g']         = revgroup_d.get(vgh_id, {}).keys()
                for grp_id in dd['g']:
                    tmpd            = {}
                    doc_tcd = {}
                    if not revgroup_d[vgh_id][grp_id][1]:
                        tmpd    = tc_d
                    else:
                        for tsr in revgroup_d[vgh_id][grp_id][1].split('#'):
                            if tsr not in tc_d:continue
                            tmpd[tsr]   = tc_d[tsr]
                    for tkey, doc_k in tc_d.items():
                        doc_tcd.setdefault(doc_k, {})[tkey]     = 1
                    for doc_id in doc_id_d.keys():
                        grp_doc_ids.setdefault(grp_id, {}).setdefault(d_revgroup_d.get(doc_id, {'':''}).keys()[0], {}).setdefault(vgh_id, {}).update(doc_tcd.get(doc_id, {}))
                    group_d[grp_id][vgh_id] = (ii, revgroup_d[vgh_id][grp_id][0], tmpd, revgroup_d[vgh_id][grp_id][1])
            f_arr.append(dd)



        g_ar    = []
        for k, v in group_d.items():
            #print [k]
            doc_grp = grp_doc_ids.get(k, {})
            for tk, tvids in doc_grp.items():
                tmp_grp_name    = ''
                if tk in grp_info:
                    tmp_grp_name    = ' - '+grp_info[tk]
                tmp_grp_name    = grp_info.get(k, k)+tmp_grp_name
                tmp_grp_name    = doc_vgh_map.get((k, tk), tmp_grp_name)    
                dd  = {'grp':tmp_grp_name, 'vids':{}, 'grpid':k, 'doc_ids':{}, 'doc_grpid':tk}
                table_ids   = {}
                for vid, i in v.items():
                    if vid not in tvids:continue
                    dd['vids'][i[0]]   = str(i[1])
                    if len(i) <3:
                        dd['vids'][i[0]]   = str(i[0])
                        continue
                    table_ids.update(tvids[vid])
                    for tk, dv in tvids[vid].items(): 
                        dd['doc_ids'][dv]  = 1
                    #for ktup in vgh_id_d[vid].keys():
                    #    table_ids[ktup[0]]  = ktup[3]
                #print k in grp_doc_map_d, grp_doc_map_d.get(k, {})
                #if k in grp_doc_map_d:
                #    table_ids   = grp_doc_map_d[k]
                #    dd['doc_filter'] = 'Y'
                #    dd['doc_ids'] = {}
                #    for k, v in grp_doc_map_d[k].items():
                #        dd['doc_ids'][v] = 1
                dd['table_ids'] = table_ids
                g_ar.append(dd)
        if not g_ar:
            g_ar.append({'grp':'', 'vids':{}, 'grpid':'new'})
        dg_ar   = []
        for k, v in d_group_d.items():
            #print [k]
            dd  = {'grp':grp_info.get(k, k), 'vids':{}, 'grpid':k, 'doc_ids':{}}
            table_ids   = {}
            for vid, i in v.items():
                dd['vids'][i[0]]   = str(i[1])
                if len(i) <3:
                    dd['vids'][i[0]]   = str(i[0])
                    continue
                table_ids.update(i[2])
                for tk, dv in i[2].items(): 
                    dd['doc_ids'][dv]  = 1
                #for ktup in vgh_id_d[vid].keys():
                #    table_ids[ktup[0]]  = ktup[3]
            #print k in grp_doc_map_d, grp_doc_map_d.get(k, {})
            #if k in grp_doc_map_d:
            #    table_ids   = grp_doc_map_d[k]
            #    dd['doc_filter'] = 'Y'
            #    dd['doc_ids'] = {}
            #    for k, v in grp_doc_map_d[k].items():
            #        dd['doc_ids'][v] = 1
            dd['table_ids'] = table_ids
            dg_ar.append(dd)
        if not dg_ar:
            dg_ar.append({'grp':'', 'vids':{}, 'grpid':'new'})
        #sys.exit()
        res = [{'message':'done', 'data':f_arr, 'groups':g_ar, 'max_tlen':max_tlen, 'docs':df_arr, 'dmax_tlen':dmax_tlen, 'dgroups':dg_ar}]
        return res

    def add_new_line_itesm(self, ijson):
        pass

    def unmerge_label(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_id           = ijson['t_id']
        table_type      = ijson['table_type']
        sql             = "select m_rows from mt_data_builder where table_type='%s' and taxo_id in (%s)"%(table_type , t_id)
        cur.execute(sql)
        res             = cur.fetchone()
        t_ids           = res[0].split('_')
        sql             = "select taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id,m_rows from mt_data_builder where table_type='%s' and taxo_id in (%s)"%(table_type , ', '.join(map(lambda x:str(x), t_ids)))
        cur.execute(sql)
        res         = cur.fetchall()
        taxo_d  = {}
        table_ids   = {}
        for rr in res:
            taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows    = rr
            doc_id      = str(doc_id)
            table_id    = str(table_id)
            table_ids[table_id]   = 1
            comp    = ''
            if gcom == 'Y' or ngcom == 'Y':
                comp    = 'Y'
            taxo_d.setdefault(taxo_id, {'order_id':order_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows})['ks'].append((table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id))


        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id)).replace('\xc2\xa0', '')
                    t       = ' '.join(t.split())
                    r_ld.setdefault(table_id, {}).setdefault(r, []).append((c, t))
        f_ar        = []
        done_d      = {}
        #for dd in f_taxo_arr:
        for row_id, dd in taxo_d.items():
            ks      = dd['ks']
            taxos   = dd['t_l'].split(' / ')[0]
            row     = {'t_id':row_id} #'t_l':taxo}
            f_dup   = ''
            label_d = {}
            for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id in ks:
                table_id    = str(table_id)
                c_id        = str(c_id)
                r           = int(c_id.split('_')[1])
                c           = int(c_id.split('_')[2])
                x           = txn_m.get('XMLID_'+c_id)
                t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                if tlabel:
                    tlabel  = self.convert_html_entity(tlabel)
                #print [taxo, table_id, c_id, t, ph, tlabel]
                row[table_id+'-'+ph+'-'+tlabel]    = {'v':t, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 't':table_id, 'r':r}
                if c_id in done_d:
                    f_dup   = 'Y'
                    row[table_id+'-'+ph+'-'+tlabel]['m']    = 'Y'
                if gcom == 'Y':
                    row[table_id+'-'+ph+'-'+tlabel]['g_f']    = 'Y'
                    row['f']    = 'Y'
                if ngcom == 'Y':
                    row[table_id+'-'+ph+'-'+tlabel]['ng_f']    = 'Y'
                    row['f']    = 'Y'
                done_d[c_id]    = 1
                txts    = r_ld[table_id][r]
                txts.sort(key=lambda x:x[0])
                txts_ar = []
                xml_ar  = []
                for x in txts:
                    txts_ar.append(x[1])
                    pkey    = table_id+'_'+str(r)+'_'+str(x[0])
                    xml_ar.append(txn_m.get('XMLID_'+pkey))
                txts    = self.convert_html_entity(' '.join(txts_ar))
                tph     = ph
                if tlabel:
                    tph = ph+'('+self.value_type_map.get(tlabel, tlabel)+')'
                grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                label_d.setdefault(grm_txts, {'txt':txts,'bbox':self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml_ar)), 'x':':@:'.join(xml_ar), 'd':doc_id, 't':table_id, 'v':{}})['v'][table_id+'-'+ph+'-'+tlabel]    = 1
            if len(label_d.keys()) > 1:
                row['lchange']  = 'Y'
                row['ldata']    = label_d.values()
            lble    = label_d.keys()
            lble.sort(key=lambda x:len(x), reverse=True)
            if dd.get('u_label', ''):
                row['t_l']  = dd.get('u_label', '')
            else:
                row['t_l']  = label_d[lble[0]]['txt']
            row['x']    = label_d[lble[0]]['x']
            row['bbox'] = label_d[ lble[0]]['bbox']
            row['t']    = label_d[ lble[0]]['t']
            row['d'] = label_d[ lble[0]]['d']
            row['l']    = len(ks)
            row['fd']   = f_dup
            row['order']   = dd['order_id']
            if dd.get('m_rows', ''):
                row['merge']   = 'Y'
            if dd.get('missing', ''):
                row['missing']  = dd['missing']
            f_ar.append(row)
        sql             = "update mt_data_builder set isvisible='Y' where table_type='%s' and taxo_id in (%s)"%(table_type , ', '.join(map(lambda x:str(x), t_ids)))
        cur.execute(sql)
        sql             = "update mt_data_builder set isvisible='N' where table_type='%s' and taxo_id in (%s)"%(table_type , t_id)
        cur.execute(sql)
        conn.commit()
        conn.close()
        res             = [{'message':'done', 'data':f_ar, 'd_ids':{t_id:1}}]
        return res

    def update_vgh_group_text(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type    = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        i_ar        = []
        dtime   = str(datetime.datetime.now()).split('.')[0]
        for rr in ijson['data']:
            sql     = "update vgh_group_map set group_txt=%s where vgh_id in (%s) and table_type ='%s'"%(rr['grpid'], ','.join(rr['vids']), table_type)
            cur.execute(sql)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return self.read_all_vgh_texts(ijson)

    def update_vgh_group(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type    = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        sql = "CREATE TABLE IF NOT EXISTS vgh_group_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id VARCHAR(20), table_type VARCHAR(100), group_txt VARCHAR(20), user_name VARCHAR(100), datetime TEXT);"
        cur.execute(sql)

        #sql = "CREATE TABLE IF NOT EXISTS doc_group_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id TEXT, table_id TEXT, table_type VARCHAR(100), group_txt VARCHAR(20), vgh_id TEXT, user_name VARCHAR(100), datetime TEXT);"
        #cur.execute(sql)

        doc_vgh     = ijson.get('doc_vgh', 'VGH')

        i_ar        = []
        dtime   = str(datetime.datetime.now()).split('.')[0]
        doc_vgh_map = []
        u_doc_vgh_map = []
        sql = "select vgh_group_id, doc_group_id, group_txt from vgh_doc_map where table_type='%s'"%(table_type)
        cur.execute(sql)
        doc_vgh_map_d = {}
        res = cur.fetchall()
        for rr in res:
            vgh_group_id, doc_group_id, group_txt   = rr
            doc_vgh_map_d[(vgh_group_id, doc_group_id)]   = group_txt
        if 1:
            new_grp = []
            for ii, rr in enumerate(ijson['data']):
                if rr.get('txt_update', '') != 'Y':continue
                if rr['grpid'] == 'new':
                    new_grp.append((rr['grp'], ii))
                else:
                    if rr.get('doc_grpid', '') != '':
                        if(rr['grpid'], rr['doc_grpid']) in doc_vgh_map_d:
                            u_doc_vgh_map.append((rr['grp'], table_type, rr['grpid'], rr['doc_grpid']))
                        else:
                            doc_vgh_map.append((rr['grp'], table_type, rr['grpid'], rr['doc_grpid']))
                        cur.execute(sql)
                    else:
                        sql     = "update vgh_group_info set group_txt='%s' where table_type ='%s' and group_id='%s'"%(rr['grp'], table_type, rr['grpid'])
                        cur.execute(sql)
            if new_grp:
                i_ar    = []
                dtime   = str(datetime.datetime.now()).split('.')[0]
                with conn:
                    #sql = "select max(taxo_id) from kpi_input"
                    sql = "select seq from sqlite_sequence WHERE name = 'vgh_group_info'"
                    cur.execute(sql)
                    r       = cur.fetchone()
                    g_id    = 1
                    if r and r[0]:
                        g_id    = int(r[0])+1
                    
                    sql = "select max(group_id) from vgh_group_info"
                    cur.execute(sql)
                    r       = cur.fetchone()
                    tg_id   = 1
                    if r and r[0]:
                        tg_id    = int(r[0])+1
                    g_id     = max(g_id, tg_id)
                    for ng in new_grp:
                        ijson['data'][ng[1]]['grpid']   = g_id
                        i_ar.append((g_id, table_type, ng[0], ijson['user'], dtime))
                        g_id    += 1
                cur.executemany('insert into vgh_group_info(group_id, table_type, group_txt, user_name, datetime) values(?,?,?,?,?)', i_ar)
        if u_doc_vgh_map:
            cur.executemany("update vgh_doc_map set group_txt=? where table_type=? and vgh_group_id=? and doc_group_id=?", u_doc_vgh_map)
        if doc_vgh_map:
            cur.executemany("insert into vgh_doc_map(group_txt, table_type , vgh_group_id, doc_group_id)values(?,?,?,?)", doc_vgh_map)
        conn.commit()
        exists      = {}
        sql         = "select vgh_id, group_txt from vgh_group_map where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        for rr in res:
            vgh_id, group_txt   = rr
            exists[(str(vgh_id), group_txt)] = 1

        #dexists      = {}
        #sql         = "select doc_id, table_id, group_txt from doc_group_map where table_type='%s'"%(table_type)
        #try:
        #    cur.execute(sql)
        #    res         = cur.fetchall()
        #except:
        #    res = []
        #for rr in res:
        #    doc_id, table_id, group_txt   = rr
        #    dexists[(str(doc_id), str(table_id), group_txt)] = 1
        i_ar    = []
        di_ar   = []
        u_ar    = []
        if doc_vgh == 'DOC':
            for rr in ijson['data']:
                for rid, doc_tables in rr['vids'].items():
                    if(rid, rr['grpid']) not in exists:
                        i_ar.append((table_type, rr['grpid'], rid, '', doc_vgh, ijson['user'], dtime))
                    #else:
                    #    u_ar.append(('#'.join(doc_tables), ijson['user'], dtime, table_type, rr['grpid'], rid, doc_vgh))
        else:
            for rr in ijson['data']:
                for rid, doc_tables in rr['vids'].items():
                    if(rid, rr['grpid']) not in exists:
                        i_ar.append((table_type, rr['grpid'], rid, '#'.join(doc_tables), doc_vgh, ijson['user'], dtime))
                    else:
                        u_ar.append(('#'.join(doc_tables), ijson['user'], dtime, table_type, rr['grpid'], rid, doc_vgh))
                        
                    #if rr.get('doc_filter', '') == 'Y':
                    #    for doc_table in doc_tables:
                    #        doc_id, table_id    = doc_table.split('-')
                    #        if(doc_id, table_id, str(rr['grpid'])) not in dexists:
                    #            di_ar.append((table_type, rr['grpid'], rid, doc_id, table_id, ijson['user'], dtime))
                    
        #print di_ar
        #sys.exit()
        cur.executemany("insert into vgh_group_map(table_type, group_txt, vgh_id, table_str, doc_vgh, user_name, datetime)values(?,?,?,?,?,?,?)", i_ar)
        cur.executemany("update vgh_group_map set table_str=?, user_name=?, datetime=? where table_type=? and  group_txt=? and  vgh_id=? and doc_vgh=?", u_ar)
        conn.commit()

        #cur.executemany("insert into doc_group_map(table_type, group_txt, vgh_id, doc_id, table_id, user_name, datetime)values(?,?,?,?,?,?,?)", di_ar)
        #conn.commit()
        #for rr in ijson['data']:
        #    sql     = "update vgh_group_map set group_txt='%s' where vgh_id in (%s) and table_type ='%s'"%(rr['grp'], ','.join(rr['vids']), table_type)
        #    cur.execute(sql)
        conn.close()
        res = [{'message':'done'}]
        return self.read_all_vgh_texts(ijson)

    def delete_vgh_group(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type    = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        i_ar        = []
        di_ar       = []
        rr  = ijson
        row_d   = rr['r_ids']
        #for rid, doc_tables in rr['vids'].items():
        #    i_ar.append((table_type, rr['grpid'], rid))
            #if rr.get('doc_filter', '') == 'Y':
            #    for doc_table in doc_tables:
            #        doc_id, table_id    = doc_table.split('-')
            #        di_ar.append((table_type, rr['grpid'], rid, doc_id, table_id))
        #print i_ar
        cur.execute("delete from vgh_group_map where row_id in (%s)"%(', '.join(filter(lambda x:x != 'None', row_d))))
        #cur.executemany("delete from vgh_group_map where table_type=? and group_txt=? and  vgh_id=?", i_ar)
        #cur.executemany("delete from doc_group_map where table_type=? and group_txt=? and  vgh_id=? and doc_id=? and table_id=?", di_ar)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def update_order_multiple(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select row_id, taxo_id, order_id from mt_data_builder where table_type='%s' and isvisible='Y' group by taxo_id"%(table_type)
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        for rr in res:
            row_id, taxo_id, order_id   = rr
            taxo_o_d[taxo_id]           = order_id
            order_d.setdefault(order_id, {})[taxo_id]   = min(row_id, order_d.get(order_id, {}).get(taxo_id, row_id))
        order_ids   = order_d.keys()
        order_ids.sort()
        tid1    = int(ijson['t_id1'])
        tid2    = map(lambda x:int(x), ijson['t_ids'])
        direction = ijson['pos']
        od1     = taxo_o_d[tid1]
        new_order   = []
        new_order_d = {}
        for order_id in order_ids:
            if order_id < od1:
                taxo_ids    = order_d[order_id].keys()
                for taxo_id in taxo_ids:
                    if taxo_id in tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id > od1:
                taxo_ids    = order_d[order_id].keys()
                order_id    += len(tid2)+1
                for taxo_id in taxo_ids:
                    if taxo_id in tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id == od1:
                if direction == 'after':
                    new_order_d[tid1] = od1
                    new_order.append((od1, tid1))
                    tod1 = od1+1
                    for tid in tid2:
                        new_order_d[tid] = tod1
                        new_order.append((tod1, tid))
                        tod1 += 1
                else:
                    tod1 = od1
                    for tid in tid2:
                        new_order_d[tid] = tod1
                        new_order.append((tod1, tid))
                        tod1 += 1
                    new_order_d[tid1] = tod1
                    new_order.append((tod1, tid1))
        new_order   = map(lambda x: x+(table_type, ), new_order)
        cur.executemany("update mt_data_builder set order_id=? where taxo_id=? and table_type=?", new_order)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'order_d':new_order_d, 'tid1':tid1, 'tid2':tid2}]
        return res

    def update_order(self, ijson):
        if ijson.get('t_ids', []):
            return self.update_order_multiple(ijson)
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select row_id, taxo_id, order_id from mt_data_builder where table_type='%s' and isvisible='Y' group by taxo_id"%(table_type)
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        for rr in res:
            row_id, taxo_id, order_id   = rr
            taxo_o_d[taxo_id]           = order_id
            order_d.setdefault(order_id, {})[taxo_id]   = min(row_id, order_d.get(order_id, {}).get(taxo_id, row_id))
        order_ids   = order_d.keys()
        order_ids.sort()
        tid1    = int(ijson['t_id1'])
        tid2    = int(ijson['t_id2'])
        direction = ijson['pos']
        od1     = taxo_o_d[tid1]
        new_order   = []
        new_order_d = {}
        for order_id in order_ids:
            if order_id < od1:
                taxo_ids    = order_d[order_id].keys()
                for taxo_id in taxo_ids:
                    if taxo_id == tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id > od1:
                taxo_ids    = order_d[order_id].keys()
                order_id    += 2
                for taxo_id in taxo_ids:
                    if taxo_id == tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id == od1:
                if direction == 'after':
                    new_order_d[tid1] = od1
                    new_order_d[tid2] = od1+1
                    new_order.append((od1, tid1))
                    new_order.append((od1+1, tid2))
                else:
                    new_order_d[tid2] = od1
                    new_order_d[tid1] = od1+1
                    new_order.append((od1, tid2))
                    new_order.append((od1+1, tid1))
        new_order   = map(lambda x: x+(table_type, ), new_order)
        cur.executemany("update mt_data_builder set order_id=? where taxo_id=? and table_type=?", new_order)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'order_d':new_order_d, 'tid1':tid1, 'tid2':tid2}]
        return res

    def merge_table_to_row(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = map(lambda x:int(x), ijson['t_ids'])
        t_id_d          = {}
        fnd_taxo        = {}
        for t in t_ids:
            t_id_d[t]   = 1
            fnd_taxo[t]  = 1
        table_ids       = map(lambda x:x.split('-')[1], ijson['st_ids'])
        tmp_tble_d      = {}
        for t in table_ids:
            tmp_tble_d[t]   = 1
        
        sql             = "select row_id, taxo_id, order_id, table_id, xml_id from mt_data_builder where table_type='%s' and isvisible='Y' and taxo_id in (%s)"%(table_type, ', '.join(map(lambda x:str(x), ijson['t_ids'])))
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        new_row_id      = {}
        table_taxo_d    = {}
        f_ph            = {}
        for rr in res:
            row_id, taxo_id, order_id, table_id, xml_id         = rr
            taxo_o_d[taxo_id]                           = order_id
            if int(taxo_id) in t_id_d and str(table_id) in tmp_tble_d:
                f_ph.setdefault(str(taxo_id), {})[(taxo_id, xml_id, row_id)]    = 1
                table_taxo_d.setdefault(str(table_id), {})[taxo_id] = 1
                new_row_id[str(row_id)]      = 1
                if int(taxo_id) in fnd_taxo:
                    del fnd_taxo[int(taxo_id)]
            order_d.setdefault(order_id, {})[taxo_id]   = min(row_id, order_d.get(order_id, {}).get(taxo_id, row_id))
        if ijson.get('taxo_flg', '') == 1:
            lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
            env         = lmdb.open(lmdb_path1)
            txn_m       = env.begin()
            lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
            env         = lmdb.open(lmdb_path1)
            txn         = env.begin()
            res = self.check_ph_overlap(f_ph, txn_m, txn)
            if res:
                return res
        error   = {}
        if ijson.get('taxo_flg', '') != 1:
            for taxo_id, cnt in table_taxo_d.items():
                if len(cnt.keys()) > 1:
                    error.update(cnt)
        if error:
            res = [{'message':'Table Overlap '+json.dumps(error.keys()), 'data':[], 'taxo_d':[]}]
            return res
        rem_taxos   = fnd_taxo.keys()
        if len(rem_taxos) != 1:
            res = [{'message':'Error Nothing to merge'}]
            return res
        if ijson.get('target_taxo', ''):
            sql     = "update mt_data_builder set user_taxonomy='%s', taxo_id=%s where row_id in (%s)"%(ijson.get('target_taxo', ''), rem_taxos[0], ', '.join(new_row_id.keys()))
        elif ijson.get("taxo_flg", '') == 1:
            res = [{"message":"Error Not allowed without target taxo"}]
            conn.close()
            return res
        else:
            sql     = "update mt_data_builder set taxo_id=%s where row_id in (%s)"%(rem_taxos[0], ', '.join(new_row_id.keys()))
        cur.execute(sql)
        conn.commit()
        res = [{'message':'done', 'sd':t_id_d, 'd':rem_taxos[0], 'table_ids':tmp_tble_d}]
        return res

    def split_row_multi(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select row_id, taxo_id, order_id, table_id, xml_id from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        new_row_id      = {}
        table_taxo_d    = {}
        for rr in res:
            row_id, taxo_id, order_id, table_id, xml_id         = rr
            taxo_o_d[taxo_id]                           = order_id
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            #if c_id.split('_')[1]   != '5':continue
            gv_xml  = c_id
            new_row_id[str(row_id)]      = 1
            table_taxo_d.setdefault(taxo_id, {}).setdefault(table_id, {}).setdefault(int(c_id.split('_')[1]), {})[str(row_id)]   = 1
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            for taxo_id, table_d in table_taxo_d.items():
                for table_id, row_d in table_d.items():
                    if len(row_d.keys()) <= 1:continue
                    rows    = row_d.keys()
                    rows.sort() 
                    for r in rows[1:]:
                        sql     = "insert into mt_data_builder(taxo_id)values(-1)"
                        #cur.execute(sql)
                        #conn.commit()
                        sql     = "update mt_data_builder set taxo_id=%s where row_id in (%s)"%(g_id, ', '.join(row_d[r].keys()))
                        print [table_id, sql]
                        #cur.execute(sql)
                        #conn.commit()
                        g_id    += 1

    def split_taxo_cell(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = ijson['t_id']
        sql             = "select distinct(user_taxonomy) from mt_data_builder where table_type='%s'"%(table_type)
        cur.execute(sql)
        res             = cur.fetchall()
        e_taxo  = {}
        for rr in res:
            e_taxo[rr[0]]  = 1
            
        
        sql             = "select row_id, taxo_id, order_id, table_id, xml_id from mt_data_builder where table_type='%s' and isvisible='Y' and row_id in (%s) and taxo_id in (%s)"%(table_type, ', '.join(map(lambda x:str(x), ijson['r_ids'])), t_ids)
        cur.execute(sql)
        res             = cur.fetchall()
        taxo_o_d        = {}
        new_row_id      = {}
        u_taxo          = {}
        for rr in res:
            row_id, taxo_id, order_id, table_id, xml_id = rr
            table_id    = str(table_id)
            xml_id      = str(xml_id)
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            taxo_o_d[taxo_id]                           = order_id
            new_row_id[str(row_id)]                     = 1
            u_taxo[self.gen_taxonomy(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))] = 1
        if u_taxo:
            u_taxo  = u_taxo.keys()
            u_taxo.sort(key=lambda x:len(x))
            u_taxo  = u_taxo[-1]
            if u_taxo in e_taxo :
                u_taxo  += '-N-1'
        else:
            u_taxo  = ''
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            r       = cur.fetchone()
            sql     = "insert into mt_data_builder(taxo_id)values(-1)"
            cur.execute(sql)
            conn.commit()
            sql     = "update mt_data_builder set taxo_id=%s, user_taxonomy='%s' where row_id in (%s)"%(g_id, u_taxo, ', '.join(new_row_id.keys()))
            cur.execute(sql)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res


    def split_row(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = map(lambda x:int(x), ijson['t_ids'])
        t_id_d          = {}
        for t in t_ids:
            t_id_d[t]   = 1
        table_ids       = map(lambda x:x.split('-')[1], ijson['st_ids'])
        tmp_tble_d      = {}
        for t in table_ids:
            tmp_tble_d[t]   = 1
        
        sql             = "select row_id, taxo_id, order_id, table_id, xml_id from mt_data_builder where table_type='%s' and isvisible='Y' and taxo_id in (%s)"%(table_type, ', '.join(map(lambda x:str(x), ijson['t_ids'])))
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        new_row_id      = {}
        for rr in res:
            row_id, taxo_id, order_id, table_id, xml_id         = rr
            taxo_o_d[taxo_id]                           = order_id
            if int(taxo_id) in t_id_d and str(table_id) in tmp_tble_d:# and xml_id in {'x2868_4#x1000428_4':1, 'x2872_4':1}:
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                #if c_id.split('_')[1]   != '5':continue
                gv_xml  = c_id
                new_row_id[str(row_id)]      = 1
            order_d.setdefault(order_id, {})[taxo_id]   = min(row_id, order_d.get(order_id, {}).get(taxo_id, row_id))
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            r       = cur.fetchone()
            sql     = "insert into mt_data_builder(taxo_id)values(-1)"
            cur.execute(sql)
            conn.commit()
            sql     = "update mt_data_builder set taxo_id=%s where row_id in (%s)"%(g_id, ', '.join(new_row_id.keys()))
            cur.execute(sql)
            conn.commit()
        order_ids   = order_d.keys()
        order_ids.sort()
        tid1    = t_ids[0]
        tid2    = g_id
        od1     = taxo_o_d[tid1]
        new_order   = []
        new_order_d = {}
        for order_id in order_ids:
            if order_id < od1:
                taxo_ids    = order_d[order_id].keys()
                for taxo_id in taxo_ids:
                    if taxo_id == tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id > od1:
                taxo_ids    = order_d[order_id].keys()
                order_id    += 2
                for taxo_id in taxo_ids:
                    if taxo_id == tid2:continue
                    new_order_d[taxo_id] = order_id
                    new_order.append((order_id, taxo_id))
            elif order_id == od1:
                new_order_d[tid1] = od1
                new_order_d[tid2] = od1+1
                new_order.append((od1, tid1))
                new_order.append((od1+1, tid2))
        new_order   = map(lambda x: x+(table_type, ), new_order)
        cur.executemany("update mt_data_builder set order_id=? where taxo_id=? and table_type=?", new_order)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'order_d':new_order_d, 'sd':t_id_d, 's':tid1, 'd':tid2, 'table_ids':tmp_tble_d}]
        return res
    
    def read_main_tables(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number)
        map_dic = {
                    'IS'    :0,
                    'BS'    :1,
                    'CF'    :2,
                    'RBS'    :3,
                    'RBG'    :4,
                    }
        
        ks = rev_m_tables.keys()
        ks.sort(key=lambda x:map_dic.get(x, 9999))
        list_mt = []
        for k in ks:
            dic = {'l': k, 'k': k, 'f': False}
            list_mt.append(dic)
        res = [{'message': 'done', 'data': list_mt}]
        return res

    def sheet_id_map(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql   = "select main_header, sub_header, sheet_id, node_name, description from node_mapping where review_flg = 0"
        cur.execute(sql)
        tres        = cur.fetchall()
        #print rr, len(tres)
        ddict = dd(set)
        for tr in tres:
            #print tr
            main_header, sub_header, sheet_id, node_name, description = map(str, tr)
            ddict[sheet_id] = [main_header, sub_header, node_name, description]
        return ddict

    def get_main_table_info_new(self, company_name, model_number, f_tables=[]):
        sheet_id_map = self.sheet_id_map()
        m_tables        = {}
        doc_m_d         = {}
        rev_m_tables    = {}
        db_file         = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select sheet_id, doc_id, doc_name, table_id from table_group_mapping"
        cur.execute(sql)
        res             = cur.fetchall()
        tmpres          = []
        table_type_m    = {}
        for rr in res:
            sheet_id, doc_id, doc_name, table_id    = rr
            main_header, sub_header, node_name, description = sheet_id_map.get(str(sheet_id), ['','','',''])
            #print rr, (main_header, sub_header, node_name, description)
            if f_tables and node_name not in f_tables:continue
            #print rr, main_header
            #if main_header not in ['Main Table']:continue
            desc_sp = description.split('-')
            desc_sp = map(lambda x:x.strip(), desc_sp[:])
            desc_sp = list(sets.Set(desc_sp))
            if len(desc_sp) == 1:
               description = desc_sp[0]
            table_type_m[node_name]    = description
            tmpres.append((node_name, doc_id, table_id, main_header))
        cur.close()
        conn.close()
        m_tables        = {}
        doc_m_d         = {}
        rev_m_tables    = {}
        main_headers    = {}
        for rr in tmpres:
            table_type, doc_id, table_id_str, main_header    = rr
            doc_id      = str(doc_id)
            table_type  = str(table_type)
            for table_id in table_id_str.split('^!!^'):
                if not table_id:continue
                main_headers[main_header]    = 1
                table_id            = str(table_id)
                m_tables[table_id]  = table_type
                rev_m_tables.setdefault(table_type, {})[table_id]   = 1
                doc_m_d[table_id]   = doc_id
        table_type_m['main_header'] = main_headers.keys()
        return m_tables, rev_m_tables, doc_m_d, table_type_m
             

    def get_main_table_info(self, company_name, model_number, f_tables=[]):
        if company_name not in ['KONECorporation']:
            return self.get_main_table_info_new(company_name, model_number, f_tables)
        db_file         = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        if f_tables:
            sql             = "select table_type, doc_id, table_id from table_map where table_type in (%s)"%(','.join(map(lambda x:'"'+x+'"', f_tables)))
        else:
            sql             = "select table_type, doc_id, table_id from table_map"
        cur.execute(sql)
        res             = cur.fetchall()
        cur.close()
        conn.close()
        m_tables        = {}
        doc_m_d         = {}
        rev_m_tables    = {}
        table_type_m    = {}
        for rr in res:
            table_type, doc_id, table_id_str    = rr
            doc_id      = str(doc_id)
            table_type  = str(table_type)
            table_type_m[table_type]    = table_type
            for table_id in table_id_str.split(':^:'):
                if not table_id:continue
                table_id            = str(table_id).strip()
                m_tables[table_id]  = table_type
                rev_m_tables.setdefault(table_type, {})[table_id]   = 1
                doc_m_d[table_id]   = doc_id
        if rev_m_tables:
            table_type_m['main_header'] = rev_m_tables.keys()[0]
        else:
            table_type_m['main_header'] = ''
        return m_tables, rev_m_tables, doc_m_d, table_type_m

    def update_label_change(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_id            = ijson['t_id']
        u_ar            = []
        for rr in ijson['data']:
            u_ar.append((rr['sts'], rr['id'], t_id))
        cur.executemany("update mt_data_builder set vgh_group=? where xml_id=? and taxo_id=?", u_ar)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def update_cid_xml_id(self, ijson):
        #sys.exit()
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        conn, cur       = conn_obj.sqlite_connection(db_file)
        try:
            sql = "alter table mt_data_builder add column xml_id TEXT"
            cur.execute(sql)
        except:pass
        sql = "select row_id, c_id from mt_data_builder"
        cur.execute(sql)
        res = cur.fetchall()
        u_ar    = []
        for rr in res:
            row_id, c_id    = rr
            if c_id:
                x   = txn_m.get("XMLID_"+str(c_id))
                if x:
                    u_ar.append((x, row_id))
        cur.executemany("update mt_data_builder set xml_id=? where row_id=?", u_ar)
        conn.commit()
        conn.close()
        pass

    def update_ph(self, ijson):
        #sys.exit()
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        conn, cur       = conn_obj.sqlite_connection(db_file)
        try:
            sql = "alter table mt_data_builder add column xml_id TEXT"
            cur.execute(sql)
        except:pass
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()
        if ijson.get('table_ids', []):
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder where table_id in (%s)"%(', '.join(map(lambda x:str(x), ijson.get('table_ids', []))))
        elif ijson.get('table_types', []):
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder where table_type in (%s)"%(', '.join(map(lambda x:'"'+str(x)+'"', ijson.get('table_types', []))))
        else:
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder"
        cur.execute(sql)
        res = cur.fetchall()
        u_ar    = []
        for rr in res:
            row_id, c_id, xml_id, table_id, ph    = rr
            if table_id and xml_id:
                table_id = str(table_id)
                key     = table_id+'_'+self.get_quid(xml_id)
                ph_map  = txn.get('PH_MAP_'+str(key))
                #print c_id, ph_map
                if ph_map:
                    period_type, period, currency, scale, value_type    = ph_map.split('^')
                    if 1:#ph != period_type+period:
                        u_ar.append((period_type+period, row_id))
        #for rr in u_ar:
        #    print rr
        #sys.exit()
        print 'Total ', len(u_ar)
        cur.executemany("update mt_data_builder set ph=? where row_id=?", u_ar)
        conn.commit()
        conn.close()
        pass

    def add_missing_c_id(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number)
        db_file         = self.get_db_path(ijson)
        print db_file
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        conn, cur       = conn_obj.sqlite_connection(db_file)
        vgh_text_d      = {}
        sql             = "select table_type, vgh, vgh_id from vgh_info"
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            table_type, vgh, vgh_id = rr
            vgh_text_d.setdefault(table_type, {})[vgh.lower()] = (vgh_id, vgh)
        sql             = "select table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id,m_rows from mt_data_builder where isvisible='Y' and table_type='%s'"%(ijson['table_type'])
        cur.execute(sql)
        res             = cur.fetchall()
        table_row_d     = {}
        table_ds        = {}
        t_col_vgh       = {}
        t_row_ph        = {}
        xml_row_d       = {}
        taxo_d          = {}
        table_row_overlap_d     = {}
        exists_table    = {}
        for rr in res:
            table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id, m_rows = rr
            if xml_id:
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id    = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                if ijson.get('table_ids', '') and int(table_id) not in ijson.get('table_ids', []):continue
                r       = int(c_id.split('_')[1])
                xml_row_d.setdefault((table_id, xml_id), {}).setdefault((r, taxo_id), {})[xml_id]  = 1
                table_row_d.setdefault((table_id, r), {}).setdefault(xml_id, {})[taxo_id]    = rr
                table_ds[table_id]        = (table_type, doc_id)
                t_col_vgh[(table_id, int(c_id.split('_')[2]))]  = vgh_text
                t_row_ph.setdefault((table_id, r), {})
                t_row_ph[(table_id, r)][(table_id, ph, '')]  = t_row_ph[(table_id, r)].get((table_id, ph, ''), 0)+1
                taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'ks':[], 'm_rows':m_rows})
                exists_table[str(table_id)] = 1
                t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                table_row_overlap_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'ks':[], 'm_rows':m_rows})
                key     = table_id+'_'+self.get_quid(xml_id)
                ph_map  = txn.get('PH_MAP_'+str(key))
                if ph_map and clean_value:
                        period_type, period, currency, scale, value_type    = ph_map.split('^')
                        table_row_overlap_d.setdefault((table_type, period_type+period, clean_value, value_type), {})[taxo_id]  = 1

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()
        i_ar                = []
        new_vgh_text_d      = {}
        vgh_triplet_data    = {}
        dtime               = str(datetime.datetime.now()).split('.')[0]
        f_taxo_d            = {}
        table_type_d        = {}
        for table_id, (table_type, doc_id) in table_ds.items():
            if ijson.get('table_types', '') and table_type not in ijson.get('table_types', []) :continue
            table_id    = str(table_id)
            if ijson.get('table_ids', '') and int(table_id) not in ijson.get('table_ids', []):continue
            k   = 'GV_'+str(table_id)
            ids = txn_m.get(k)
            if not ids:continue
            ids         = ids.split('#')
            rd          = {}
            for c_id in ids:
                r       = int(c_id.split('_')[1])
                c       = int(c_id.split('_')[2])
                x       = txn_m.get('XMLID_'+c_id)
                t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                if not t:continue
                key     = table_id+'_'+self.get_quid(x)
                ph_map  = txn.get('PH_MAP_'+str(key))
                if ph_map:
                        period_type, period, currency, scale, value_type    = ph_map.split('^')
                        if period_type and period:
                            rd.setdefault(r, {})[(c, x)] = (c_id, period_type, period, currency, scale, value_type)
            rows    = rd.keys()
            rows.sort()
            new_rows    = {}
            for r in rows:
                col_xmls    = rd[r].keys()
                col_xmls.sort(key=lambda x:x[0])
                ph_ind      = t_row_ph.get((table_id, r), {})
                old_rows    = {}
                #print '\n=================================='
                for c, xml in col_xmls:
                    if(table_id, xml) in xml_row_d:
                        #print ((table_id, xml))
                        old_rows.update(xml_row_d[(table_id, xml)])
                #print 'r ',r, old_rows
                if not old_rows:
                    new_rows[r] = 1
                if len(old_rows.keys()) != 1:
                    continue
                o_r, taxo_id = old_rows.keys()[0]
                #print taxo_d[taxo_id]
                o_d = table_row_d[(table_id, o_r)]
                nrows   = {}
                for c, xml in col_xmls:
                    #print c, xml, xml in o_d
                    if xml in o_d:continue
                    c_id, period_type, period, currency, scale, value_type    = rd[r][(c, xml)]
                    ph  = period_type+period
                    tlabel  = ''
                    if (table_id, ph, tlabel) in ph_ind:
                        cind    = ph_ind[(table_id, ph, tlabel)]
                        ph_ind[(table_id, ph, tlabel)]    = cind+1
                        tlabel  = tlabel+'-'+str(cind) if tlabel else str(cind)
                    else:
                        ph_ind[(table_id, ph, tlabel)]    = 1
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    nrows[(c_id, xml, ph, tlabel)]   = t
                if nrows:
                    print '\n==========================================='
                    print table_id, table_type
                    for k, v in nrows.items():
                        c_id, xml, ph, tlabel   = k
                        print '\tNEW CELL',(table_type, doc_id), taxo_id, k, v
                        x       = xml
                        triplet =  self.read_triplet(table_id, txn_trip, xml, txn_trip_default)
                        vgh_triplet_data[(table_id, xml)] = triplet.get('VGH', {}).keys()
                        vgh_txt = vgh_triplet_data.get((table_id, xml), []) 
                        if not vgh_txt:
                            vgh_txt = [[txn_m.get('COLTEXT_'+table_id+'_'+str(c_id.split('_')[2]))]]
                        vgh_txt = ' '.join(vgh_txt[0])
                        gcom    = ''
                        ngcom   = ''
                        comp    = ''
                        try:
                            tlabel    = tlabel.decode('utf-8')
                        except:pass
                        try:
                            vgh_txt    = vgh_txt.decode('utf-8')
                        except:pass
                        if vgh_txt.lower() in vgh_text_d.get(table_type, {}):
                            vgh_id  = vgh_text_d[table_type][vgh_txt.lower()][0]
                        else:
                            vgh_id  = len(vgh_text_d.get(table_type, {}).keys())+1
                            vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                            new_vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                        #taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'rid':row_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows})
                        i_ar.append((taxo_id, taxo_d[taxo_id]['order_id'], table_type, taxo_d[taxo_id]['t_l'], taxo_d[taxo_id]['u_label'], taxo_d[taxo_id]['missing'], table_id, c_id, ph, tlabel, gcom, ngcom, 'Y', vgh_id, doc_id, -1, x, 'SYSTEM', dtime))
            rows    = new_rows.keys()
            rows.sort()
            for r in rows:
                table_type_d.setdefault(table_type, {}).setdefault(table_id, {})[r] = 1
                continue
                col_xmls    = rd[r].keys()
                col_xmls.sort(key=lambda x:x[0])
                ph_ind      = {}
                o_d         = {}
                nrows   = {}
                for c, xml in col_xmls:
                    if xml in o_d:continue
                    c_id, period_type, period, currency, scale, value_type    = rd[r][(c, xml)]
                    ph  = period_type+period
                    tlabel  = ''
                    if (table_id, ph, tlabel) in ph_ind:
                        cind    = ph_ind[(table_id, ph, tlabel)]
                        ph_ind[(table_id, ph, tlabel)]    = cind+1
                        tlabel  = tlabel+'-'+str(cind) if tlabel else str(cind)
                    else:
                        ph_ind[(table_id, ph, tlabel)]    = 1
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    nrows[(table_id, table_type, doc_id, c_id, xml, ph, tlabel)]   = t
                if nrows:
                    print '\n==========================================='
                    ks  = {}
                    dd  = {}
                    for k, v in nrows.items():
                        table_id, table_type, doc_id, c_id, xml, ph, tlabel = k
                        ks[(table_id, c_id, ph, tlabel, doc_id)]  = 1
                        triplet =  self.read_triplet(table_id, txn_trip, xml, txn_trip_default)
                        vgh_triplet_data[(table_id, xml)] = triplet.get('VGH', {}).keys()
                        print '\tNEW ROW ',k, v
                        key         = table_id+'_'+self.get_quid(xml)
                        for rc in ['COL', 'ROW']:
                            ngcom        = txn.get('COM_NG_'+rc+'MAP_'+str(key))
                            if ngcom:
                                for eq in ngcom.split('|'):
                                    formula, eq_str = eq.split(':$$:')
                                    dd.setdefault('ngcom', {})[c_id] = formula
                    
                            gcom        = txn.get('COM_G_'+rc+'MAP_'+key)
                            if gcom:
                                for eq in gcom.split('|'):
                                    formula, eq_str = eq.split(':$$:')
                                    dd.setdefault('gcom', {})[c_id] = formula
                    taxo    = 'TAS_TAXO_'+str(len(f_taxo_d.get(table_type, [])))
                    dd['t_l']   = [taxo]
                    dd['ks']    = ks.keys() 
                    f_taxo_d.setdefault(table_type, []).append(dd)
                    
        if table_type_d:
            #conn, cur       = conn_obj.sqlite_connection(db_file)
            #sql = "delete from mt_data_builder where table_id in (%s)"%(', '.join(map(lambda x:str(x), table_ids)))
            #print sql
            #cur.execute(sql)
            dtime   = str(datetime.datetime.now()).split('.')[0]
            #i_ar    = []
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            self.form_new_row(ijson, g_id, table_type_d, new_vgh_text_d, vgh_text_d, vgh_triplet_data, i_ar, doc_m_d, table_row_overlap_d)
        vgh_groups  = []
        for k, v in new_vgh_text_d.items():
            for t, vtup in v.items():
                vgh_groups.append((k, vtup[1], vtup[0]))
        print vgh_groups
        for rr in i_ar:
            print rr
        print 'Total ', len(i_ar)
        #cur.executemany("insert into mt_data_builder(taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label, gcom, ngcom, isvisible, vgh_text, doc_id, prev_id, xml_id, user_name, datetime)values(?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)", i_ar)

        #cur.executemany("insert into vgh_info(table_type, vgh, vgh_id)values(?,?, ?)", vgh_groups)
        #conn.commit()
        conn.close()
        pass


    def add_new_table(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        vgh_text_d      = {}
        sql             = "select table_type, vgh, vgh_id from vgh_info"
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            table_type, vgh, vgh_id = rr
            vgh_text_d.setdefault(table_type, {})[vgh.lower()] = (vgh_id, vgh)

            
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        sql             = "select table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id,m_rows from mt_data_builder where isvisible ='Y'"
        cur.execute(sql)
        res             = cur.fetchall()
        table_row_d     = {}
        exists_table    = {}
        for rr in res:
            table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id, m_rows = rr
            if xml_id:
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id    = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                r       = int(c_id.split('_')[1])
                exists_table[str(table_id)] = 1
                t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                key     = table_id+'_'+self.get_quid(xml_id)
                ph_map  = txn.get('PH_MAP_'+str(key))
                table_row_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'ks':[], 'm_rows':m_rows})
                if ph_map and clean_value:
                        period_type, period, currency, scale, value_type    = ph_map.split('^')
                        table_row_d.setdefault((table_type, period_type+period, clean_value, value_type), {})[taxo_id]  = 1
        conn.close()


        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()

        table_ids    = ijson['table_ids']
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number)
        #print m_tables
        #print '3136' in m_tables
        vgh_triplet_data                = {}
        f_taxo_d                      = {}
        new_vgh_text_d              = {}
        table_type_d                = {}
        for table_id in table_ids:
            table_id   = str(table_id)
            table_type  = m_tables[table_id]
            table_type_d.setdefault(table_type, {})[table_id]   = {'ALL'}
                    
        if table_type_d:
            conn, cur       = conn_obj.sqlite_connection(db_file)
            i_ar    = []
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            self.form_new_row(ijson, g_id, table_type_d, new_vgh_text_d, vgh_text_d, vgh_triplet_data, i_ar, doc_m_d, table_row_d)
            vgh_groups  = []
            for k, v in new_vgh_text_d.items():
                for t, vtup in v.items():
                    vgh_groups.append((k, vtup[1], vtup[0]))
            print 'Total ', len(i_ar)
            print vgh_groups
            #for rr in i_ar:
            #    print rr
            for table_type, table_ids in  table_type_d.items():
                sql = "delete from mt_data_builder where table_id in (%s) and table_type ='%s'"%(', '.join(map(lambda x:str(x), table_ids)), table_type)
                print sql
                cur.execute(sql)
            cur.executemany("insert into mt_data_builder(taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label, gcom, ngcom, isvisible, vgh_text, doc_id, prev_id, xml_id, user_name, datetime)values(?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)", i_ar)

            cur.executemany("insert into vgh_info(table_type, vgh, vgh_id)values(?,?, ?)", vgh_groups)
            conn.commit()
            conn.close()

    def validate_cell(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type       = str(ijson['table_type'])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1        = lmdb.open(lmdb_path)
        txn1        = env1.begin()

        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        
        sql         = "select taxo_id, table_id, doc_id, xml_id, ph from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()
        invalid_cell_dict = {}
        dmap = {'0':1, '1':1, '2':1, '3':1, '4':1, '5':1, '6':1, '7':1, '8':1, '9':1}
        for row in res:
            row = map(str, row)
            taxo_id, table_id, doc_id, xml_id, ph = row
            tk   = self.get_quid(table_id+ '_' +xml_id)
 
            c_id        = txn_m.get('XMLID_MAP_' + tk)
            if not c_id:continue

            ctable, crow, ccol = c_id.split('_')
            mkey = '%s-%s-'%(ctable, ccol)           

            cell_txt    = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
            numFlg = 0

            final_cell_txt = ''
            for t in cell_txt:
                if t in dmap:
                    final_cell_txt += t

            if final_cell_txt.strip():
                clean_value = cell_txt
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(cell_txt)
                except:
                    clean_value = ''
                if (not clean_value.strip()):
                    #print [taxo_id, table_id, doc_id, xml_id, ph, final_cell_txt, clean_value]
                    invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 1

            x       = xml_id
            cell_bbox   = self.get_bbox_frm_xml(txn1, table_id, x)
            if not cell_bbox:
                if invalid_cell_dict.get(taxo_id, {}).get(mkey, '') != 1:
                    invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 2
                else:
                    invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 3
        return invalid_cell_dict

    def form_new_row(self, ijson, g_id, table_type_d, new_vgh_text_d, vgh_text_d, vgh_triplet_data, i_ar, doc_m_d, table_row_overlap_d):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        print lmdb_path2
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()
        #for k, v in txn_trip_default.cursor():
        #    print k, v
        
        import create_table_seq
        obj = create_table_seq.TableSeq()
        dtime   = str(datetime.datetime.now()).split('.')[0]
        for table_type, table_d in table_type_d.items():
            print table_type, table_d.keys()
            ijson['table_type'] = table_type
            ijson['ret']        = 'Y'
            #print table_d
            f_taxo_arr  = obj.create_seq_across_mt_taxo(ijson, {}, table_d)
            prev_id     = -1
            n_taxos_cnt     = 0
            for ii, dd in enumerate(f_taxo_arr):
                ks      = dd['ks']
                taxos   = dd['t_l']
                taxo    = ' / '.join(list(sets.Set(taxos))) 
                missing = 'Y'
                try:
                    taxo    = taxo.decode('utf-8')
                except:pass
                #print '\n===================================================='
                #print taxo
                n_taxos    = {}
                tmp_i_ar    = []
                for (table_id, c_id, ph, tlabel) in ks:
                    doc_id  = doc_m_d[table_id]
                    x       = txn_m.get('XMLID_'+c_id)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    if 1:#str(deal_id) == '220' and table_id == '1264':
                        triplet =  self.read_triplet(table_id, txn_trip_default, x, txn_trip_default)
                    else:
                        triplet =  self.read_triplet(table_id, txn_trip, x, txn_trip_default)
                    vgh_triplet_data[(table_id, x)] = triplet.get('VGH', {}).keys()
                    vgh_txt = vgh_triplet_data.get((table_id, x), []) 
                    if not vgh_txt:
                        #vgh_txt = [[binascii.a2b_hex(txn_m.get('COLTEXT_'+table_id+'_'+str(c_id.split('_')[2])))]]
                        print 'NO Triplet ', (table_id, x), [vgh_txt]
                        sys.exit()
                    vgh_txt = ' '.join(vgh_txt[0])
                    gcom    = ''
                    ngcom   = ''
                    if c_id in dd.get('gcom', {}):
                        gcom    = 'Y'
                    if c_id in dd.get('ngcom', {}):
                        ngcom    = 'Y'
                    try:
                        tlabel    = tlabel.decode('utf-8')
                    except:pass
                    try:
                        vgh_txt    = vgh_txt.decode('utf-8')
                    except:pass
                    if vgh_txt.lower() in vgh_text_d.get(table_type, {}):
                        vgh_id  = vgh_text_d[table_type][vgh_txt.lower()][0]
                    else:
                        vgh_id  = len(vgh_text_d.get(table_type, {}).keys())+1
                        vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                        new_vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                    #print '\t', [c_id, ph, tlabel, t]    
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    clean_value = t
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(t)
                    except:
                        clean_value = ''
                        pass
                    key     = table_id+'_'+self.get_quid(x)
                    ph_map  = txn.get('PH_MAP_'+str(key))
                    if ph_map and clean_value and len(clean_value.split('.')[0].strip('-')) > 2:
                            period_type, period, currency, scale, value_type    = ph_map.split('^')
                            #print (table_type, period_type+period, clean_value, value_type), table_row_overlap_d.get((table_type, period_type+period, clean_value, value_type), {})
                            n_taxos.update(table_row_overlap_d.get((table_type, period_type+period, clean_value, value_type), {}))
                    tmp_i_ar.append((g_id, g_id, table_type, taxo, '', 'I', table_id, c_id, ph, tlabel, gcom, ngcom, 'Y', vgh_id, doc_id, prev_id, x, 'SYSTEM', dtime))
                if n_taxos:
                    #print 'n_taxos ',len(n_taxos.keys()), n_taxos.keys()
                    if len(n_taxos.keys()) == 1:
                        n_id    = n_taxos.keys()[0]
                        t_ar    = []
                        tmpdd  = table_row_overlap_d[n_id]
                        for trr in tmp_i_ar:
                            ttlis   = list(trr)
                            ttlis[0]    = n_id
                            ttlis[1]    = tmpdd['order_id']
                            ttlis[3]    = tmpdd['t_l']
                            ttlis[4]    = tmpdd['u_label']
                            ttlis[5]    = tmpdd['missing']
                            t_ar.append(tuple(trr))
                        tmp_i_ar    =  t_ar
                    else:
                        n_taxos_cnt += 1
                else:   
                    n_taxos_cnt += 1
                i_ar    +=  tmp_i_ar
                g_id    += 1
                prev_id = ii+1
            print 'RES ', table_type, len(f_taxo_arr), n_taxos_cnt

    def show_triplet(self, table_id, txn1, xml, txn1_default):
        triplet = {}
        gv_tree = txn1.get(self.get_quid(table_id+'^!!^'+xml))
        if not gv_tree:
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml))
        if gv_tree:
            gv_tree = eval(gv_tree)
            triplet = gv_tree['triplet']
        dd  = {}
        for tk, tv in triplet.items():
            print '\n', tk
            for ii, tv1 in enumerate(tv):  
                print 
                for tv2 in tv1:
                    print '\t', tv2
        return dd
        

    
    def read_triplet(self, table_id, txn1, xml, txn1_default):
        triplet = {}
        flip    = txn1.get('FLIP_'+table_id)
        gv_tree = txn1.get(self.get_quid(table_id+'^!!^'+xml))
        if not gv_tree:
            flip    = txn1_default.get('FLIP_'+table_id)
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml))
        if gv_tree:
            gv_tree = eval(gv_tree)
            triplet = gv_tree['triplet']
        dd  = {}
        rev_map = {}
        if flip == 'Y':
            rev_map = {
                        'HGH':'VGH',
                        'VGH':'HGH',
                        }
        for tk, tv in triplet.items():
            if tk not in ['HRP']:continue
            for ii, tv1 in enumerate(tv):  
                for tv2 in tv1:
                    tflg    = ''
                    if tv2:
                        tflg    = tv2[0][2]
                        dd.setdefault(rev_map.get(tflg, tflg), {})[tuple(map(lambda x:' '.join(x[0].replace('\xc2\xa0', '').split()).strip(), tv2))] = 1
        return dd


    def get_order_phs(self, phs, dphs, report_map_d):
        doc_ph_d    = {}
        pk_map_d    = {}
        for ii, ph in enumerate(phs):
            pk_map_d[ph['k']]    = ph['dph']
            doc_id, table_id = ph['g'].split()[0].split('-')
            doc_ph_d.setdefault(doc_id, {}).setdefault(ph['n'], {})[ph['k']]   = 1
        dphs_ar         = report_year_sort.year_sort(dphs.keys())
        reported_phs    = {}
        for dph in dphs_ar:
            doc_ids = dphs[dph].keys()
            doc_ids.sort()
            d_ph    = dph[:-4]
            d_year  = dph[-4:]
            #print '\n+++++++++++++++++++++++++++++++++++'
            #print dph
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
                if f == 0:
                    for ph in doc_phs_ar:
                        t_ph    = ph[:-4]
                        t_year  = ph[-4:]
                        if d_year == t_year:
                            if d_ph in report_map_d[t_ph]:
                                if t_ph == 'H1':
                                    tmpph   = 'Q2'+t_year
                                    if tmpph not in reported_phs and (ph not in reported_phs):
                                        reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                elif t_ph == 'H2':
                                    tmpph   = 'Q4'+t_year
                                    if tmpph not in reported_phs and (ph not in reported_phs):
                                        reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                else:
                                    reported_phs.setdefault(ph, {}).update(doc_phs[ph]) 
                                #break
        fphs    = report_year_sort.year_sort(reported_phs.keys())
        phs = []
        for ph in fphs:
            tph = pk_map_d[reported_phs[ph].keys()[0]]
            phs.append({'k':reported_phs[ph].keys()[0], 'ph':ph, 'dph':tph})
        return phs, reported_phs

    def get_overlap_rows(self, grp):
        table_d     = {}
        table_cd    = {}
        rev_map     = {}
        value_types = grp.keys()
        new_grp     = {}
        value_types.sort(key=lambda x:len(x), reverse=True)
        if len(value_types) == 1:
            return value_types
        #print '\n',value_types
        done_grp    = {}
        for value_type in value_types:
            exist_grp   = {}
            v_set   = sets.Set(value_type)
            for v in done_grp.keys():
                if sets.Set(v).intersection(v_set):
                    exist_grp[v]    = 1
            if exist_grp:
                nv  = ()
                for e in exist_grp:
                    del done_grp[e]
                    nv  += e
                nv  += value_type
                nv  = list(sets.Set(nv))
                nv.sort()
                nv  = tuple(nv)
                done_grp[nv]    = 1
            else:
                done_grp[value_type]    = 1
        return done_grp.keys()

    def validate_pt_value(self, pt, pyear, key_value, f_phs_d, rr, ph_derived):
        def c_f_n(n):
            return float(self.convert_floating_point(n).replace(',', ''))
        opr     = {
                    '=' : 1,
                    '+' : 1,
                    '-' : 1,
                    '*' : 1,
                    '/' : 1,
                    '(' : 1,
                    ')' : 1,
                    '>' : 1,
                    '<' : 1,
                    ',' : 1,
                    ',' : 1,
                    }
        v_d     = {
                    'H2'    : [
                                'H2>max(FY,H1)',
                                'H2<c_f_n(Q3+Q4)',
                                ],
                    'Q4'    : [
                                'Q4>max(FY,Q1,Q2,Q3)',
                                'Q4>c_f_n(FY-Q1+Q2+Q3)',
                                ],
                    'H1'    : [
                                'H1>max(FY,H2)',
                                'H1<c_f_n(Q1+Q2)',
                                ],
                    'FY'    : [
                                    'FY<c_f_n(H1+Q3+Q4)',
                                    'FY<c_f_n(H1+H2)',
                                    'FY<c_f_n(Q1+Q2+H2)',
                                    'FY<c_f_n(Q1+Q2+Q3+Q4)'
                                ],
                    'Q2'    : [
                                    'Q2>c_f_n(H1-Q1)',
                                ],
                    'Q1'    : [
                                    'Q1>c_f_n(H1-Q2)',
                                ],
                    'Q4'    : [
                                    'Q4>c_f_n(H2-Q3)',
                                ],
                    'Q3'    : [
                                    'Q3>c_f_n(H2-Q4)',
                                ],
                        
                    }
        pt_d    = {
                    'FY':1,
                    'H1':1,
                    'H2':1,
                    'M9':1,
                    'Q1':1,
                    'Q2':1,
                    'Q3':1,
                    'Q4':1,
                    }
        for expr in v_d.get(pt, []):
            operands    = []
            tmp_ar      = []
            scal_ind    = {}
            scale_d     = {}
            for c in expr:
                #print '\t',[c, c in opr]
                if c in opr:
                    if tmp_ar:
                        t_pt = ''.join(tmp_ar)
                        if t_pt in pt_d:
                            phk = f_phs_d.get(t_pt+pyear, '')
                            v   = rr.get(phk, {}).get('v', '')
                            try:
                                v = numbercleanup_obj.get_value_cleanup(v)
                            except:
                                v = ''
                                pass
                            
                            if v == '':
                                v   = '0'
                            else:
                                s   = rr.get(phk, {}).get('phcsv', {}).get('s', '')
                                if s:
                                    scale_d[s]  = 1
                                    scal_ind[len(operands)] = s
                            if ph_derived == 'Y':
                                operands.append(str(self.convert_floating_point(abs(float(v))).replace(',', '')))
                            else:
                                operands.append(str(self.convert_floating_point(float(v)).replace(',', '')))
                        else:
                            operands.append(t_pt)
                    tmp_ar  = []
                    operands.append(c)
                elif c:
                    tmp_ar.append(c)
            if tmp_ar:
                t_pt = ''.join(tmp_ar)
                if t_pt in pt_d:
                    phk = f_phs_d.get(t_pt+pyear, '')
                    v   = rr.get(phk, {}).get('v', '')
                    try:
                        v = numbercleanup_obj.get_value_cleanup(v)
                    except:
                        v = ''
                        pass
                    if v == '':
                        v   = '0'
                    else:
                        s   = rr.get(phk, {}).get('phcsv', {}).get('s', '')
                        if s:
                            scale_d[s]  = 1
                            scal_ind[len(operands)] = s
                    if ph_derived == 'Y':
                        operands.append(str(self.convert_floating_point(abs(float(v))).replace(',', '')))
                    else:
                        operands.append(str(self.convert_floating_point(float(v)).replace(',', '')))
                else:
                    operands.append(t_pt)
            conver_arr  = []
            m_scale = ''
            if len(scale_d.keys()) > 1:
                num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
                scales  = map(lambda x:(num_obj[self.scale_map_d[x]], x), scale_d.keys())
                scales.sort(key=lambda x:x[0])
                m_scale = scales[-1][1]
            if m_scale:
                for ind, s in scal_ind.items():
                    if s != m_scale:
                        tv, nscale   = self.convert_frm_to(operands[ind], self.scale_map_d[s]+' - '+self.scale_map_d[m_scale], s)
                        conver_arr.append((operands[ind], tv, self.scale_map_d[s]+' - '+self.scale_map_d[m_scale]))
                        operands[ind]   = str(tv)
            tmp_ar      = []
            expr_str    = ''.join(operands)
            ot          = eval(expr_str)
            if ot == True:
                return ot, ' ('+expr+' == '+expr_str+')' +' -- '+str(conver_arr)
        return False, ''
            

    def get_deriv_phs(self, ijson):
        if ijson.get('pid', ''):
            company_name    = ijson['company_name']
            mnumber         = ijson['model_number']
            model_number    = mnumber
            deal_id         = ijson['deal_id']
            project_id      = ijson['project_id']
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql         = "select reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str from company_config where project_id=%s"%(ijson['pid'])
            cur.execute(sql)
            res = cur.fetchall()
            conn.close()
            deriv_phs   = {}
            for rr in res:
                reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str  = rr
                if periods:
                    for dph in periods.split('##'):
                        if dph.strip():
                            deriv_phs[dph]  = 1
            return  deriv_phs
        return {}
    def create_final_output(self, ijson):
        cinfo           = ijson.get('g_cinfo', {})
        deal_id         = str(ijson['deal_id'])
        report_pt       = ''
        report_year     = ''
        rsp_period      = cinfo.get('rsp_period', '')
        rsp_year        = cinfo.get('rsp_year', '')
        if rsp_period:
            report_pt       = rsp_period
        if rsp_year:
            report_year     = int(rsp_year)
        ijson['report_pt']  = report_pt
        ijson['report_year']  = report_year
        ijson_c = copy.deepcopy(ijson)
        if ijson.get('reported', '') == 'Y' or cinfo.get('reporting_type', '') == 'Reported':
            return self.create_final_output_with_ph(ijson_c, 'P', 'N')
        if ijson.get('project_name', '').lower() == 'schroders':
            return [{'message':'Error Not Valid'}]
            
        if cinfo.get('reporting_type', '') == 'Reported':
            return [{'message':'Error Not Valid'}]
        ijson_c = copy.deepcopy(ijson)
        res = self.create_final_output_with_ph(ijson_c, 'P')
        if res[0]['message'] != 'done':
            return res
        ijson_c = copy.deepcopy(ijson)
        res1 = self.create_final_output_with_ph(ijson_c, 'P-1')
        gen_type    = ijson.get('type','')
        ijson_c = copy.deepcopy(ijson)
        res1 = self.create_final_output_with_ph(ijson_c, 'P', 'N')
        return res

    def re_gen_all_final_output(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)

        sql = "select group_id, table_type, group_txt from vgh_group_info"
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[(table_type, str(group_id))]   = group_txt

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        sql         = "select row_id, table_type, gen_type, taxo_ids, group_text, datetime from final_output"
        cur.execute(sql)
        res = cur.fetchall()
        final_ar    = []
        sql         = "select table_type, vgh_id, group_txt, table_str from vgh_group_map"
        cur.execute(sql)
        tres        = cur.fetchall()
        vgh_grp     = {}
        for rr in tres:
            table_type, vgh_id, group_txt, table_str   = rr
            table_type  = str(table_type)
            group_txt   = str(group_txt)
            tc_d    = {}
            if table_str:
                for tcid in table_str.split('#'):
                    tc_d[tcid]  = 1
            vgh_grp.setdefault((table_type, group_txt), {})[vgh_id] = tc_d
        grp_ar_d    = {}
        for rr in res:
            row_id, table_type, gen_type, taxo_ids, group_text, tdatetime  = rr
            if '2019-01-10 ' not in tdatetime:continue
            grp_ar_d.setdefault(table_type, {})[group_text] = 1
        g_d = {}
        for table_type in grp_ar_d.keys():
            ijson['table_type'] = table_type
            g_ar    = self.create_group_ar(ijson, txn_m)
            for rr in g_ar:
                g_d[(table_type, rr['grpid'])] =  rr
        for rr in res:
            row_id, table_type, gen_type, taxo_ids, group_text, tdatetime  = rr
            if '2019-01-10 ' not in tdatetime:continue
            table_type  = str(table_type)
            group_text   = str(group_text)
            ijson_c   = copy.deepcopy(ijson)
            ijson_c['table_type']   = table_type
            ijson_c['type']         = gen_type
            ijson_c['row_id']         = row_id
            ijson_c['print']         = 'Y'
            if taxo_ids:
                ijson_c['t_ids']         = map(lambda x:int(x), taxo_ids.split(','))
            if gen_type == 'group' and  group_text:
                #print (table_type, group_text), (table_type, group_text) not in vgh_grp
                if (table_type, group_text) not in g_d:continue
                ijson_c['data']     = [g_d[(table_type, group_text)]['n']]
                ijson_c['grpid']    = group_text
                ijson_c['vids']     = g_d[(table_type, group_text)]['vids']
            #else:continue
            #print ijson_c
            final_ar.append(copy.deepcopy(ijson_c))
        conn.close()
        #sys.exit()
        txtpath      = '/var/www/html/DB_Model/%s/'%(company_name)
        os.system("mkdir -p "+txtpath)
        btxtpath      = '/var/www/html/DB_Model_backup/%s/'%(company_name)
        os.system("mkdir -p "+btxtpath)
        os.system('cp -r %s %s/%s'%(txtpath, btxtpath, datetime.datetime.now().strftime('%Y-%m-%d-%H_%M_%S')))
        os.system("rm -rf "+txtpath)
        row_error   = []
        for ii, ijson_c in enumerate(final_ar):
            if ijson_c['table_type'] in ['RBG', 'RBS']:continue
            print 'Running ', ii, ' / ', len(final_ar), [ijson_c['table_type'], ijson_c['type'], ijson_c.get('data', []), ijson_c.get('grpid', ''), ijson_c.get('vids', [])]

            tijson_c    = copy.deepcopy(ijson_c)
            res = self.create_final_output_with_ph(tijson_c, 'P')
            if res[0]['message'] != 'done':
                print 'Error ',res[0]['message']
                row_error.append((res[0]['message'], ijson_c['row_id']))
                continue
            tijson_c    = copy.deepcopy(ijson_c)
            res1 = self.create_final_output_with_ph(tijson_c, 'P-1')
        conn, cur   = conn_obj.sqlite_connection(db_file)
        cur.executemany('update final_output set error_txt=? where row_id=?', row_error)
        conn.commit()
        conn.close()
        self.store_bobox_data(ijson)
        import model_view.data_builder_new_excel as PH_LABEL
        obj = PH_LABEL.MRD_excel()
        res = obj.gen_output(ijson) 
        return res

    def gen_final_output(self, ijson, ret_flg=None):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)

        sql = "select group_id, table_type, group_txt from vgh_group_info"
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[(table_type, str(group_id))]   = group_txt

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        for col in ['rsp_period', 'rsp_year', 'rfr', 'restated_period']:
            try:
                sql = 'alter table company_config add column %s TEXT'%(col)
                cur.execute(sql)
            except:pass

        sql         = "select reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str,rsp_period, rsp_year, rfr, restated_period from company_config where project_name='%s'"%(ijson["project_name"])
        cur.execute(sql)
        res = cur.fetchall()
        final_ar    = []
        sql         = "select table_type, vgh_id, group_txt, table_str from vgh_group_map"
        cur.execute(sql)
        tres        = cur.fetchall()
        vgh_grp     = {}
        total_years = 10
        for rr in tres:
            table_type, vgh_id, group_txt, table_str   = rr
            table_type  = str(table_type)
            group_txt   = str(group_txt)
            tc_d    = {}
            if table_str:
                for tcid in table_str.split('#'):
                    tc_d[tcid]  = 1
            vgh_grp.setdefault((table_type, group_txt), {})[vgh_id] = tc_d
        grp_ar_d    = {}
        ar  = []
        deriv_phs   = {}
        cinfo       = {} 
        for rr in res:
            reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period  = rr
            if reporting_type == 'Reported':
                rfr, restated_period    ='',''
            cinfo   = {'reporting_type':reporting_type, 'total_years':total_years, 'derived_ph':derived_ph, 'periods':periods, 'rfr':rfr, 'restated_period':restated_period}
            total_years = int(total_years)
            if derived_ph:
                for dph in derived_ph.split('##'):
                    if dph.strip():
                        deriv_phs[dph]  = 1
                    
            for pc in excel_config_str.split('^!!^'):
                p, cs    = pc.split('$$')
                p_sp = p.split('@:@')
                text, group_id, tmpy, ttype = p_sp[:4]
                if tmpy == '0' and ttype:
                    if group_id:
                        ar.append((1, ttype, 'group', '', group_id, ''))
                        grp_ar_d.setdefault(ttype, {})[group_id] = 1
                    else:
                        ar.append((1, ttype, 'all', '', '', ''))
                for c in cs.split('@@'):
                    if not c:continue
                    c_sp    = c.split('^^')
                    group_id, table_type    = c_sp[:2]
                    if group_id:
                        ar.append((1, table_type, 'group', '', group_id, ''))
                        grp_ar_d.setdefault(table_type, {})[group_id] = 1
                    else:
                        ar.append((1, table_type, 'all', '', group_id, ''))
            
        g_d = {}
        for table_type in grp_ar_d.keys():
            ijson['table_type'] = table_type
            g_ar    = self.create_group_ar(ijson, txn_m)
            for rr in g_ar:
                g_d[(table_type, rr['grpid'])] =  rr
        sql = "select group_id, table_type, group_txt from vgh_group_info where table_type like 'HGHGROUP%'"
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        for rr in res:
            group_id, table_type, group_txt = rr
            g_d[(table_type, group_id)] = group_txt
        ijson['deriv_phs']  = deriv_phs
        report_pt       = ''
        report_year     = ''
        if rsp_period:
            report_pt       = rsp_period
        if rsp_year:
            report_year     = int(rsp_year)
        ijson['report_pt']  = report_pt
        ijson['report_year']    = report_year
        ijson['year']           = total_years
        ijson['g_cinfo']        = cinfo
        for rr in ar:
            row_id, table_type, gen_type, taxo_ids, group_text, tdatetime  = rr
            table_type  = str(table_type)
            group_text   = str(group_text)
            ijson_c   = copy.deepcopy(ijson)
            ijson_c['table_type']   = table_type
            ijson_c['type']         = gen_type
            ijson_c['row_id']         = [table_type, gen_type, group_text]
            #ijson_c['print']         = 'Y'
            if taxo_ids:
                ijson_c['t_ids']         = map(lambda x:int(x), taxo_ids.split(','))
            if gen_type == 'group' and  group_text:
                #print (table_type, group_text), (table_type, group_text) not in vgh_grp
                if (table_type, group_text) not in g_d:continue
                ijson_c['data']     = [g_d[(table_type, group_text)]['n']]
                ijson_c['grpid']    = group_text
                ijson_c['vids']     = g_d[(table_type, group_text)]['vids']
            #else:continue
            #print ijson_c
            final_ar.append(copy.deepcopy(ijson_c))
        conn.close()
        if ret_flg == 'Y':
            return final_ar
        #sys.exit()
        txtpath      = '/var/www/html/DB_Model_Missing/%s/'%(company_name)
        os.system("rm -rf "+txtpath)
        txtpath      = '/var/www/html/DB_Model_Reported_Missing/%s/'%(company_name)
        os.system("rm -rf "+txtpath)
        txtpath      = '/var/www/html/DB_Model/%s/'%(company_name)
        os.system("mkdir -p "+txtpath)


        btxtpath      = '/var/www/html/DB_Model_backup/%s/'%(company_name)
        os.system("mkdir -p "+btxtpath)
        os.system('cp -r %s %s/%s'%(txtpath, btxtpath, datetime.datetime.now().strftime('%Y-%m-%d-%H_%M_%S')))
        os.system("rm -rf "+txtpath)


        txtpath      = '/var/www/html/DB_Model_Reported/%s/'%(company_name)
        btxtpath      = '/var/www/html/DB_Model_Reported_backup/%s/'%(company_name)
        os.system("mkdir -p "+btxtpath)
        os.system('cp -r %s %s/%s'%(txtpath, btxtpath, datetime.datetime.now().strftime('%Y-%m-%d-%H_%M_%S')))
        os.system("rm -rf "+txtpath)
        row_error   = []
        if ijson.get('PRINT', '') != 'Y':
            disableprint()
        for ii, ijson_c in enumerate(final_ar):
            #if ijson_c['table_type'] in ['RBG', 'RBS']:continue
            print 'Running ', ii, ' / ', len(final_ar), [ijson_c['table_type'], ijson_c['type'], ijson_c.get('data', []), ijson_c.get('grpid', '')]
            #print json.dumps(ijson_c)
            if reporting_type == 'Reported':#ijson.get('project_name', '').lower() == 'schroders':
                tijson_c    = copy.deepcopy(ijson_c)
                res = self.create_final_output_with_ph(tijson_c, 'P', 'N')
                if res[0]['message'] != 'done':
                    #print 'Error ',res[0]['message']
                    row_error.append({'error':res[0]['message'], 'table_type':ijson_c['table_type'], 'type':ijson_c['type'], 'grpid':ijson_c.get('grpid', ''), 'vids':ijson_c.get('vids', []), 'data':ijson_c.get('data', [])})
                continue

            if reporting_type in ['Restated', 'Both']:
                tijson_c    = copy.deepcopy(ijson_c)
                res = self.create_final_output_with_ph(tijson_c, 'P')
                if res[0]['message'] != 'done':
                    #print 'Error ',res[0]['message']
                    row_error.append({'error':res[0]['message'], 'table_type':ijson_c['table_type'], 'type':ijson_c['type'], 'grpid':ijson_c.get('grpid', ''), 'vids':ijson_c.get('vids', []), 'data':ijson_c.get('data', [])})
                    #continue
                tijson_c    = copy.deepcopy(ijson_c)
                res1 = self.create_final_output_with_ph(tijson_c, 'P-1')
            if reporting_type in ['Both']:
                tijson_c    = copy.deepcopy(ijson_c)
                self.create_final_output_with_ph(tijson_c, 'P', 'N')
        if ijson.get('PRINT', '') == 'Y':
            print 'DONE TEXT'
        enableprint()
        #if row_error:
        #    res = [{"message":"Error", 'data':row_error}]
        #    return res
        self.store_bobox_data(ijson)
        import model_view.data_builder_new_excel_pr3 as PH_LABEL
        obj = PH_LABEL.MRD_excel()
        try:
            res = obj.gen_output(ijson)
        except:
            res = [{"message":"Error while Creating Excel"}]
        res = json.loads(res)
        res[0]['DB_ERR']    = row_error
        return res



    def gen_taxonomy(self, txt):
        alpha      = {'a':1, 'b':1, 'c':1, 'd':1, 'e':1, 'f':1, 'g':1, 'h':1, 'i':1, 'j':1, 'k':1, 'l':1, 'm':1, 'n':1, 'o':1, 'p':1, 'q':1, 'r':1, 's':1, 't':1 ,'u':1, 'v':1, 'w':1, 'x':1, 'y':1, 'z':1}
        tmp_ar  = []
        for c in txt:
            if c.lower() in alpha:
                tmp_ar.append(c)
        return ''.join(tmp_ar)

    def gen_taxonomy_alpha_num(self, txt):
        alpha      = {'a':1, 'b':1, 'c':1, 'd':1, 'e':1, 'f':1, 'g':1, 'h':1, 'i':1, 'j':1, 'k':1, 'l':1, 'm':1, 'n':1, 'o':1, 'p':1, 'q':1, 'r':1, 's':1, 't':1 ,'u':1, 'v':1, 'w':1, 'x':1, 'y':1, 'z':1, '0':1,'1':1,'2':1,'3':1,'4':1, '5':1,'6':1,'7':1, '8':1, '9':1}
        tmp_ar  = []
        for c in txt:
            if c.lower() in alpha:
                tmp_ar.append(c)
        return ''.join(tmp_ar)


    def flip_sign(self, t_res, ph_formula_d):
        t_res, t_data, t_f_phs, t_f_phs_d, taxo_value_grp    = t_res
        flip_d  = {}
        for ti, rr in enumerate(t_data[:]):
            t_row_f     = ph_formula_d.get(('F', str(rr['t_id'])), ())
            if not t_row_f:
                t_row_f     = ph_formula_d.get(('SYS F', str(rr['t_id'])), ())
            if not t_row_f:continue
            for ii, ph in enumerate(t_f_phs):
                if ph['k'] not in t_data[ti]:continue
                v_d     = t_data[ti][ph['k']]
                rest_ar = v_d.get('rest_ar', [])
                if not rest_ar:continue
                v_d   = v_d['org_d']
                o_v     = v_d['v']
                try:
                    o_v = numbercleanup_obj.get_value_cleanup(o_v)
                except:
                    o_v = ''
                if o_v == '' or float(o_v) in [0.0, -0.0]:continue
                sign    = '+'   
                if '-' in o_v:
                    sign    = '-'   
                else:
                    continue
                ks  = []
                for key in rest_ar:
                    if key not in t_data[ti]:continue
                    to_v     = t_data[ti][key]['v']
                    try:
                        to_v = numbercleanup_obj.get_value_cleanup(to_v)
                    except:
                        to_v = ''
                    if to_v == '' or float(to_v) in [0.0, -0.0]:continue
                    if '-' in to_v:
                        continue
                    #flip_d.setdefault(ti, {})[key]  = sign
                    self.flip_sign_foroperands(t_row_f[1], key, sign, ph_formula_d, flip_d, taxo_value_grp, t_data, ks)
                if ks:
                    #print '\n=========================================='
                    #print rr['t_l']
                    #print key
                    #print [v_d['t'], v_d['v'], v_d['phcsv']['pt']+v_d['phcsv']['p']], rest_ar
                    for (key, n_d, tmpti, t_l) in ks:
                        #print '\n\t', key, [t_l]
                        #print '\t', [n_d['t'], n_d['v'], n_d['phcsv']['pt']+n_d['phcsv']['p']]
                        flip_d.setdefault(tmpti, {})[key]  = sign
                        flip_d.setdefault(ti, {})[key]  = sign
        return flip_d


    def flip_sign_foroperands(self, operands, key, sign, ph_formula_d, flip_d, taxo_value_grp, t_data, ks):
        t_ops   = []
        for ft in operands:
            if ft['op'] != '=':
                t_ops.append(ft)
        done_d  = {}
        while operands:
            tmp_operands    = []
            for ft in operands:
                    if ft['type'] == 'v':
                        dd  = {}
                        dd['clean_value']   = ft['txid']
                    else:
                        ti  = taxo_value_grp[ft['txid']]
                        if ti in done_d:continue
                        done_d[ti]  =   1
                        #flip_d.setdefault(ti, {})[key]  = sign
                        t_row_f     = ph_formula_d.get(('F', str(ft['txid'])), ())
                        if not t_row_f:
                            t_row_f     = ph_formula_d.get(('SYS F', str(ft['txid'])), ())
                        if t_row_f:
                            tmp_operands    += t_row_f[1]
                        if key not in t_data[ti]:continue
                        n_d     = t_data[ti][key]
                        if 'org_d' in n_d:
                            n_d = n_d['org_d']
                        n_v     = n_d['v']
                        try:
                            n_v = numbercleanup_obj.get_value_cleanup(n_v)
                        except:
                            n_v = ''
                        if n_v == '' or float(n_v) in [0.0, -0.0]:continue
                        n_sign    = '+'   
                        if '-' in n_v:
                            n_sign    = '-'   
                        if n_sign != sign:
                            ks.append((key, n_d, ti, t_data[ti]['t_l']))
            operands    = tmp_operands

    def get_restated_value(self, res, ph_formula_d, t_ids, txn_m, mat_date, txn, validate_op, actual_v_d, dphs, ijson, ph_f_map, years, ph_gid_map, y, key_map_rev, key_map_rev_t, disp_name, table_type, group_id, doc_d, restated_model):
        phs                 = copy.deepcopy(res[0]['phs'])
        data                = copy.deepcopy(res[0]['data'])
        rc_d                = {}
        consider_ks         = {}
        consider_table      = {}
        consider_rows       = {}
        xml_row_map         = {}
        ph_kindex           = {}
        for ii, ph in enumerate(phs):
            ph_kindex[ph['k']]  = ii
        duplicate_taxonomy  = {}
        phk_data            = {}
        phk_ind_data        = {}
        validation_error    = {}
        #print validate_op
        clean_v_d       = {}
        ph_ti               = {}
        taxo_value_grp  = {}
        taxo_id_dict    = {}
        csv_d   = {}
        xml_col_map = {}
        row                 = 1
        row_ind_map         = {}
        taxo_unq_d          = {}
        table_xml_d         = {}
        more_than_one_scale = {}
        for ti, rr in enumerate(data[:]):
            taxo_id_dict[str(rr['t_id'])]    = rr
            taxo_value_grp[str(rr['t_id'])]  = ti
            t_s_f       = ph_formula_d.get(('S', str(rr['t_id'])), {})
            if t_ids and rr['t_id'] not in t_ids:continue
            taxo    = rr.get('taxo', '')
            tmptaxo = taxo.split(' / ')
            f_taxo  = {}
            for tt in tmptaxo:
                for t1 in tt.split('@'):
                    if 'TASTAXO_' in t1 or 'TAS_TAXO' in t1:continue
                    if ' - ' in t1:
                        f_taxo[t1.split(' - ')[1]]  = 1
                    else:
                        f_taxo[t1]  = 1
            f_taxo  = f_taxo.keys()
            f_taxo.sort()
            f_taxo  = ' / '.join(f_taxo[:1])
            tf_taxo    = self.gen_taxonomy_alpha_num(rr['t_l'])
            if not f_taxo:
                f_taxo    = self.gen_taxonomy(rr['t_l'])
            if not f_taxo:
                f_taxo    = rr['t_l']
            if not tf_taxo:
                tf_taxo    = rr['t_l']
            duplicate_taxonomy.setdefault(tf_taxo, {})[rr['t_id']]   = rr['t_l']
            if f_taxo not in taxo_unq_d:
                taxo_unq_d[f_taxo]  = 1
            else:
                ol_cnt  = taxo_unq_d[f_taxo]
                taxo_unq_d[f_taxo]  = ol_cnt+1
                f_taxo  += '-'+str(ol_cnt)
            rr['f_taxo']      = f_taxo
            res[0]['data'][ti]['f_taxo']      = f_taxo
            row += 1
            row_ind_map[ti] = row
            scale_d         = {}
            xml_col_map.setdefault(ti, {})
            for ii, ph in enumerate(phs):
                if ph['k'] not in rr:continue
                v_d         = rr[ph['k']]
                t           = v_d['v']
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                if not clean_value and v_d['t'] in actual_v_d:
                    clean_value = v_d['v']
                x_c    = self.clean_xmls(v_d['x'])
                if x_c:
                    table_id    = v_d['t']
                    x_c    = x_c.split(':@:')[0].split('#')[0]
                    table_xml_d.setdefault(table_id, {})[(int(x_c.split('_')[1]), int(x_c.split('_')[0].strip('x')))]   = 1
                tk   = self.get_quid(v_d['t']+'_'+v_d['x'])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if c_id:
                    ktup    = (rr['t_id'], v_d['t'], int(c_id.split('_')[1]))
                    if ktup in mat_date:
                        v_d['mdate']    = mat_date[ktup][2]
                        res[0]['data'][ti][ph['k']]['mdate']    = mat_date[ktup][2]
                        v_d.setdefault('phcsv', {})['mdate']    =  mat_date[ktup][2]
                        res[0]['data'][ti][ph['k']].setdefault('phcsv', {})['mdate']    = mat_date[ktup][2]
                xml_row_map.setdefault((v_d['t'], v_d['x']), {})[ti]    = 1
                xml_col_map.setdefault(ti, {})[(v_d['t'], v_d['x'])]         = ph['k']
                key     = v_d['t']+'_'+self.get_quid(v_d['x'])
                ph_map  = txn.get('PH_MAP_'+str(key))
                if ph_map:
                    period_type, period, currency, scale, value_type    = ph_map.split('^')
                if ph['k'] in t_s_f and clean_value:
                    sgrpid, form    = t_s_f[ph['k']]
                    v, nscale   = self.convert_frm_to(clean_value, form, scale)
                    if v:
                        v   =str(v)
                        clean_value = v
                        rr[ph['k']]['expr_str']   = form+'('+str(rr[ph['k']]['v'])+') = '+str(v)
                        rr[ph['k']]['v']    = v
                        rr[ph['k']]['fv']   = 'Y'
                        res[0]['data'][ti][ph['k']]['expr_str']   = form+'('+str(rr[ph['k']]['v'])+') = '+str(v)
                        res[0]['data'][ti][ph['k']]['v']    = v
                        res[0]['data'][ti][ph['k']]['fv']   = 'Y'
                        csv_d[(v_d['t'], v_d['x'])]    = (period_type, period, currency, self.scale_map_rev_d[nscale], value_type)
                if (v_d['t'], v_d['x']) not in csv_d:
                    period_type, period, currency, scale, value_type    = v_d['phcsv']['p'], v_d['phcsv']['pt'], v_d['phcsv']['c'], v_d['phcsv']['s'], v_d['phcsv']['vt']
                    csv_d[(v_d['t'], v_d['x'])]    = (period_type, period, currency, scale, value_type)
                if v_d['v'].strip():
                    scale_d.setdefault(csv_d[(v_d['t'], v_d['x'])][3], {})[v_d['rid']]         = 'N'
                        
                clean_v_d.setdefault(ti, {})[ph['k']]   = clean_value
                ph_ti.setdefault(ph['n'], {})[ti]  = ph['k']
                if clean_value == '':continue
                phk_data.setdefault(ph['k'], []).append((ti, clean_value))
                phk_ind_data.setdefault(ph['k'], {})[ti]    = clean_value
                if not t_ids or rr['t_id'] in t_ids:
                    if validate_op.get(rr['t_id'], {}).get(ph['k'], ''):
                        validation_error.setdefault(rr['t_id'], {})[ph['k']]    = validate_op[str(rr['t_id'])][ph['k']]
            if len(scale_d.keys()) > 1:
                more_than_one_scale[rr['t_id']] = {}
                for sc, rid in scale_d.items():
                    more_than_one_scale[rr['t_id']].update(rid)
        if validation_error:
            res = [{'message':'Error Validation failed', 'data':validation_error}]
            return res
        if 0:#more_than_one_scale:
            res = [{'message':'Error More than one scale', 'data':more_than_one_scale}]
            return res

        if 0:#duplicate_taxonomy:# and gen_type != 'display' :
            tmp_d = {}
            for f_taxo_id, tid_dict in duplicate_taxonomy.items():
                if len(tid_dict.keys()) > 1:
                    tmp_d.update(tid_dict)
            if tmp_d:
                res = [{'message':'Error Duplicate taxonomy', 'data':tmp_d}]
                return res
                    
            
        
        f_phs, reported_phs               = self.get_order_phs(phs, dphs, self.report_map_d)
        dphs_all         = report_year_sort.year_sort(dphs.keys())
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
        col_m               = self.get_column_map(ph_kindex, reported_phs, f_phs, phk_ind_data, data, ph_formula_d, taxo_value_grp)
        tt_ph_f             = self.read_ph_config_formula(ijson)
        
        n_phs   = {}
        #if 1:
        if ijson.get('print', '') == 'Y':
            print 'col_m ', col_m
        #sys.exit()
        flip_d  = {}
        f_phs, f_phs_d, n_phs, f_k_d, new_phs, missing_value, t_op_d  = self.calculate_derivative_values(data, res, ph_formula_d, clean_v_d, col_m, taxo_value_grp, ph_ti, f_phs, f_phs_d, ph_f_map, years, done_ph, ph_gid_map, y, ph_map_d, t_ids, xml_row_map, xml_col_map, txn, taxo_id_dict, key_map_rev, key_map_rev_t, disp_name, table_type, group_id, row_ind_map, csv_d, doc_d, dphs_all, restated_model, phk_ind_data, reported_phs, tt_ph_f, 'Y', flip_d, {}, ijson)    

        return (res, data, f_phs, f_phs_d, taxo_value_grp)

    def calculate_derivative_values(self, data, res, ph_formula_d, clean_v_d, col_m, taxo_value_grp, ph_ti, f_phs, f_phs_d, ph_f_map, years, done_ph, ph_gid_map, y, ph_map_d, t_ids, xml_row_map, xml_col_map, txn, taxo_id_dict, key_map_rev, key_map_rev_t, disp_name, table_type, group_id, row_ind_map, csv_d, doc_d, dphs, restated_model, phk_ind_data, reported_phs, tt_ph_f, ret_flg, flip_d, actual_v_d, ijson):
        cinfo           = ijson.get('g_cinfo', {})
        if tt_ph_f:
            remain_phs  = tt_ph_f.keys()
        else:
            remain_phs = ph_f_map.keys()
        n_d     = {}
        new_phs = []
        n_y     = str(int(y)+1)
        for ph in remain_phs:
            for year in years.keys():
                tph  = ph+year
                if tph in done_ph or tph in n_d:continue
                if 'Q4'+y == tph:
                        if 'FY'+y not in f_phs_d:continue
                elif 'Q4'+n_y == tph:
                    if 'Q3'+n_y not in f_phs_d:continue
                elif 'H2'+n_y == tph:
                    if 'Q3'+n_y not in f_phs_d and 'FY'+n_y not in f_phs_d:continue
                n_d[tph]             =1
                new_phs.append({'k':tph, 'ph':tph, 'new':'Y', 'dph':(tph[:-4], int(tph[-4:]))})
        pk_map_d        = {}
        pk_map_rev_d    = {}
        latest_year     = {}
        if new_phs:
            ph_map_d    = {}
            #for k, v in f_phs_d.items():
            #    print k, v
            for ph in f_phs+new_phs:
                ph_map_d[ph['ph']]   = ph
                f_phs_d[ph['ph']]   = ph['k']
                done_ph[ph['ph']]  = 1
                pk_map_d[ph['k']]    = ph['dph']
                #if ph.get('new', '') != 'Y':
                #    pk_map_rev_d[ph['dph']] = ph['k']
                latest_year[ph['dph'][1]]     = 1
            #print f_phs_d.keys()
            #print n_phs.keys()
            #if 'FY'+y not in n_d and 'Q4'+y in n_d:
            #    del ph_map_d['Q4'+y]
            #    del f_phs_d['Q4'+y]
            dphs_ar             = report_year_sort.year_sort(f_phs_d.keys())
            dphs_ar.reverse()
            f_phs               = map(lambda x:ph_map_d[x], dphs_ar)
        rem_phs  = {}
        for ph in res[0]['phs']:
            pk_map_d[ph['k']]    = ph['dph']
            pk_map_rev_d[ph['dph']] = ph['k']
            latest_year[ph['dph'][1]]     = 1
                
            if ph['k'] not in phk_ind_data:continue
            if ph['n'] not in f_phs_d:
                rem_phs[ph['n']]    = 1
        latest_year = latest_year.keys()
        latest_year.sort()
        latest_year = latest_year[-1]
        n_phs   = {}
        cfgrps, cf_all_grps, grps, all_grps, d_all_grps = {}, {}, {}, {}, {}
        f_k_d   = {}
        missing_value   = {'PKS':{}}
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            #print '\n============================'
            #print [ti, rr['t_l']]
            
            key_value           = clean_v_d.get(ti, {})
            for ph in f_phs:
                #print ph, ph['k'] not in rr, clean_v_d.get(ti, {}).get(ph['k'], '') == '', col_m.get(ph['k'], {})
                if (ph['k'] not in rr or clean_v_d.get(ti, {}).get(ph['k'], '') == '') and (ti in col_m.get(ph['k'], {})):
                    #print '\tYES', col_m[ph['k']][ti], col_m[ph['k']][ti] in rr
                    if col_m[ph['k']][ti] in rr:
                        clean_v_d.setdefault(ti, {})[ph['k']]   = clean_v_d[ti][col_m[ph['k']][ti]]
                        rr[ph['k']] = rr[col_m[ph['k']][ti]]
                        res[0]['data'][ti][ph['k']]    = rr[col_m[ph['k']][ti]]
                if (ph['k'] not in rr or clean_v_d.get(ti, {}).get(ph['k'], '') == ''):
                    phks    = filter(lambda x:x!=ph['k'], reported_phs.get(ph['ph'], {}).keys())
                    for phk in phks:
                        if (phk in rr or clean_v_d.get(ti, {}).get(phk, '') != ''):
                            missing_value.setdefault(rr['t_id'], {})[phk]   = 1
                            missing_value['PKS'][phk]   = 1
                    

            if restated_model == 'Y':
                restated_year   = ''
                if cinfo.get('restated_period', '') not in ['', 'Latest', None]:
                    restated_year   = int(cinfo['restated_period'].split('P+')[1])
                ph_value_d  = {}
                for ii, ph in enumerate(res[0]['phs']):
                    if ph['k'] not in rr:continue
                    clean_value = clean_v_d[ti][ph['k']]
                    sign        = flip_d.get(ti, {}).get(ph['k'], '')
                    if sign and str(rr[ph['k']]['t']) not in actual_v_d:
                        #print '\n============================'
                        #print [ti, rr['t_l'], ph, clean_value, sign]
                        if clean_value:
                            if float(clean_value) not in [0.0, -0.0]:
                                if sign == '+' and '-' in clean_value:
                                    clean_value = clean_value.strip('-')
                                    clean_v_d[ti][ph['k']]  = clean_value
                                    data[ti][ph['k']]['v']       = clean_value
                                    res[0]['data'][ti][ph['k']]['v'] = clean_value
                                elif sign == '-' and '-' not in clean_value:
                                    clean_value = '-'+clean_value
                                    clean_v_d[ti][ph['k']]  = clean_value
                                    data[ti][ph['k']]['v']       = clean_value
                                    res[0]['data'][ti][ph['k']]['v'] = clean_value
                            pass
                    ph_value_d.setdefault(ph['n'], {}).setdefault(clean_value, {})[ph['k']]          = 1
                #print '\n============================'
                #print rr['t_l']
                #print ph_value_d
                for ii, ph in enumerate(f_phs):
                    if ph['ph'] not in dphs:continue
                    trs_value   = []
                    c_ph, c_year      = pk_map_d[ph['k']]
                    if ph['k'] not in rr or clean_v_d[ti].get(ph['k'], '') == '':
                        other_values            = ph_value_d.get(ph['ph'], {});
                        for v in other_values.keys():
                            if 1:#v and v != clean_v_d[ti].get(ph['k'], ''):
                                for key in other_values[v].keys():
                                    if key == ph['k']:continue
                                    if v == '' and rr.get(ph['k'], {'v':''})['v'] == '' and rr[key]['v'] == '':continue
                                    #print '\t',[ph['k'], v, (c_ph, c_year+restated_year),  pk_map_d[key]]
                                    if restated_year:
                                        if ((c_ph, c_year+restated_year) == pk_map_d[key]):
                                            trs_value.append(key)
                                    else:
                                            trs_value.append(key)
                    else:
                        v_d = rr[ph['k']]
                        other_values            = ph_value_d.get(ph['ph'], {})
                        clean_value             = clean_v_d[ti][ph['k']]
                        for v in other_values.keys():
                            if 1:#v and v != clean_v_d[ti][ph['k']]:
                                for key in other_values[v].keys():
                                    if key == ph['k']:continue
                                    if v == '' and rr[ph['k']]['v'] == '' and rr[key]['v'] == '':continue
                                    if restated_year:
                                        if ((c_ph, c_year+restated_year) == pk_map_d[key]): 
                                            if  rr[key]['t'] != v_d['t']:
                                                trs_value.append(key)
                                    else:
                                            if  rr[key]['t'] != v_d['t']:
                                                trs_value.append(key)
                    trs_value.sort(key=lambda x:(dphs.index(doc_d[rr[x]['d']][0]), rr[x]['d']))
                    if trs_value:
                        prev_v      =  copy.deepcopy(rr.get(ph['k'], {'v':'','x':'','bbox':[], 'd':'', 't':'', 'phcsv':{'c':'', 'vt':'','s':'','p':'','pt':''}}))
                        rr[ph['k']] = copy.deepcopy(rr[trs_value[-1]])
                        rr[ph['k']]['rest_ar']  = trs_value[:]
                        rr[ph['k']]['org_d']    = prev_v
                        rr[ph['k']]['x'] = rr[ph['k']]['x']
                        #csv_d.setdefault(ti, {})[ph['k']]   = csv_d[ti][trs_value[-1]]
                        clean_v_d[ti][ph['k']]   = clean_v_d[ti][trs_value[-1]]
                        res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
                        #print 'RESTATED_ ', [rr['t_l'], ph, ph['ph'] in dphs, trs_value]
                    elif cinfo.get('rfr', '') != 'Y'  and ph['k'] in rr:
                        if restated_year:
                            #print (restated_year, latest_year, c_year+restated_year, (c_ph, c_year+restated_year), (c_ph, c_year+restated_year)  not in pk_map_rev_d)
                            if latest_year <= c_year+restated_year and ((c_ph, c_year+restated_year)  not in pk_map_rev_d):
                                continue
                        elif cinfo.get('restated_period', '') == 'Latest':
                            if c_year+1 == latest_year and ((c_ph, latest_year)  not in pk_map_rev_d):
                                continue
                            elif c_year == latest_year and ((c_ph, latest_year+1)  not in pk_map_rev_d):
                                continue

                        rr[ph['k']]['x']        = rr[ph['k']]['x']
                        rr[ph['k']]['v']        = ''
                        clean_v_d[ti][ph['k']]  = ''
                        res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        t_op_d  = {}
        if ret_flg  == 'Y':
            return f_phs, f_phs_d, n_phs, f_k_d, new_phs, missing_value, t_op_d

        if ijson.get('project_name', '').lower() != 'schroders':
            self.formula_derivations(data, res, f_phs,ph_formula_d, xml_row_map, xml_col_map, txn, t_ids, taxo_id_dict, taxo_value_grp, clean_v_d, f_k_d, key_map_rev, key_map_rev_t, disp_name, row_ind_map, csv_d, 'V' )    
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            t_ph_f      = ph_formula_d.get(str(rr['t_id']), {})
            t_cf_f       = ph_formula_d.get(('CF', str(rr['t_id'])), {})
            t_cfs_f     = ph_formula_d.get(('CF_F', str(rr['t_id'])), ())
            if tt_ph_f:
                t_ph_f  = {}
                for ii, ph in enumerate(f_phs):
                    if ph['k'] not in rr or not key_value.get(ph['k'], ''):
                        if ph['ph'][:-4] in tt_ph_f:
                            t_ph_f[ph['k']] = tt_ph_f[ph['ph'][:-4]]
                            t_ph_f[('rid', ph['k'])] = 'RID-0'
            #t_s_f       = ph_formula_d.get(('S', str(rr['t_id'])), {})
            #print '\n==============================='
            #print rr['t_l'], t_ph_f
            key_value           = clean_v_d.get(ti, {})
            #print '\toperand_rows ',operand_rows
            for ph in f_phs:
                #if (ph['k'] not in rr or clean_v_d.get(ti, {}).get(ph['k'], '') == '') and (ti in col_m.get(ph['k'], {})):
                #    if col_m[ph['k']][ti] in rr:
                #        clean_v_d.setdefault(ti, {})[ph['k']]   = clean_v_d[ti][col_m[ph['k']][ti]]
                #        rr[ph['k']] = rr[col_m[ph['k']][ti]]
                #        res[0]['data'][ti][ph['k']]    = rr[col_m[ph['k']][ti]]
                if ph['k'] not in rr or not clean_v_d.get(ti, {}).get(ph['k'], ''):
                    f   = self.calculate_user_ph_values(ph, rr, ti, t_cf_f, t_ph_f, t_cfs_f, taxo_value_grp, ph_ti, res, data, f_phs_d, key_value, ph_map_d, cfgrps, cf_all_grps, grps, all_grps, n_phs, ph_f_map, ph_gid_map, d_all_grps, taxo_id_dict, 0)
                            
            #for ph in ph_f_map.keys():
            #    for year in years.keys():
            #        if ph+year in done_ph:continue
            #        f   = 0
            #        tmpph   = ph+year
            #        pd_d    = {'k':tmpph, 'ph':tmpph}
            #        f   = self.calculate_user_ph_values(pd_d, rr, ti, t_cf_f, t_ph_f, t_cfs_f, taxo_value_grp, ph_ti, res, data, f_phs_d, key_value, ph_map_d, cfgrps, cf_all_grps, grps, all_grps, n_phs, ph_f_map, ph_gid_map, d_all_grps,taxo_id_dict, 0)
            #        #print 'f ', f, rr.get(pd_d['k'], {}).get('v')
                    
        #sys.exit()

        
        if 0:#n_phs:
            if 'FY'+y not in n_phs and 'Q4'+y in n_phs:
                del n_phs['Q4'+y]
            ph_map_d    = {}
            for ph in f_phs:
                ph_map_d[ph['ph']]   = ph
            for k, v in n_phs.items():
                ph_map_d[k] = {'g':'NEW-'+k,'k':k, 'ph':k}
                f_phs_d[k]   = k
            dphs_ar             = report_year_sort.year_sort(f_phs_d.keys())
            dphs_ar.reverse()
            #print dphs_ar
            f_phs   = map(lambda x:ph_map_d[x], dphs_ar)
        t_op_d   = self.formula_derivations(data, res, f_phs,ph_formula_d, xml_row_map, xml_col_map, txn, t_ids, taxo_id_dict, taxo_value_grp, clean_v_d, f_k_d, key_map_rev, key_map_rev_t, disp_name, row_ind_map, csv_d, 'F')    
        return f_phs, f_phs_d, n_phs, f_k_d, new_phs, missing_value, t_op_d


    def form_fcol_from_sys_formula(self, ti, operand_rows, row_op, row_ind_map, data, ph):
        formula = []
        d_formula   = []
        #if user_f == None:
        row_op[ti]  = {'=':1}
        for ri, rowtup in enumerate(operand_rows):
            tmp_arr = []
            d_tmp_arr = []
            operands    = []
            operand_ids    = []
            rowtup  =(ti, )+rowtup
            for rid in rowtup:
                if rid != ti:
                    row_op[rid]  = {'+':1}
                rv_d    = data[rid].get(ph['k'], {})
                if rv_d:
                    t       = rv_d['v']
                    clean_value = t
                    try:
                        clean_value = float(numbercleanup_obj.get_value_cleanup(t))
                    except:
                        clean_value = 0.00
                        pass
                    if 1:
                        operand_ids.append(rid)
                        operands.append(clean_value)
            if len(operand_ids) > 1 and operand_ids[0] == ti:
                tres = gcom_operator.check_formula_specific(operands[1:], operands[0])
                if not tres:continue
                
                for i, t_r in enumerate(tres):
                    row_op[operand_ids[i+1]]    = {'+':1} if t_r == 0 else {'-':1}
            tmp_arr = []
            for rid in rowtup:
                rv_d    = data[rid]
                op      = row_op[rid].keys()[0]
                dd  = {'txid':str(rv_d['t_id']), 'type':'t', 'op':op}
                tmp_arr.append(dd)
            formula.append(tmp_arr)
        formula.sort(key=lambda x:len(x), reverse=True)
        if formula:
            return formula[0]
        return []

    def formula_derivations(self, data, res, f_phs,ph_formula_d, xml_row_map, xml_col_map, txn, t_ids, taxo_id_dict, taxo_value_grp, clean_v_d, f_k_d, key_map_rev, key_map_rev_t, disp_name, row_ind_map, csv_d, update_form ):
        t_op_d  = {}
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            t_row_f     = ph_formula_d.get(('SYS F', str(rr['t_id'])), ())
            ttype   = ''
            op_d        = {}
            if t_row_f:
                op_d    = ph_formula_d.get(("OP", t_row_f[0]), {})
                ttype   = 'SYS'
            if not t_row_f:
                t_row_f     = ph_formula_d.get(('F', str(rr['t_id'])), ())
            if t_row_f:
                ttype   = 'USER'
            r_key, rs_value, operand_rows, row_form, row_op = self.read_formula(ti, rr, f_phs, txn, xml_row_map, xml_col_map[ti])
            if update_form == 'V' and 0:
                if not t_row_f:
                    for ph in f_phs:
                        if ph['k'] in rr:
                            formula_ar  = self.form_fcol_from_sys_formula(ti, operand_rows, row_op, row_ind_map, data, ph)
                            if formula_ar:
                                t_row_f = ('', formula_ar)
                                ph_formula_d[('F', str(rr['t_id']))] = t_row_f
                                break
            if t_row_f:
                #print 't_id', rr['t_id'], rr['t_l'], [ttype]
                if update_form == 'F':
                    for ft in t_row_f[1]:#t_row_f[1]:
                        if ft['type'] == 'v':
                            pass
                        else:
                            tmp_ti  = taxo_value_grp[ft['txid']]
                            t_op_d.setdefault(tmp_ti, {})[ti]   = 1
                trow_op  = {}
                ph_key_map  = {}
                for tr in f_phs:
                    ph_key_map[tr['k']]  = tr['ph']
                #print 't_id', rr['t_id'], rr['t_l']
                #for tr in t_row_f[1]:
                #    print tr
                val_dict, form_d = self.get_formula_evaluation(t_row_f[1], taxo_id_dict, map(lambda x:x['k'], f_phs), {}, None, 'Y', op_d)
                toperand_rows    = []
                l   = len(t_row_f[1])
                er_op   = 1
                for ft in t_row_f[1]:
                    if ft['type'] != 'v' and ft['op'] != '=':
                        if ft['txid'] not in taxo_value_grp:
                            er_op = 0
                            break
                        toperand_rows.append(taxo_value_grp[ft['txid']])
                        trow_op[taxo_value_grp[ft['txid']]]  = {ft['op']:1}
                if er_op == 0:continue
                toperand_rows    = [tuple(toperand_rows)]
                if val_dict:
                    for k, v in val_dict.get(str(rr['t_id']), {}).items():
                        if rr.get(k, {}).get('v', None) != None:
                            v_d = rr.get(k, {})
                            try:
                                clean_value = numbercleanup_obj.get_value_cleanup(v_d['v'])
                            except:
                                clean_value = '0'
                                pass
                            if clean_value == '':
                                clean_value = '0'
                            clean_value = float(clean_value)
                            sum_val     = sum(v['v_ar'])
                            n_value     = abs(clean_value - float(sum_val)) #v['v'].replace(',', '')) 
                            n_value     = self.convert_floating_point(n_value).replace(',', '')
                            if n_value != '0':
                                rr[k]['c_s']    = n_value
                                res[0]['data'][ti][k]['c_s']   = n_value
                                #if deal_id == '216':
                                #    print [ph_key_map[k], clean_value, ' - ',float(v['v'].replace(',', '')), ' = ',n_value]
                            continue
                        if rr.get(k, {}).get('v', '') != '': continue
                        if update_form == 'V':
                            clean_v_d.setdefault(ti, {})[k]   = str(v['v'])
                            rr[k]   = v
                            res[0]['data'][ti][k]   = copy.deepcopy(v)
                if update_form == 'F':
                    #print '\n============================================'
                    #print rr['t_l']
                    for ph in f_phs:
                        if ph['k'] in rr:
                            #print '\t',[ph, rr[ph['k']]['v'], rr[ph['k']].get('PH_D')]
                            f_k_d.setdefault(ti, {})[ph['k']]   = {}
                            self.add_row_formula(ti, f_k_d[ti][ph['k']], toperand_rows, res, data, ph, key_map_rev, key_map_rev_t, trow_op, txn, disp_name, row_ind_map, csv_d, 'Y', form_d, taxo_value_grp)
                            self.add_column_formula(ti, f_k_d[ti][ph['k']], row_form, res, rr, ph, data, key_map_rev, key_map_rev_t, txn, disp_name, csv_d)
            elif 0:
                r_formula   = []
                for ph in f_phs:
                    if ph['k'] in rr:
                        f_k_d.setdefault(ti, {})[ph['k']]   = {}
                        self.add_row_formula(ti, f_k_d[ti][ph['k']], operand_rows, res, data, ph, key_map_rev, key_map_rev_t, row_op, txn, disp_name, row_ind_map, csv_d, '', {}, {})
                        self.add_column_formula(ti, f_k_d[ti][ph['k']], row_form, res, rr, ph, data, key_map_rev, key_map_rev_t, txn, disp_name, csv_d)
                        if rr[ph['k']].get('f_col', []):
                            r_formula   = rr[ph['k']]['f_col'][0]
                if r_formula:
                    tmpar   = []
                    for ii, tr in enumerate(r_formula):
                        tmpar.append({'txid':str(tr['taxo_id']), 'type':'t', 't_type':table_type, 'g_id':group_id, 'op':tr['operator']})
                    t_row_f = ('NEW F', tmpar)
                    trow_op  = {}
                    ph_key_map  = {}
                    for tr in f_phs:
                        ph_key_map[tr['k']]  = tr['ph']
                    val_dict, form_d = self.get_formula_evaluation(t_row_f[1], taxo_id_dict, map(lambda x:x['k'], res[0]['phs']))
                    toperand_rows    = []
                    l   = len(t_row_f[1])
                    for ft in t_row_f[1]:
                        if ft['type'] != 'v' and ft['op'] != '=':
                            toperand_rows.append(taxo_value_grp[ft['txid']])
                            trow_op[taxo_value_grp[ft['txid']]]  = {ft['op']:1}
                    toperand_rows    = [tuple(toperand_rows)]
                    if val_dict:
                        val_dict    = val_dict.get(str(rr['t_id']), {})
                        for k, v in val_dict.items():
                            clean_v_d.setdefault(ti, {})[k]   = str(v['v'])
                            rr[k]   = v
                            res[0]['data'][ti][k]   = copy.deepcopy(v)
                        for ph in f_phs:
                            if ph['k'] in rr and (ph['k'] in val_dict or (not rr[ph['k'] ].get('f_col', []))):
                                f_k_d.setdefault(ti, {})[ph['k']]   = {}
                                self.add_row_formula(ti, f_k_d[ti][ph['k']], toperand_rows, res, data, ph, key_map_rev, key_map_rev_t, trow_op, txn, disp_name, row_ind_map, csv_d, 'Y', {}, {})
        return t_op_d

    def get_column_map(self, ph_kindex, reported_phs, f_phs, phk_ind_data, data, ph_formula_d, taxo_value_grp):
        col_m   = {}
        num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
        for ph in f_phs:
            phi         = ph_kindex[ph['k']]+1
            if ph['k'] not in phk_ind_data:continue
            #remain_phs  = phs[:phi]
            c_col       = sets.Set(phk_ind_data[ph['k']].keys())
            col_map     = {}
            #print '\n-----------------------------------------------'
            #print ph, reported_phs.get(ph['ph'], {}).keys()
            phks    = reported_phs.get(ph['ph'], {}).keys()
            phks.sort(key=lambda x:ph_kindex[x])
            done_r  = {}
            for phk in phks:
                if phk == ph['k']:continue
                if phk not in phk_ind_data:continue
                d_col   = sets.Set(phk_ind_data[phk].keys())
                common  = c_col.intersection(d_col)
                c_f = 1
                for c in list(common):
                    t_row_f     = ph_formula_d.get(('F', str(data[c]['t_id'])), ())
                    if not t_row_f:
                        t_row_f     = ph_formula_d.get(('SYS F', str(data[c]['t_id'])), ())
                    if not t_row_f:continue
                    formula = t_row_f[1]
                    res     = {} #formula[0]
                    opers   = [] #formula[1:]
                    for rr in formula:
                        if rr['op'] == '=':
                            res = rr
                        else:
                            opers.append(rr)
                    op_overlap  = 'N'
                    for oper in opers:
                        op_txid    = oper['txid']
                        op_type    = oper['type']
                        if op_type == 'v':
                            pass
                        else:
                            tc  = taxo_value_grp[oper['txid']]
                            if ph['k'] not in data[tc] or phk not in data[tc]:continue
                            ph_csv1  = data[tc][ph['k']]['phcsv']['s']
                            ph_csv2  = data[tc][phk]['phcsv']['s']
                            if ph_csv1 == ph_csv2:
                                if phk_ind_data[ph['k']][c] != phk_ind_data[phk][c]:
                                    op_overlap  = 'Y'
                                    break
                            elif ph_csv1 and ph_csv2:
                                if num_obj[self.scale_map_d[ph_csv2]] > num_obj[self.scale_map_d[ph_csv1]]:
                                        tmp2    = ph_csv1
                                        ph_csv1 = ph_csv2
                                        ph_csv2 = tmp2
                                v2          = phk_ind_data[phk][c]
                                #print 'ELSE'
                                v2, nscale   = self.convert_frm_to(v2, self.scale_map_d[ph_csv2]+' - '+self.scale_map_d[ph_csv1], ph_csv2)
                                v2          = self.convert_floating_point(v2).replace(',','')
                                #print '\tELSE',[ph_csv2, ph_csv1, (float(v2), float(phk_ind_data[ph['k']][c])), float(phk_ind_data[ph['k']][c]) != float(v2)]
                                if float(phk_ind_data[ph['k']][c]) != float(v2):
                                    op_overlap  = 'Y'
                                    break
                    if op_overlap  == 'Y':
                        c_f = 0
                        break
                        
                #print '\t', [phk, c_f, list(common)]
                if c_f == 1:
                    rem     =  list(d_col - c_col)
                    #print 'Remaining ', rem
                    for r in rem:
                        if r in done_r:continue
                        col_map[r]  = phk
                        done_r[r]   = 1
                
                phi -= 1
            #print 'FINAL ', col_map
            if col_map:
                #print ph['ph']
                #print '\t',ph['k'], col_map
                col_m[ph['k']]               = col_map
        return col_m

    def read_mat_date(self, ijson, mat_date, txn_m):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        if mat_date.get('vids', {}) and isinstance(mat_date.get('vids', {}), list):
            mat_date['vids']   = mat_date['vids'][0]
        else:
            mat_date['vids']   = {}
        table_type    = ijson['table_type']
        mat_d   = {}
        if 1:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            if not mat_date.get('vids', {}):
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            else:
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(mat_date['vids'].keys()))
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            g_vids  = mat_date.get('vids', {})
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type    = rr
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                c   = int(c_id.split('_')[2])
                r   = int(c_id.split('_')[1])
                if g_vids.get(vgh_text, {}) and table_id+'-'+str(c) not in g_vids.get(vgh_text, {}):continue
                t   = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                if t:
                    mat_d[(taxo_id, table_id, r)]   = (c_id, xml_id, t)
    
        return mat_d

                



    def create_final_output_with_ph(self, ijson,ph_filter='P', restated_model='Y'):
        cinfo           = ijson.get('g_cinfo', {})
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)


        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        years       = {}
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs.setdefault(ph, {})[doc_id]        = 1
                years[ph[-4:]]       = {}
        y   = str(c_year)
        #print 'FY'+y not in dphs, 'Q4'+y in dphs
        #sys.exit()
        if 'FY'+y not in dphs and 'Q4'+y in dphs:
            del dphs['Q4'+y]
            
        dphs_ar         = report_year_sort.year_sort(dphs.keys())
        dphs_ar.reverse()
        ignore_doc_ids  = {}
        if ph_filter != 'P':
            rem = int(ph_filter.split('-')[1])
            new_phs = dphs_ar[rem:]
            for ph in dphs_ar[:rem]:
                ignore_doc_ids.update(dphs[ph])
                for doc_id in dphs[ph].keys():
                    del doc_d[doc_id]
                del dphs[ph]

        ph_f_map, ph_grp_ar, ph_gid_map         = self.read_ph_formula(ijson, 'Y')

        group_id    = ijson.get('grpid', '')
        report_pt   = ijson.get('report_pt', '')
        report_year = ijson.get('report_year', '')
        #if '-' in group_id:
        #    group_id, doc_grpid = group_id.split('-')
        #    #ijson['grpid']  = group_id
        ph_formula_d                            = self.read_ph_user_formula(ijson, '')
        if group_id:
            ph_formula_d                            = self.read_ph_user_formula(ijson, group_id, ph_formula_d)
        if ijson.get('NO_FORM', '') == 'Y':
            ph_f_map        = {}
            ph_formula_d    = {}

        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
        if ijson['table_type'] not in rev_m_tables:
            return [{"message":'done',"ID":0}]
        final_d         = {}
        table_mapping_d = {
                    }
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()
        
        #for table_type in rev_m_tables.keys():
        #    if ijson.get('table_types', []) and table_type not in ijson.get('table_types', []):continue
        key_map = {
                        1   : 'actual_value',
                        2   : 'clean_value',
                        3   : 'd',
                        4   : 'pno',
                        5   : 'bbox',
                        6   : 'gv_ph',
                        7   : 'scale',
                        8   : 'value_type',
                        9   : 'currency',
                        10  : 'ph',
                        11  : 'label change',
                        12  : 're_stated',
                        13  : 'period',
                        14  : 'description',
                        15  : 'f_col',
                        16  : 'f_row',
                        17  : 'operator',
                        18  : 'taxo',
                        19  : 'row_id',
                        20  : 'col_id',
                        21  : 'taxo_id',
                        22  : 'maturity_date',
                        23  : 'h_or_t',
                        24  : 'table_id',
                        25  : 'xml_id',
                    }
        key_map_rev     = {}
        key_map_rev_t   = {}
        for k, v in key_map.items():
            key_map_rev[v]  = k
            key_map_rev_t[v]  = v
        t_ids       = ijson.get('t_ids', [])
        report_map_d    = {
                            'Q1'    : {'Q1':1},
                            'FY'    : {'FY':1},
                            'Q2'    : {'Q2':1},
                            'Q3'    : {'Q3':1},
                            'Q4'    : {'Q4':1},
                            'H1'    : {'H1':1, 'Q2':1},
                            'H2'    : {'H2':1, 'Q4':1, 'FY':1},
                            'M9'    : {'M9':1, 'Q3':1},
                            }
        order_d     = self.order_d
        table_name_mp_d = {
                            '216' :{
                                        'STD'   : 'Debt coming due in the next two years',
                                        'LTD'   : 'Debt',
                                        'OLSE'  : 'Rent Expense',
                                        'COGS'  : 'Cash and Cash equivalent',
                                        'COSS' : 'Stock based compensation', 
                                        'RNTL'    : 'Revolver Availability',
                                    },
                              '218':      {
                                        'RNTL':'Revolver Availability',
                                        'COGSS':'Plan Benefit Obligation',
                                        'STD':'Debt coming due in the next two years',
                                        'LTD':'Debt',
                                        'OLSE':'Rent Expense'
                                    },

                              '221':      {
                                        'STD':'Debt coming due in the next two years',
                                        'LTD':'Debt',
                                        'OLSE':'Rent Expense'
                                    },
                               '220':{
                                        'COGS':'Employee Benefit Plans',
                                        'STD': 'Debt coming due in the next two years', 
                                        'LTD':'Debt',
                                        'OLSE':'Rent Expense',
                                        'CLSE':'Capital Lease Obligation'
                                    },
                            '217' :{
                                        'STD'   : 'Debt coming due in the next two years',
                                        'LTD'   : 'Debt',
                                        'OLSE'  : 'Rent Expense',
                                    },
                            '219' :{
                                        'COGSS' : 'Plan Benefit Obligation',
                                        'LTD'   : 'Debt', 
                                        'STD' : 'Debt coming due in the next two years', 
                                        'RNTL'   : 'Revolver Availability', 
                                    },
                            '83'    :{
                                        'FE'    : 'Finance Income/Expense',
                                    }
                                

                            }
        gen_type    = ijson.get('type','')
        grp_mad_d   = {}
        table_type  = ijson['table_type']
        db_file     = self.config['cinfo']
        conn, cur   = conn_obj.sqlite_connection(db_file)
        
        sql = "CREATE TABLE IF NOT EXISTS model_id_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type TEXT, gen_id INTEGER, description TEXT, user_name VARCHAR(100), datetime TEXT);"
        cur.execute(sql)
        conn.commit()
        sql = "select gen_id, description from model_id_map where table_type='%s'"%(table_type)
        cur.execute(sql)
        res = cur.fetchone()
        gen_id, description = '', ''
        if res:
            try:
                gen_id  = int(res[0])
            except:pass
            if res[1]:
                description = res[1]
        disp_name   = table_name_mp_d.get(deal_id, {}).get(table_type, table_type_m.get(table_type, table_type))
        if gen_id == '':
            if table_type in self.order_d:
                gen_id  = self.order_d[table_type]
            else:
                with conn:
                    sql = "select seq from sqlite_sequence WHERE name = 'model_id_map'"
                    cur.execute(sql)
                    r       = cur.fetchone()
                    if r:
                        g_id    = int(r[0])
                        sql     = "select max(gen_id) from model_id_map" 
                        cur.execute(sql)
                        r       = cur.fetchone()
                        tg_id   = int(r[0])
                        g_id    = max(g_id, tg_id)
                        g_id    += 1
                    else:
                        g_id    = 1
                    gen_id  = g_id
            cur.executemany('insert into model_id_map(gen_id, description, table_type)values(?,?,?)', [(gen_id, disp_name, table_type)])
            conn.commit()
        conn.close()
        #if description:
        #    disp_name   = description
            

        if 1:#gen_type == 'group':
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql         = "select table_id  from clean_actual_value_status where table_type='%s'"%(table_type)
            try:
                cur.execute(sql)
                res = cur.fetchall()
            except:
                res = []

            actual_v_d  = {}
            for rr in res:
                table_id    = str(rr[0])
                actual_v_d[table_id]    = 1     
            sql = "select vgh_group_id, doc_group_id, group_txt from vgh_doc_map where table_type='%s'"%(table_type)
            cur.execute(sql)
            vgh_doc_map = {}
            res = cur.fetchall()
            for rr in res:
                vgh_group_id, doc_group_id, group_txt   = rr
                vgh_doc_map[(vgh_group_id, doc_group_id)]   = group_txt
            sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
            try:
                cur.execute(sql)
                res = cur.fetchall()
            except:
                res = []
            grp_info    = {}
            for rr in res:
                tgroup_id, table_type, group_txt = rr
                grp_info[str(tgroup_id)]   = group_txt
            sql = "select group_id, table_type, group_txt from vgh_group_info where table_type like '%s'"%('HGHGROUP-'+table_type+'%')
            try:
                cur.execute(sql)
                res = cur.fetchall()
            except:
                res = []
            for rr in res:
                #print rr
                tgroup_id, table_type, group_txt = rr
                grp_info[str(table_type)]   = group_txt
            #sql         = "select row_id, group_txt from vgh_group_map where table_type='%s'"%(table_type)
            #cur.execute(sql)
            #res = cur.fetchall()
            #for rr in res:
            #    if rr[1] not in grp_mad_d:
            #        grp_mad_d[rr[1]]    = len(grp_mad_d.keys())
            conn.close()
            tmp_map = {}    
            #grpids  = grp_info.keys()
            #grpids.sort(key=lambda x:int(x))
            #for ii, grp in enumerate(grpids):
            #    tmp_map[grp_info[grp]]  = ii
            #grp_mad_d   = tmp_map
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        for table_type in [ijson['table_type']]:
            #disp_name   = table_name_mp_d.get(deal_id, {}).get(table_type, table_type_m.get(table_type, table_type))
            ijson['gen_output'] = 'Y'
            ijson['ignore_doc_ids'] = ignore_doc_ids
            if ijson.get('DB_DATA', []):
                res = ijson['DB_DATA']
            else:
                res                 = self.create_seq_across(ijson)
            mdata               = self.create_maturity_date(ijson, txn_m)
            mat_date            = {}
            if mdata:
                if '-' in group_id:
                    group_id_sp = group_id.split('-')
                    if group_id_sp[1] in mdata:
                        mat_date            = mdata[group_id_sp[1]]
                elif '' in mdata:
                    mat_date            = mdata['']
            if mat_date:
                mat_date    = self.read_mat_date(ijson, mat_date, txn_m)
            flip_d  = {}
            if restated_model == 'Y':# and (ijson.get('FV', '') == 'Y' or (table_type == 'FE' and deal_id=='36')):
                t_res   = self.get_restated_value(copy.deepcopy(res), ph_formula_d, t_ids, txn_m, mat_date, txn, {}, actual_v_d, dphs, ijson, ph_f_map, years, ph_gid_map, y, key_map_rev, key_map_rev_t, disp_name, table_type, group_id, doc_d, restated_model)
                if isinstance(t_res, list) and t_res[0]['message'] != 'done':
                    return t_res
                flip_d  = self.flip_sign(t_res, ph_formula_d)
                
                                
                            
                                
                                
                    
            org_res             = copy.deepcopy(res)
            phs                 = copy.deepcopy(res[0]['phs'])
            data                = copy.deepcopy(res[0]['data'])
                
                    
            if t_ids:
                validate_op         = self.validate_cell_data(filter(lambda x:x['t_id'] in t_ids, data), phs, actual_v_d)
            else:
                validate_op         = self.validate_cell_data(data, phs, actual_v_d)
            rc_d                = {}
            consider_ks         = {}
            consider_table      = {}
            consider_rows       = {}
            xml_row_map         = {}
            ph_kindex           = {}
            for ii, ph in enumerate(phs):
                ph_kindex[ph['k']]  = ii
            duplicate_taxonomy  = {}
            phk_data            = {}
            phk_ind_data        = {}
            validation_error    = {}
            #print validate_op
            clean_v_d       = {}
            ph_ti               = {}
            taxo_value_grp  = {}
            taxo_id_dict    = {}
            csv_d   = {}
            xml_col_map = {}
            row                 = 1
            row_ind_map         = {}
            taxo_unq_d          = {}
            table_xml_d         = {}
            more_than_one_scale = {}
            rsp_phs             = {}
            pt_order_d          = {}
            pt_arr         = report_year_sort.year_sort(['FY2018', 'Q12018', 'Q22018', 'Q32018', 'Q42018', 'H12018', 'H22018', 'M92018'])
            for ii, ph in  enumerate(pt_arr):
                pt_order_d[ph[:-4]]          = ii
                
            if report_pt and report_year:
                #print pt_arr,[report_pt,report_year]
                for ii, ph in enumerate(phs):
                    if len(ph['n']) < 6:continue
                    pt  = ph['n'][:-4]
                    py  = ph['n'][-4:]
                    try:
                        py  = int(py)
                    except:
                        continue
                    #print [ph['n'], pt, py, pt_order_d.get(pt, -1), pt_order_d[report_pt]]
                    if (py < report_year) or  (py == report_year and pt_order_d.get(pt, -1) <= pt_order_d[report_pt]):
                        rsp_phs.setdefault(ph['n'], []).append(ph['k'])
                #for k, v in rsp_phs.items():
                #    print k, v
            for ti, rr in enumerate(data[:]):
                taxo_id_dict[str(rr['t_id'])]    = rr
                taxo_value_grp[str(rr['t_id'])]  = ti
                t_s_f       = ph_formula_d.get(('S', str(rr['t_id'])), {})
                if t_ids and rr['t_id'] not in t_ids:continue
                taxo    = rr.get('taxo', '')
                tmptaxo = taxo.split(' / ')
                f_taxo  = {}
                for tt in tmptaxo:
                    for t1 in tt.split('@'):
                        if 'TASTAXO_' in t1 or 'TAS_TAXO' in t1:continue
                        if ' - ' in t1:
                            f_taxo[t1.split(' - ')[1]]  = 1
                        else:
                            f_taxo[t1]  = 1
                f_taxo  = f_taxo.keys()
                f_taxo.sort()
                f_taxo  = ' / '.join(f_taxo[:1])
                tf_taxo    = self.gen_taxonomy_alpha_num(rr['t_l'])
                if not f_taxo:
                    f_taxo    = self.gen_taxonomy(rr['t_l'])
                if not f_taxo:
                    f_taxo    = rr['t_l']
                if not tf_taxo:
                    tf_taxo    = rr['t_l']
                duplicate_taxonomy.setdefault(tf_taxo, {})[rr['t_id']]   = rr['t_l']
                if f_taxo not in taxo_unq_d:
                    taxo_unq_d[f_taxo]  = 1
                else:
                    ol_cnt  = taxo_unq_d[f_taxo]
                    taxo_unq_d[f_taxo]  = ol_cnt+1
                    f_taxo  += '-'+str(ol_cnt)
                rr['f_taxo']      = f_taxo
                res[0]['data'][ti]['f_taxo']      = f_taxo
                row += 1
                row_ind_map[ti] = row
                scale_d         = {}
                xml_col_map.setdefault(ti, {})
                for ii, ph in enumerate(phs):
                    if ph['k'] not in rr:continue
                    v_d         = rr[ph['k']]
                    t           = v_d['v']
                    clean_value = t
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(t)
                    except:
                        clean_value = ''
                        pass
                    if not clean_value and v_d['t'] in actual_v_d:
                        clean_value = v_d['v']
                    x_c    = self.clean_xmls(v_d['x'])
                    if x_c:
                        table_id    = v_d['t']
                        x_c    = x_c.split(':@:')[0].split('#')[0]
                        table_xml_d.setdefault(table_id, {})[(int(x_c.split('_')[1]), int(x_c.split('_')[0].strip('x')))]   = 1
                    tk   = self.get_quid(v_d['t']+'_'+v_d['x'])
                    c_id        = txn_m.get('XMLID_MAP_'+tk)
                    if c_id:
                        ktup    = (rr['t_id'], v_d['t'], int(c_id.split('_')[1]))
                        if ktup in mat_date:
                            v_d['mdate']    = mat_date[ktup][2]
                            res[0]['data'][ti][ph['k']]['mdate']    = mat_date[ktup][2]
                            v_d.setdefault('phcsv', {})['mdate']    =  mat_date[ktup][2]
                            res[0]['data'][ti][ph['k']].setdefault('phcsv', {})['mdate']    = mat_date[ktup][2]
                    xml_row_map.setdefault((v_d['t'], v_d['x']), {})[ti]    = 1
                    xml_col_map.setdefault(ti, {})[(v_d['t'], v_d['x'])]         = ph['k']
                    key     = v_d['t']+'_'+self.get_quid(v_d['x'])
                    ph_map  = txn.get('PH_MAP_'+str(key))
                    if ph_map:
                        period_type, period, currency, scale, value_type    = ph_map.split('^')
                    if ph['k'] in t_s_f and clean_value:
                        sgrpid, form    = t_s_f[ph['k']]
                        v, nscale   = self.convert_frm_to(clean_value, form, scale)
                        if v:
                            v   =str(v)
                            clean_value = v
                            rr[ph['k']]['expr_str']   = form+'('+str(rr[ph['k']]['v'])+') = '+str(v)
                            rr[ph['k']]['v']    = v
                            rr[ph['k']]['fv']   = 'Y'
                            res[0]['data'][ti][ph['k']]['expr_str']   = form+'('+str(rr[ph['k']]['v'])+') = '+str(v)
                            res[0]['data'][ti][ph['k']]['v']    = v
                            res[0]['data'][ti][ph['k']]['fv']   = 'Y'
                            csv_d[(v_d['t'], v_d['x'])]    = (period_type, period, currency, self.scale_map_rev_d[nscale], value_type)
                    if (v_d['t'], v_d['x']) not in csv_d:
                        period_type, period, currency, scale, value_type    = v_d['phcsv']['p'], v_d['phcsv']['pt'], v_d['phcsv']['c'], v_d['phcsv']['s'], v_d['phcsv']['vt']
                        csv_d[(v_d['t'], v_d['x'])]    = (period_type, period, currency, scale, value_type)
                    if v_d['v'].strip():
                        scale_d.setdefault(csv_d[(v_d['t'], v_d['x'])][3], {})[v_d['rid']]         = 'N'
                            
                    clean_v_d.setdefault(ti, {})[ph['k']]   = clean_value
                    ph_ti.setdefault(ph['n'], {})[ti]  = ph['k']
                    if clean_value == '':continue
                    phk_data.setdefault(ph['k'], []).append((ti, clean_value))
                    phk_ind_data.setdefault(ph['k'], {})[ti]    = clean_value
                    if not t_ids or rr['t_id'] in t_ids:
                        if validate_op.get(rr['t_id'], {}).get(ph['k'], ''):
                            validation_error.setdefault(rr['t_id'], {})[ph['k']]    = 'N'
                if len(scale_d.keys()) > 1:
                    more_than_one_scale[rr['t_id']] = {}
                    for sc, rid in scale_d.items():
                        more_than_one_scale[rr['t_id']].update(rid)
            if validation_error and 0:
                res = [{'message':'Error Validation failed', 'data':validation_error}]
                return res
            if 0:#more_than_one_scale:
                res = [{'message':'Error More than one scale', 'data':more_than_one_scale}]
                return res

            if 0:#duplicate_taxonomy:# and gen_type != 'display' :
                tmp_d = {}
                for f_taxo_id, tid_dict in duplicate_taxonomy.items():
                    if len(tid_dict.keys()) > 1:
                        tmp_d.update(tid_dict)
                if tmp_d:
                    res = [{'message':'Error Duplicate taxonomy', 'data':tmp_d}]
                    return res
                        
                
            
            f_phs, reported_phs               = self.get_order_phs(phs, dphs, self.report_map_d)
            pt_arr         = report_year_sort.year_sort(rsp_phs.keys())
            d_new_phs       = []
            for ph in pt_arr:
                dd  = {'ph':ph, 'new':"Y", 'k':rsp_phs[ph][-1], 'g':ph, 'dph':(ph[:-4], int(ph[-4:]))}
                f_phs   = [dd]+f_phs
                reported_phs[ph]    = {}
                for tk in rsp_phs[ph]:
                    reported_phs[ph][tk]    = 1
                    
                d_new_phs.append(copy.deepcopy(dd))
            dphs_all         = report_year_sort.year_sort(dphs.keys())
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
            col_m               = self.get_column_map(ph_kindex, reported_phs, f_phs, phk_ind_data, data, ph_formula_d, taxo_value_grp)
            tt_ph_f             = self.read_ph_config_formula(ijson)
            
            n_phs   = {}
            #if 1:
            if ijson.get('print', '') == 'Y':
                print 'col_m ', col_m
            #sys.exit()
            f_phs, f_phs_d, n_phs, f_k_d, new_phs, missing_value, t_op_d  = self.calculate_derivative_values(data, res, ph_formula_d, clean_v_d, col_m, taxo_value_grp, ph_ti, f_phs, f_phs_d, ph_f_map, years, done_ph, ph_gid_map, y, ph_map_d, t_ids, xml_row_map, xml_col_map, txn, taxo_id_dict, key_map_rev, key_map_rev_t, disp_name, table_type, group_id, row_ind_map, csv_d, doc_d, dphs_all, restated_model, phk_ind_data, reported_phs, tt_ph_f, 'N', flip_d, actual_v_d, ijson)    
            #self.calculate_missing_values_by_fx(t_ids, res, data, ph_formula_d, taxo_id_dict, table_type, group_id, row_formula, f_phs)
            run_date_diff   = 0
            if table_type == 'LTD' and str(deal_id) == '216':
                run_date_diff   = 1
            elif table_type == 'DEBT' and str(deal_id) == '218':
                run_date_diff   = 1
            if run_date_diff == 1:
                data, clean_v_d, f_k_d    = self.add_date_diff_row(ijson, data, f_phs, t_ids, company_name, model_number, clean_v_d, f_k_d, f_phs_d, taxo_unq_d)
                res[0]['data']  = data
            #if ph['ph'] in self.ignore_phs.get(str(deal_id), {}).get(ijson.get('grpid', ''), {}):continue
            if self.ignore_phs.get(str(deal_id), {}).get(ijson.get('grpid', ''), {}):
                f_phs   = filter(lambda x:x['ph'] not in self.ignore_phs[str(deal_id)][ijson['grpid']], f_phs)
                     

            n_y                 = str(int(y)+1)
            row                 = 1
            update_sign         = ''
            if ijson.get('project_name', '').lower() == 'schroders' and restated_model=='N':# and ijson.get('FV', '') == 'Y':
                update_sign         = 'Y'
                self.flip_formula_sign(data, res, f_phs, clean_v_d, taxo_value_grp)
            f_col_k = {}
            for ti, rr in enumerate(data[:]):
                for ii, ph in enumerate(f_phs):
                    if ph['k'] in rr:
                        f_col_k[ ph['k']]   = 1
            f_phs   = filter(lambda x:x['k'] in f_col_k, f_phs)
            pt_value_e          = {}
            for ti, rr in enumerate(data[:]):
                if t_ids and rr['t_id'] not in t_ids:continue
                #print '\n============================'
                #print rr['t_id'], rr['t_l']
                row += 1
                col = 0
                f_taxo  = rr['f_taxo']
                dd  = {
                        key_map_rev['actual_value']     : rr['t_l'],
                        key_map_rev['taxo']             : f_taxo,
                        key_map_rev['clean_value']      : '',
                        key_map_rev['d']                : rr['d'],
                        key_map_rev['pno']              : rr['x'].split(':@:')[0].split('#')[0].split('_')[1],
                        key_map_rev['bbox']             : rr['bbox'],
                        key_map_rev['h_or_t']           : rr.get('th_flg', '')
                        }
                lchange = []
                for lc in rr.get('ldata', []):
                    if lc['s'] == 'Y' and lc['x']:
                        periods = report_year_sort.year_sort(lc['v'].keys())
                        periods.reverse()
                        ydd  = {key_map_rev['actual_value']:lc['txt'], key_map_rev['d']:lc['d'],  key_map_rev['bbox']:lc['bbox'], key_map_rev['pno']:lc['x'].split(':@:')[0].split('#')[0].split('_')[1], key_map_rev['period']:periods[0]}
                        lchange.append(ydd)
                dd[key_map_rev['label change']]  = lchange
                rc_d[(row, col)]    = dd
                tmpphs              = [] 
                key_value           = clean_v_d.get(ti, {})
                ph_value_d          = {}
                indx_d              = {}
                #xml_col_map         = {}
                ph_map              = {}
                for ii, ph in enumerate(phs):
                    if ph['k'] not in rr:continue
                    ph_map[ph['k']]              = ph['ph']
                    v_d = rr[ph['k']]
                    t       = v_d['v']
                    clean_value = clean_v_d.get(ti, {}).get(ph['k'], '')
                    tmpphs.append(ph)
                    #key_value[ph['k']]       = clean_value
                    #print [ph['n'], clean_value, ph['k']]
                    ph_value_d.setdefault(ph['n'], {}).setdefault(clean_value, {})[ph['k']]          = 1
                    indx_d[ph['k']]              = ii
                #f_phs       = self.get_order_phs(tmpphs, dphs, report_map_d)
                #r_key, rs_value, operand_rows, row_form, row_op = self.read_formula(ti, rr, f_phs, txn, xml_row_map, xml_col_map)
                #print '\n============================='
                #print rr['t_l']
                rs_value    = {}
                consider_rows[rr['t_id']]       = 1
                for ii, ph in enumerate(f_phs):
                    col += 1
                    if ph['k'] in rr:
                        consider_ks[ph['k']]         = 1
                    if ph['k'] not in rr:
                        if ph['ph'] in dphs and key_value.get(ph['k'], '') == '' and restated_model=='Y':
                            trs_value   = rr.get(ph['k'], {}).get('rest_ar', [])
                            #other_values            = ph_value_d.get(ph['ph'], {});
                            #trs_value    = []
                            #for v in other_values.keys():
                            #    if v != key_value.get(ph['k'], ''):
                            #        for key in other_values[v].keys():
                            #                if 1:#indx_d[key] > c_index:
                            #                    trs_value.append(key)
                            
                            if trs_value:
                                dd      = self.get_gv_dict({'v':'','x':'','bbox':[], 'd':'', 't':''}, txn, key_map_rev, ph, csv_d)
                                trs_value.sort(key=lambda x:(dphs_all.index(doc_d[rr[x]['d']][0]), rr[x]['d']))
                                self.add_restated_values(ti, rr, {'v':'','x':'','bbox':[], 'd':'', 't':''}, dd, res, trs_value, key_map_rev, key_map_rev_t, ph, txn, dphs_ar, doc_d, csv_d, actual_v_d)
                                rc_d[(row, col)]    = dd
                        continue
                    pt, pyear   = ph['ph'][:-4], ph['ph'][-4:]
                    if pyear != n_y or rr[ph['k']].get('PH_D', '') == 'Y':
                        pt_flg, expr_str  = self.validate_pt_value(pt, pyear, key_value, f_phs_d, rr, rr[ph['k']].get('PH_D', ''))
                        if pt_flg:
                            cs_f    = 0
                            cs_ar   = []
                            for p_id in t_op_d.get(ti, {}).keys():
                                cs_ar.append(data[p_id].get(ph['k'], {}).get('cs', ''))
                                if data[p_id].get(ph['k'], {}).get('cs', '') != '':
                                    cs_f == 1
                                    break
                            #pt_value_e.setdefault(rr['t_id'], {})[ph['k']]  = 'Y'
                            #res[0]['data'][ti][ph['k']]['expr_str'] =  res[0]['data'][ti][ph['k']].get('expr_str', '')+' -- '+expr_str+str([t_op_d.get(ti, {}).keys(), cs_ar])
                            if (not t_op_d.get(ti, {}).keys() or cs_f == 1):
                                pt_value_e.setdefault(rr['t_id'], {})[ph['k']]  = 'N'
                                res[0]['data'][ti][ph['k']]['expr_str'] =  res[0]['data'][ti][ph['k']].get('expr_str', '')+' -- '+expr_str+str([t_op_d.get(ti, {}).keys(), cs_ar])
                                pass
                    #r_key[ph['k']]   = 1
                    consider_ks[ph['k']]         = 1
                    rs_value[ph['k']]  = []
                    #other_values            = ph_value_d.get(ph['ph'], {})
                    #c_index                 = indx_d[ph['k']]
                    v_d                     = rr[ph['k']]
                    t                       = v_d['v']
                    consider_table[v_d['t']]       =  1
                    clean_value             = key_value[ph['k']]
                    #if clean_value == '':continue
                    #for v in other_values.keys():
                    #    if v != key_value[ph['k']]:
                    #        for key in other_values[v].keys():
                    #            if 1:#indx_d[key] > c_index:
                    #                if  rr[key]['t'] != v_d['t']:
                    #                    rs_value[ph['k']].append(key)
                    
                    dd      = self.get_gv_dict(v_d, txn, key_map_rev, ph, csv_d, actual_v_d)
                    #rs_value[ph['k']].sort(key=lambda x:(dphs_ar.index(doc_d[rr[x]['d']][0]), rr[x]['d']))
                    trs_value   = v_d.get('rest_ar', [])
                    if trs_value:
                        #print '\t',[ph['k'], trs_value]
                        del v_d['rest_ar']
                        self.add_restated_values(ti, rr, v_d['org_d'],dd, res, trs_value, key_map_rev, key_map_rev_t, ph, txn, dphs_ar, doc_d, csv_d, actual_v_d)
                        del v_d['org_d']
                    #self.add_row_formula(ti, dd, operand_rows, res, data, ph, key_map_rev, key_map_rev_t, row_op, txn, disp_name, row_ind_map, csv_d)
                    #self.add_column_formula(ti, dd, row_form, res, rr, ph, data, key_map_rev, key_map_rev_t, txn, disp_name, csv_d)
                    #print (v_d['v'], v_d['t'], v_d.get('expr_str', ''), clean_value, (row, col))
                    tmp_fd  = f_k_d.get(ti, {}).get(ph['k'], {})
                    if key_map_rev['f_col'] in tmp_fd:
                        dd[key_map_rev['f_col']] = tmp_fd[key_map_rev['f_col']]
                        if update_sign == 'Y':
                            dd[key_map_rev['f_col']]    = self.update_form_sign(data, taxo_value_grp, dd[key_map_rev['f_col']])
                    if key_map_rev['f_row'] in tmp_fd:
                        dd[key_map_rev['f_row']] = tmp_fd[key_map_rev['f_row']]
                    rc_d[(row, col)]    = dd
                            
                        

            #self.flip_formula_sign(data, res, f_phs)
            table_type          = table_mapping_d.get(table_type, table_type)
            #print 'Result ', len(rc_d.keys())
            #print consider_ks
            #print f_phs
            #for ph in res[0]['phs']:
            #    if ph['k'] in consider_ks:
            #        print ph
            #sys.exit()
            res[0]['data']      = filter(lambda x:x['t_id'] in consider_rows, res[0]['data'])
            res[0]['phs']       = filter(lambda x:x['k'] in consider_ks, res[0]['phs'])
            new_phs = new_phs + d_new_phs
            if new_phs:
                ph_map_d    = {}
                for ph in filter(lambda x:x['k'] in consider_ks, res[0]['phs']):
                    ph_map_d[ph['n']]   = ph
                for ph in new_phs:
                    ph['n'] = ph['ph']
                    ph_map_d[ph['n']]   = ph
                dphs_ar             = report_year_sort.year_sort(ph_map_d.keys())
                dphs_ar.reverse()
                res[0]['phs']       = map(lambda x:ph_map_d[x], dphs_ar)
            else:
                res[0]['phs']       = filter(lambda x:x['k'] in consider_ks, res[0]['phs'])
            res[0]['phs']       = filter(lambda x:x['k'] in consider_ks, res[0]['phs'])
            #print res[0]['phs']
            #sys.exit()
            res[0]['table_ar']  = filter(lambda x:x['t'] in consider_table, res[0]['table_ar'])
            main_header = 'Main Table'
            
            if len(res[0]['main_header'])   == 1 and res[0]['main_header'][0]:
                main_header = {'Schroders':'Main Table'}.get(res[0]['main_header'][0], res[0]['main_header'][0])

            if gen_type == 'all':
                final_d[gen_id] = (binascii.b2a_hex(disp_name)+':@@:'+ph_filter, {'data':rc_d, 'key_map':key_map_rev, 'table_type':main_header}, res, disp_name,)
            elif gen_type == 'group':
                if 'HGHGROUP' in ijson['grpid']:
                    grpid_sp    = ijson['grpid']
                    grp_name    = grp_info[grpid_sp]
                    disp_name   = disp_name+' - '+grp_name
                elif '-' in ijson['grpid']:
                    grpid_sp    = ijson['grpid'].split('-')
                    grp_name    = grp_info[grpid_sp[0]]+' - '+grp_info[grpid_sp[1]]
                    if (grpid_sp[0], grpid_sp[1]) in vgh_doc_map:
                        grp_name    = vgh_doc_map[(grpid_sp[0], grpid_sp[1])]
                    disp_name   = disp_name+' - '+grp_name
                else:
                    disp_name   = disp_name+' - '+grp_info[ijson['grpid']] #ijson['data'][0]
            
                #final_d[str(gen_id*100)+ijson['grpid']] = (binascii.b2a_hex(disp_name)+':@@:'+ph_filter, {'data':rc_d, 'key_map':key_map_rev, 'table_type':main_header}, res, disp_name)
                final_d[str(gen_id)+'-'+ijson['grpid']] = (binascii.b2a_hex(disp_name)+':@@:'+ph_filter, {'data':rc_d, 'key_map':key_map_rev, 'table_type':main_header}, res, disp_name)
            elif gen_type == 'display':
                if ijson.get('grpid', ''):
                    if 'HGHGROUP' in ijson['grpid']:
                        grpid_sp    = ijson['grpid']
                        grp_name    = grp_info[grpid_sp]
                        disp_name   = disp_name+' - '+grp_name
                    elif '-' in ijson['grpid']:
                        grpid_sp    = ijson['grpid'].split('-')
                        grp_name    = grp_info[grpid_sp[0]]+' - '+grp_info[grpid_sp[1]]
                        if (grpid_sp[0], grpid_sp[1]) in vgh_doc_map:
                            grp_name    = vgh_doc_map[(grpid_sp[0], grpid_sp[1])]
                        disp_name   = disp_name+' - '+grp_name
                    else:
                        disp_name   = disp_name+' - '+grp_info[ijson['grpid']] #ijson['data'][0]
                if ijson.get('NO_FORM', '') != 'Y':
                    if gen_type == 'display':
                        if pt_value_e:
                            res = [{'message':'Error Peroid type validation Error', 'data':pt_value_e, 'res':res}]
                            return res
                    else:
                        if pt_value_e:
                            res = [{'message':'Error Peroid type validation Error', 'data':pt_value_e}]
                            return res
                res[0]['disp_name'] = disp_name
                return res
                
        #print key_map
        #print final_d.keys()
        if restated_model == 'Y':
            txtpath      = '/var/www/html/DB_Model/%s/'%(company_name)
        else:
            txtpath      = '/var/www/html/DB_Model_Reported/%s/'%(company_name)
        os.system("mkdir -p '%s'"%txtpath)
        path         = '/mnt/eMB_db/%s/%s/FINAL_OUTPUT/'%(company_name, model_number)
        os.system("mkdir -p "+path)
        for k, v in final_d.items():
            iD  = k
            k   = str(k) +'-'+ph_filter
            fname   = txtpath+'/'+k+'.txt'
            fout    = open(fname, 'w')
            fout.write(str({v[3]:v[1]}))
            fout.close()
        if restated_model == 'Y':
            txtpath      = '/var/www/html/DB_Model_Missing/%s/'%(company_name)
        else:
            txtpath      = '/var/www/html/DB_Model_Reported_Missing/%s/'%(company_name)
        os.system("mkdir -p '%s'"%txtpath)
        for k, v in final_d.items():
            k   = str(k) +'-'+ph_filter
            fname   = txtpath+'/'+k+'.html'
            org_res[0]['phs']   = filter(lambda x:x['k'] in missing_value['PKS'], org_res[0]['phs'])
            org_res[0]['data']  = filter(lambda x:x['t_id'] in missing_value, org_res[0]['data']) 
            if not org_res[0]['phs']:continue
            table_str   = '<table border=1>'
            tr          = '<tr><th rowspan=2>Description</th>'
            tr1         = '<tr>'
            for r in org_res[0]['phs']:
                th          = '<th>%s</th>'%(r['g'])
                th1          = '<th>%s</th>'%(r['ph'])
                tr          += th
                tr1          += th1
            tr          += '</tr>'
            tr1          += '</tr>'
            table_str   += tr+tr1    
            for rr in org_res[0]['data']:
                tr  = "<tr><td>%s</td>"%(rr['t_l'])
                for ph in org_res[0]['phs']:
                    td  = '<td>%s</td>'%(rr.get(ph['k'], {}).get('v', ''))
                    tr  += td
                tr  += '<tr>'
                table_str   += tr
            table_str   += '</table>'
            fout    = open(fname, 'w')
            fout.write(table_str)
            fout.close()
        res = [{'message':'done', 'ID':iD}]
        if ijson.get('NO_FORM', '') != 'Y':
            if gen_type == 'display':
                if pt_value_e:
                    res = [{'message':'Error Peroid type validation Error', 'data':pt_value_e, 'res':res}]
                    return res
            else:
                if pt_value_e:
                    res = [{'message':'Error Peroid type validation Error', 'data':pt_value_e}]
                    return res
        return res
        env1        = lmdb.open(path, map_size=2**39)
        iD          = ''
        with env1.begin(write=True) as txn1:
            for k, v in final_d.items():
                iD  = k
                k   = str(k) +'-'+ph_filter
                fname   = txtpath+'/'+k+'.txt'
                fout    = open(fname, 'w')
                fout.write(str({v[3]:v[1]}))
                fout.close()
                txn1.put('OUTPUT_'+str(k), str(v[1]))
                txn1.put('DISPLAYOUTPUT_'+str(k), str(v[2]))
                txn1.put('TEXT_VALUE_'+str(k), str(v[0]))
        #print path
        #sys.exit()
        res = [{'message':'done', 'ID':iD}]
        return res

    def update_form_sign(self, data, taxo_value_grp, f_col):
        for formula in f_col:
            for ft in formula:
                if ft['op']   == '=':continue
                tid         = ft['t_id']
                ti          = taxo_value_grp[str(tid)]
                ft['op']    =  '+'
        return f_col
            
                

    def flip_formula_sign(self, data, res, f_phs, clean_v_d, taxo_value_grp):
        for tmp_ti, rr in enumerate(data[::-1]):
            sign_d  = {}
            #print '\n==============================================='
            #print [rr['t_l']]
            for ph in f_phs:
                #print '\n\t', ph
                f_col   = rr.get(ph['k'], {}).get('f_col', [])
                if not f_col:continue
                formula = f_col[0]
                op_sign = {}
                for ft in formula:
                    if ft['operator'] == '=':continue
                    op_sign[ft['taxo_id']]  = ft['operator']

                tmp_ti          = taxo_value_grp[str(rr['t_id'])]
                res_sign    = ''
                if res[0]['data'][tmp_ti].get(ph['k'], {}).get('flip_sign', '') == 'Y':
                    clean_value = clean_v_d.get(tmp_ti, {}).get(ph['k'], '')
                    res_sign        = '+'   
                    if '-' in clean_value:
                        res_sign    = '-'   
                #print 'res_sign ', [res_sign]
                for tid, op in op_sign.items():
                    ti          = taxo_value_grp[str(tid)]
                    #print '\t\t',[tid,op, data[ti]['t_l']]
                    if ph['k'] in data[ti]:
                        clean_value = clean_v_d[ti][ph['k']]
                            
                        #print '\t\t', [clean_value]
                        sign        = '+'   
                        if '-' in clean_value:
                            sign    = '-'   
                        if data[ti]['th_flg'] == 'N' and 0:
                            if sign == '-':
                                clean_value = clean_value.strip('-') 
                            else:
                                clean_value = '-'+clean_value 
                            clean_v_d[ti][ph['k']]  = clean_value
                            data[ti][ph['k']]['v']       = clean_value
                            res[0]['data'][ti][ph['k']]['v'] = clean_value
                            continue
                        if res_sign:
                            if res_sign == '-':
                                if sign == '-':
                                    sign    = '+'
                                elif sign == '+':
                                    sign    = '-'
                            elif res_sign == '+':
                                if sign == '-':
                                    sign    = '+'
                                elif sign == '+':
                                    sign    = '-'
                            if sign == '-':
                                clean_value = '-'+clean_value
                            else:
                                clean_value = clean_value.strip('-')
                            clean_v_d[ti][ph['k']]  = clean_value
                            data[ti][ph['k']]['v']       = clean_value
                            res[0]['data'][ti][ph['k']]['v'] = clean_value
                            res[0]['data'][ti][ph['k']]['flip_sign'] = 'Y'
                        #print '\t\t', [sign, clean_value]
                        
                            
                        if op == '-':
                            if sign == '-':
                                clean_value = clean_value.strip('-') 
                            else:
                                clean_value = '-'+clean_value 
                            clean_v_d[ti][ph['k']]  = clean_value
                            data[ti][ph['k']]['v']       = clean_value
                            res[0]['data'][ti][ph['k']]['v'] = clean_value
                            res[0]['data'][ti][ph['k']]['flip_sign'] = 'Y'
                        #print '\t\tFINAL', [clean_value]
        for ti, rr in enumerate(data[:0]):
            for ph in f_phs:
                f_col   = rr.get(ph['k'], {}).get('f_col', [])
                if not f_col:continue
                formula = f_col[0]
                if ph['k'] in data[ti]:
                    clean_value = clean_v_d[ti][ph['k']]
                    sign        = '+'   
                    if '-' in clean_value:
                        sign    = '-'   
                    if data[ti]['th_flg'] == 'N':
                        if sign == '-':
                            clean_value = clean_value.strip('-') 
                        else:
                            clean_value = '-'+clean_value 
                        clean_v_d[ti][ph['k']]  = clean_value
                        data[ti][ph['k']]['v']       = clean_value
                        res[0]['data'][ti][ph['k']]['v'] = clean_value

    def detect_date(self, txt_d, fye):
        for txt in txt_d.keys():
            cmd = 'cd /root/ParaAPI>/dev/null;python Para_Comp_API_Back_New.py %s "%s";cd - >/dev/null'%(fye, txt)
            res = os.popen(cmd).read()
            txt_d[txt]  = eval(res)
        
        return txt_d
        Para_Comp_API_Back_New.disableprint()
        for txt in txt_d.keys():
            year, periodtype, month1 = map(str, date_obj.Process_String(txt, fye))
            if year.strip():
                txt_d[txt]  = {'p':year, 'pt':periodtype, 'm':str(month1)}
            else:
                year1, periodtype1, month2 = map(str, date_obj.Process_String_Func(txt, fye))
                txt_d[txt]  = {'p':year1, 'pt':periodtype1, 'm':str(month2)}
        Para_Comp_API_Back_New.enableprint()
        return txt_d
    


    def add_date_diff_row(self, ijson, data, f_phs, t_ids, company_name, model_number, clean_v_d, f_k_d, f_phs_d, taxo_unq_d):
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()
        #print lmdb_path2

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        #print lmdb_path2
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()
        txt_d           = {}
        ph_map          = {
                                'Q1'    : {'m':3, "mstr":"March 31"},
                                'Q2'    : {'m':6, "mstr":"June 30"},
                                'H1'    : {'m':6, "mstr":"June 30"},
                                'Q3'    : {'m':9, "mstr":"September 30"},
                                'M9'    : {'m':9, "mstr":"September 30"},
                                'Q4'    : {'m':12, "mstr":"Dec 31"},
                                'FY'    : {'m':12, "mstr":"Dec 31"},
                                'H2'    : {'m':12, "mstr":"Dec 31"},
                            }
            
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            txt_d[rr['t_l']]    = {}
            continue
            for ii, ph in enumerate(f_phs):
                if ph['k'] not in rr:continue
                v_d = rr[ph['k']]
                t   = v_d['t']          
                x   = v_d['x']          
                dd  = self.read_triplet(t, txn_trip, x, txn_trip_default)
                VGH = dd.get('VGH', {})
                if VGH:
                    txt_d[' '.join(VGH.keys()[0])]   = {}
                    txt_d[rr['t_l']]    = {}
        self.detect_date(txt_d, 13)
        ph_ind  = {}
        for ii, ph in enumerate(f_phs):
            year    = ph['ph'][-4:]
            ph_ind[ph['ph']]  = ii
            txt_d[ph['ph']] = {}
            txt_d[ph['ph']]['p'] = year
            txt_d[ph['ph']]['m'] = ph_map[ph['ph'][:-4]]['m']
        import calendar
        ph_value_d  = {}
        cond_ar     = [
                        (1.0, 'Less than one year'),
                        ]
        for i in range(2, 100):
            cond_ar.append((float(i), 'Less than '+self.numToWords(i)+' years'))
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            n_row   = 0
            for ii, ph in enumerate(f_phs):
                if ph['k'] not in rr or (clean_v_d.get(ti, {})[ph['k']] == ''):continue
                v_d = rr[ph['k']]
                t   = v_d['t']          
                x   = v_d['x']          
                VGH = ph['ph']
                HGH = txt_d[rr['t_l']]
                if HGH:
                    VGH     = txt_d[VGH]
                    #print '\n==============================================================================='
                    #print 'HGH ', rr['t_l']
                    #print 'VGH', VGH
                    #print HGH
                    #print VGH
                    if VGH:
                        year_h  = HGH['p']
                        month_h = HGH['m']

                        year_v  = VGH['p']
                        month_v = VGH['m']
                        if year_h and year_v:
                            if month_h:
                                d2  = datetime.datetime.strptime('%s-%s'%(year_h, month_h), '%Y-%m')
                            else:
                                d2  = datetime.datetime.strptime('%s'%(year_h), '%Y')
                            if month_v:
                                d1  = datetime.datetime.strptime('%s-%s'%(year_v, month_v), '%Y-%m')
                            else:
                                d1  = datetime.datetime.strptime('%s'%(year_v), '%Y')
                            #print '\n==============================================================================='
                            #print 'HGH ', rr['t_l']
                            #print 'VGH', ph['ph']
                            #print HGH
                            #print VGH
                            diffy   = d2.year - d1.year
                            diff    = d2 - d1.replace(d2.year)
                            d_i_y   = calendar.isleap(d2.year) and 366 or 365
                            d_y     = abs(diffy + (diff.days + diff.seconds/86400.0)/d_i_y)
                            #print 'd_y ',[ph['ph'], clean_v_d[ti][ph['k']]],d_y
                            f_less  = 0
                            for c_r in cond_ar:
                                if d_y <= c_r[0]:
                                    ph_value_d.setdefault(c_r, {}).setdefault(ph['ph'], []).append((clean_v_d[ti][ph['k']], copy.deepcopy(rr[ph['k']])))
                                    #break
                                    f_less  = 1
                                elif f_less == 1:break
        pks = ph_value_d.keys()
        pks.sort(key=lambda x:x[0])
        for pk in pks:
            rr  = {'t_id':"N-"+str(len(data)), 't_l':'Debt '+pk[1]}
            #print '\n============================================='
            #print pk
            for k in ph_value_d[pk]:
                #print k
                v   = ph_value_d[pk][k]
                f_val   = eval('+'.join(map(lambda x:x[0], v)))
                f_val   = self.convert_floating_point(f_val)
                #print '\tf_val ',f_val
                dd      = v[0][1] 
                if 'rest_ar' in dd:
                    del dd['rest_ar']
                if 'org_d' in dd:
                    del dd['org_d']
                dd['v'] = f_val
                rr[f_phs_d[k]]   = dd
                rr.update(dd)
                clean_v_d.setdefault(len(data), {})[f_phs_d[k]] = f_val
            f_taxo      = self.gen_taxonomy(rr['t_l'])
            if f_taxo not in taxo_unq_d:
                taxo_unq_d[f_taxo]  = 1
            else:
                ol_cnt  = taxo_unq_d[f_taxo]
                taxo_unq_d[f_taxo]  = ol_cnt+1
                f_taxo  += '-'+str(ol_cnt)
            rr['f_taxo']    = f_taxo
            data.append(rr)
        return data, clean_v_d, f_k_d

    def read_triplet_ar(self, table_id, txn1, xml, txn1_default):
        triplet = {}
        gv_tree = txn1.get(self.get_quid(table_id+'^!!^'+xml))
        if not gv_tree:
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml))
        if gv_tree:
            gv_tree = eval(gv_tree)
            triplet = gv_tree['triplet']
        dd  = {}
        for tk, tv in triplet.items():
            if tk not in ['HRP']:continue
            for ii, tv1 in enumerate(tv):  
                for tv2 in tv1:
                    tflg    = ''
                    if tv2:
                        tflg    = tv2[0][2]
                        dd.setdefault(tflg, []).append(tv2)
        return dd

    def read_formula(self, ti, rr, f_phs, txn, xml_row_map, xml_col_map):
        r_key           = {}
        rs_value        = {}
        operand_rows    = []
        row_form        = {}
        row_op          = {}
        if 1:#ijson.get('form', '') == 'Y':
            g_ng, f_d, all_xml= self.check_formula(rr, f_phs, txn)
            if len(g_ng.keys())> 1:
                #print 'Error ', ti, rr['t_l'], g_ng
                #sys.exit()
                pass
            if f_d:
                #print '\n*******************************************************************'
                #print ti, rr['t_l']
                operand_rows    = {}
                for formula, xml_ids in f_d.items():
                    #print formula, len(xml_ids.keys()), len(list(sets.Set(all_xml.keys())-sets.Set(xml_ids.keys())))
                    if formula[1] == 'ROW':
                        for (table_id, pxml), c_xml in xml_ids.items():
                            tmp_rows    = {}
                            #print
                            p_k         = xml_col_map[(table_id, pxml)]
                            child_k     = []
                            for cx in c_xml[1:]:
                                cxml, op = cx.split('!!')
                                if cxml:
                                    if (table_id, cxml) in xml_col_map:
                                        child_k.append((ti,xml_col_map[(table_id, cxml)], op))
                                    else:
                                        child_k = []
                                        break
                            if child_k:
                                child_k.sort()
                                row_form.setdefault(p_k, {})[tuple(child_k)]    = 1
                                
                        
                    else:    
                        for (table_id, pxml), c_xml in xml_ids.items():
                            tmp_rows    = {}
                            #print
                            for cx in c_xml[1:]:
                                cxml, op = cx.split('!!')
                                if cxml:
                                    #print (table_id, cxml), (table_id, cxml) in xml_row_map
                                    if (table_id, cxml) in xml_row_map:
                                        trows   = xml_row_map[(table_id, cxml)]
                                        for tr in trows.keys():
                                            row_op.setdefault(tr, {})[op]   = 1
                                        tmp_rows.update(trows)
                                    else:
                                        tmp_rows    = {}
                                        break
                            trows   = tmp_rows.keys()
                            trows.sort()
                            if trows:
                                operand_rows[tuple(trows)]  = 1
                operand_rows    = self.get_overlap_rows(operand_rows)
                operand_rows.sort()
                #print len(operand_rows), operand_rows
                #print 'operand_rows   ', map(lambda x:(x, row_op[x].keys()), operand_rows)
        return r_key, rs_value, operand_rows, row_form, row_op

    def add_restated_values(self, ti, rr, v_d, dd, res, rs_value, key_map_rev, key_map_rev_t, ph, txn, dphs_ar, doc_d, csv_d, actual_v_d={}):
        odd      = self.get_gv_dict(v_d, txn, key_map_rev, ph, csv_d)
        dd1     = self.get_gv_dict(v_d, txn, key_map_rev_t, ph, csv_d)
        dd1['t'] = v_d['t']
        dd1['tn']= v_d['t']+'-'+v_d['d']+' ('+doc_d.get(v_d['d'], ('', ''))[1]+'-'+doc_d.get(v_d['d'], ('',''))[0]+')'
        dd1['x']= v_d['x']
        dd1['R']= 'Y'
        re_stated   = []
        re_stated1   = []
        #print '\n====================================='
        #print ph['ph'], [v_d['v'], v_d['t'], v_d['d']], other_values
        for rs_k in rs_value:#[ph['k']]:
            rv_d     = rr[rs_k]
            #print '\t',[rv_d['v'], rv_d['t'], rv_d['d']]
            tdd     = self.get_gv_dict(rv_d, txn, key_map_rev, ph, csv_d, actual_v_d)
            tdd[key_map_rev['ph']]   = doc_d[rv_d['d']][0]
            tdd1    = self.get_gv_dict(rv_d, txn, key_map_rev_t, ph, csv_d, actual_v_d)
            tdd1['ph']   = ph['ph'] #doc_d[rv_d['d']][0]
            re_stated.append(tdd)
            tdd1['t'] = rv_d['t']
            tdd1['tn']= rv_d['t']+'-'+rv_d['d']+' ('+doc_d[rv_d['d']][1]+'-'+doc_d[rv_d['d']][0]+')'
            tdd1['x']= rv_d['x']
            re_stated1.append(tdd1)
        if re_stated:
            res[0]['data'][ti].setdefault(ph['k'], v_d)['re_stated'] = [copy.deepcopy(dd1)]+re_stated1
            re_stated   = [copy.deepcopy(odd)]+re_stated
            dd[key_map_rev['re_stated']]    = re_stated
            for tk in ['actual_value', 'clean_value', 'd', 'pno', 'bbox', 'gv_ph', 'currency', 'scale', 'value_type']:
                if key_map_rev[tk] in re_stated[-1]:
                    dd[key_map_rev[tk]] = re_stated[-1][key_map_rev[tk]]
                elif key_map_rev[tk] in dd:
                    dd[key_map_rev[tk]] = ''

    def add_row_formula(self, ti, dd, operand_rows, res, data, ph, key_map_rev, key_map_rev_t, row_op, txn, disp_name, row_ind_map, csv_d, user_f, form_d, taxo_value_grp):
        formula = []
        d_formula   = []
        #if user_f == None:
        row_op[ti]  = {'=':1}
        for ri, rowtup in enumerate(operand_rows):
            tmp_arr = []
            d_tmp_arr = []
            operands    = []
            operand_ids    = []
            rowtup  =(ti, )+rowtup
            for rid in rowtup:
                if user_f == None:
                    if rid != ti:
                        row_op[rid]  = {'+':1}
                rv_d    = data[rid].get(ph['k'], {})
                if rv_d:
                    t       = rv_d['v']
                    clean_value = t
                    try:
                        clean_value = float(numbercleanup_obj.get_value_cleanup(t))
                    except:
                        clean_value = 0.00
                        pass
                    if 1:
                        operand_ids.append(rid)
                        operands.append(clean_value)
            if 0:#user_f == None:
                if len(operand_ids) > 1 and operand_ids[0] == ti:
                    tres = gcom_operator.check_formula_specific(operands[1:], operands[0])
                    if not tres:continue
                    
                    for i, t_r in enumerate(tres):
                        row_op[operand_ids[i+1]]    = {'+':1} if t_r == 0 else {'-':1}
            if form_d.get(ph['k'], []):
                for ft in form_d[ph['k']]:
                    if ft['type'] != 'v':
                        row_op[taxo_value_grp[ft['txid']]]      = {ft['op']:1}
            
                    
            for rid in rowtup:
                rv_d    = data[rid].get(ph['k'], {})
                if rv_d:
                    d_tmp_d   = self.get_gv_dict(rv_d, txn, key_map_rev_t, ph, csv_d)
                    d_tmp_d['t']= rv_d['t']
                    d_tmp_d['x']= rv_d['x']
                    t       = rv_d['v']
                    clean_value = t
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(t)
                    except:
                        clean_value = ''
                        pass

                    c_d = {'doc_id':rv_d['d'], 'op':row_op[rid].keys()[0], 'ph':ph['ph'], 't':data[rid]['f_taxo'], 'ty':disp_name, 'page_no':rv_d['x'].split(':@:')[0].split('#')[0].split('_')[1], 'bbox':rv_d['bbox'], 'row_id':row_ind_map[rid], 'v':clean_value, 'label':data[rid]['t_l'], 't_id':data[rid]['t_id']}
                else:
                    d_tmp_d   = {}
                    c_d = {'doc_id':'', 'op':row_op[rid].keys()[0], 'ph':ph['ph'], 't':data[rid]['f_taxo'], 'ty':disp_name, 'page_no':'', 'bbox':'', 'row_id':row_ind_map[rid], 'v':'', 'label':data[rid]['t_l'], 't_id':data[rid]['t_id']}
                tmp_arr.append(c_d)

                d_tmp_d[key_map_rev_t['description']]   = data[rid]['t_l']
                d_tmp_d[key_map_rev_t['taxo_id']]       = data[rid]['t_id']
                d_tmp_d[key_map_rev_t['operator']]      = row_op[rid].keys()[0]
                if ti   == rid:
                    d_tmp_d['R']    = 'Y'
                d_tmp_arr.append(d_tmp_d)
            formula.append(tmp_arr)
            d_formula.append(d_tmp_arr)
        if formula:
            dd[key_map_rev['f_col']]  = formula
        if d_formula:
            res[0]['data'][ti][ph['k']]['f_col'] = d_formula
            res[0]['data'][ti][ph['k']]['g_f'] = 'Y'
            res[0]['data'][ti][ph['k']]['f'] = 'Y'
            res[0]['data'][ti][ph['k']]['fv'] = 'Y'
            data[ti][ph['k']]['f_col'] = d_formula
            data[ti][ph['k']]['g_f'] = 'Y'
            data[ti][ph['k']]['f'] = 'Y'
            data[ti][ph['k']]['fv'] = 'Y'

    def add_column_formula(self, ti, dd, row_form, res, rr, ph, data, key_map_rev, key_map_rev_t, txn, disp_name, csv_d):
        formula = []
        rid = ti
        d_formula = []
        for col_k in row_form.get(ph['k'], {}).keys():
            tmp_arr     = []
            d_tmp_arr   = []
            col_k       = ((ti, ph['k'], '='), )+col_k
            for c_k in col_k:
                rid, op, c_k = c_k
                rv_d    = data[rid].get(c_k, {})
                t       = rv_d['v']
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                if rv_d:
                    c_d = {'doc_id':rv_d['d'], 'op':op, 'ph':ph['ph'], 't':data[rid]['f_taxo'], 'ty':disp_name, 'page_no':rv_d['x'].split(':@:')[0].split('#')[0].split('_')[1], 'bbox':rv_d['bbox'], 'v':clean_value, 'label':data[rid]['t_l']}
                else:
                    c_d = {'doc_id':'', 'op':op, 'ph':ph['ph'], 't':data[rid]['f_taxo'], 'ty':disp_name, 'page_no':'', 'bbox':'', 'row_id':row_ind_map[rid], 'v':'', 'label':data[rid]['t_l']}
                tmp_arr.append(c_d)

                if rv_d:
                    d_tmp_d   = self.get_gv_dict(rv_d, txn, key_map_rev_t, ph_map[c_k], csv_d)
                    d_tmp_d['t']= rv_d['t']
                    d_tmp_d['x']= rv_d['x']
                else:
                    d_tmp_d   = {}
                d_tmp_d[key_map_rev_t['description']]   = res[0]['data'][rid]['t_l']
                d_tmp_d[key_map_rev_t['taxo_id']]   = res[0]['data'][rid]['t_id']
                d_tmp_d[key_map_rev_t['operator']]      = op
                if op == '=':
                    d_tmp_d['R']    = 'Y'
                d_tmp_arr.append(d_tmp_d)
        if formula:
            dd[key_map_rev['f_row']]  = formula
        if d_formula:
            res[0]['data'][ti][ph['k']]['f_row'] = d_formula


    def check_formula(self, rr, f_phs, txn):
        f_d = {}
        all_xml = {}
        g_ng    = {}
        for ii, ph in enumerate(f_phs):
            if ph['k'] not in rr:continue
            v_d = rr[ph['k']]
            if v_d.get('rest_ar', []):continue
            table_id    = v_d['t']
            key         = table_id+'_'+self.get_quid(v_d['x'])
            all_xml[v_d['x']] = 1
            for gflg in ['G', 'NG']:
                for rc in ['COL', 'ROW']:
                    gcom        = txn.get('COM_OP_'+gflg+'_'+rc+'MAP_'+key)
                    if gcom:
                        for eq in gcom.split('|'):
                            formula, eq_str = eq.split(':$$:')
                            g_ng[gflg]=1
                            #f_taxo_arr[ii].setdefault('gcom', {})[c_id] = formula
                            f_d.setdefault((gflg, rc, formula), {})[(table_id, v_d['x'])]   = eq_str.split('^')
        return g_ng,f_d, all_xml


    def read_final_output(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        path         = '/mnt/eMB_db/%s/%s/FINAL_OUTPUT/'%(company_name, model_number)
        env1        = lmdb.open(path)
        text        = ijson.get('text', '')
        res = [{'message':'Error'}]
        with env1.begin() as txn1:
            op  = txn1.get('DISPLAYOUTPUT_'+str(text))
            if op:
                res = eval(op)
        return res

    def read_final_output_demo(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        path        = '/mnt/eMB_db/%s/%s/FINAL_OUTPUT/'%(company_name, model_number)
        env1        = lmdb.open(path)
        text        = ijson.get('ph', 'P')
        res         = [{'message':'Error'}]
        k           = ijson['table_type']
        with env1.begin() as txn1:
            op  = txn1.get('OUTPUT_'+str(k))
            if op:
                res = eval(op)
                
                pass
        return []

    def read_final_output_ks(self, ijson):
        res = [{'message':'done', 'data':[]}]
        return res
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        path         = '/mnt/eMB_db/%s/%s/FINAL_OUTPUT/'%(company_name, model_number)
        env1        = lmdb.open(path)
        text        = ijson.get('text', '')
        table_type  = ijson['table_type']
        arr = [{'k':'null', 'n':''}]
        order_d     = self.order_d
        txtpath      = '/var/www/html/DB_Model/%s/'%(company_name)
        os.system("mkdir -p "+txtpath)
        with env1.begin() as txn1:
            ks  = []
            for k, v in txn1.cursor():
                if 'DISPLAYOUTPUT_' in k:
                    try:
                        k_sp    = k.split('DISPLAYOUTPUT_')[1].split('-')[0][0]
                    except:continue
                    if str(order_d[table_type]) != k_sp :continue
                    fname   = txtpath+'/'+k.split('DISPLAYOUTPUT_')[1]+'.txt'
                    if not os.path.exists(fname):continue
                    ks.append(k.split('DISPLAYOUTPUT_')[1])
            for k in ks:
                #try:
                txt_sp =txn1.get('TEXT_VALUE_'+k).split(':@@:')
                txt = binascii.a2b_hex(txt_sp[0])
                arr.append({'k':k, 'n':txt, 'ph':''.join(txt_sp[1:])})
                #if table_type   == arr[-1]['n']:
                #    arr[-1]['n']    = 'ALL'
                #except:pass
        res = [{'message':'done', 'data':arr}]
        return res


    def get_gv_dict(self, v_d, txn, key_map_rev, ph, csv_d, actual_v_d={}):
        t       = v_d['v']
        clean_value = t
        try:
            clean_value = numbercleanup_obj.get_value_cleanup(t)
        except:
            clean_value = ''
            pass
        dd  = {
                key_map_rev['actual_value']     : v_d['v'],
                key_map_rev['clean_value']      : clean_value if str(v_d['t']) not in actual_v_d else v_d['v'],
                key_map_rev['d']                : v_d['d'],
                key_map_rev['pno']              : v_d['x'].split(':@:')[0].split('#')[0].split('_')[1] if v_d['x'] else '',
                key_map_rev['bbox']             : v_d['bbox'],
                key_map_rev['ph']               : ph['ph'],
                key_map_rev['table_id']         : v_d['t'],
                key_map_rev['xml_id']           : v_d['x'],
                }
        
        key     = v_d['t']+'_'+self.get_quid(v_d['x'].strip('RESTATED_'))
        ph_map  = txn.get('PH_MAP_'+str(key))
        if ph_map:
            period_type, period, currency, scale, value_type    = ph_map.split('^')
            if(v_d['t'], v_d['x']) in csv_d    :
                period_type, period, currency, scale, value_type    = csv_d[(v_d['t'], v_d['x'])]
            dd[key_map_rev['gv_ph']]        = period_type+period
            if v_d.get('fv', '') == 'Y':
                dd[key_map_rev['gv_ph']]        = ph['ph']
            dd[key_map_rev['currency']]  = currency
            dd[key_map_rev['scale']]     = scale
            dd[key_map_rev['value_type']]= value_type
        return dd

    def clean_value(self, txt):
        return txt

    def insert_new_taxonomy(self, ijson):
        import create_table_seq
        obj = create_table_seq.TableSeq()
        return obj.insert_new_taxonomy(ijson)

    def re_order(self, ijson):
        return self.re_order_seq(ijson)
        return
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        #m_tables, rev_m_tables, doc_m_d = self.get_main_table_info(company_name, model_number)
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        #c_year      = int(datetime.datetime.now().strftime('%Y'))
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):continue
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])
        #m_path          = self.taxo_path%(company_name, model_number)
        #path            = m_path+'/TAXO_RESULT/'
        #env1        = lmdb.open(path, max_dbs=27)
        #db_name1    = env1.open_db('other')
        #txn         = env1.begin(db=db_name1)

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        #lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        #lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        #env1    = lmdb.open(lmdb_path)
        #txn1    = env1.begin()
        table_ph_d  = {}
        all_ph_d    = {}
        table_type  = str(i_table_type)
        f_taxo_arr  = [] #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        table_ids   = {}
        g_ar        = []
        table_col_phs   = {}
        ph_d        = {}
        phcsv_d     = {}
        if not taxo_d:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            if not ijson.get('vids', {}):
                sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
                try:
                    cur.execute(sql)
                    res = cur.fetchall()
                except:
                    res = []
                grp_info    = {}
                for rr in res:
                    group_id, table_type, group_txt = rr
                    grp_info[str(group_id)]   = group_txt
                sql         = "select vgh_id, group_txt from vgh_group_map where table_type='%s'"%(table_type)
                cur.execute(sql)
                res         = cur.fetchall()
                group_d     = {}
                for rr in res:
                    vgh_id, group_txt   = rr
                    group_d.setdefault(group_txt, {})[vgh_id]   = 1
                g_ar    = []
                for k, v in group_d.items():
                    dd  = {'n':grp_info.get(k, k), 'vids':v.keys(), 'grpid':k}
                    g_ar.append(dd)
            if not ijson.get('vids', {}):
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, isvisible from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            else:
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, isvisible from mt_data_builder where table_type='%s' and isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(ijson['vids']))
            cur.execute(sql)
            res         = cur.fetchall()
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, isvisible    = rr
                doc_id      = str(doc_id)
                if doc_id not in doc_d:continue
                table_id    = str(table_id)
                if table_id not in m_tables:continue
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                gv_xml  = c_id
                c   = int(c_id.split('_')[2])
                table_col_phs.setdefault((table_id, c), {})[ph]   = table_col_phs.setdefault((table_id, c), {}).get(ph, 0) +1
                table_ids[table_id]   = 1
                comp    = ''
                #if gcom == 'Y' or ngcom == 'Y':
                #    comp    = 'Y'
                taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'rid':row_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows})['ks'].append((table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id, xml_id))
                if vgh_group == 'N':
                    taxo_d[taxo_id]['l_change'][xml_id]  = 1
            conn.close()
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
        f_ar        = []
        done_d      = {}
        #for dd in f_taxo_arr:
        tmptable_col_phs    = {}
        for k, v in table_col_phs.items():
            phs = v.keys()
            phs.sort(key=lambda x:v[x], reverse=True)
            tmptable_col_phs[k] = phs[0]
        row_ids = taxo_d.keys()
        row_ids.sort(key=lambda x:len(taxo_d[x]['ks']), reverse=True)
        f_taxo_arr   = []
        for row_id in row_ids:
            dd  = {'t_l':[str(row_id)], 'ks':map(lambda x:(str(x[0]), str(x[1])), taxo_d[row_id]['ks'])}
            f_taxo_arr.append(dd)
        import create_table_seq
        obj = create_table_seq.TableSeq()
        ph_d    = {}
        phcsv_d = {}
        f_taxo_ar  = obj.resolve_conflict_by_gcom(f_taxo_arr, txn_m, txn, ph_d, phcsv_d, doc_m_d)
        new_order   = []
        new_order_d = {}
        for ii, rr in enumerate(f_taxo_ar):
            new_order_d[rr['t_l'][0]] = ii+1
            new_order.append((ii+1, rr['t_l'][0], table_type))
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        cur.executemany("update mt_data_builder set order_id=? where taxo_id=? and table_type=?", new_order)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'order_d':new_order_d}]
        return res
            
            



        

        

        
    


        
    

            
            


        
        
        


    def find_missing_table(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number)
        db_file         = self.get_db_path(ijson)
        print db_file
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id,m_rows from mt_data_builder"
        cur.execute(sql)
        res             = cur.fetchall()
        table_ds        = {}
        for rr in res:
            table_type, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, vgh_text, vgh_group, doc_id, xml_id, m_rows = rr
            if xml_id:
                table_ds[table_id]        = (table_type, doc_id)
        conn.close()
        missing_table   = list(sets.Set(m_tables.keys()) - sets.Set(table_ds.keys()))
        print len(missing_table), missing_table
        ijson['table_ids']  = missing_table
        #self.add_new_table(ijson)


    def re_order_seq(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        #m_tables, rev_m_tables, doc_m_d = self.get_main_table_info(company_name, model_number)
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        #c_year      = int(datetime.datetime.now().strftime('%Y'))
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):continue
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])
        #m_path          = self.taxo_path%(company_name, model_number)
        #path            = m_path+'/TAXO_RESULT/'
        #env1        = lmdb.open(path, max_dbs=27)
        #db_name1    = env1.open_db('other')
        #txn         = env1.begin(db=db_name1)

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        #lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        #lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        #env1    = lmdb.open(lmdb_path)
        #txn1    = env1.begin()
        table_ph_d  = {}
        all_ph_d    = {}
        table_type  = str(i_table_type)
        f_taxo_arr  = [] #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        table_ids   = {}
        g_ar        = []
        table_col_phs   = {}
        ph_d        = {}
        phcsv_d     = {}
        table_xml_d = {}
        if not taxo_d:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            if not ijson.get('vids', {}):
                sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
                try:
                    cur.execute(sql)
                    res = cur.fetchall()
                except:
                    res = []
                grp_info    = {}
                for rr in res:
                    group_id, table_type, group_txt = rr
                    grp_info[str(group_id)]   = group_txt


                sql         = "select vgh_id, group_txt from vgh_group_map where table_type='%s'"%(table_type)
                cur.execute(sql)
                res         = cur.fetchall()
                group_d     = {}
                for rr in res:
                    vgh_id, group_txt   = rr
                    group_d.setdefault(group_txt, {})[vgh_id]   = 1
                g_ar    = []
                for k, v in group_d.items():
                    dd  = {'n':grp_info.get(k, k), 'vids':v.keys(), 'grpid':k}
                    g_ar.append(dd)
            if not ijson.get('vids', {}):
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, isvisible from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            else:
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, isvisible from mt_data_builder where table_type='%s' and isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(ijson['vids']))
            cur.execute(sql)
            res         = cur.fetchall()
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, isvisible    = rr
                doc_id      = str(doc_id)
                if doc_id not in doc_d:continue
                table_id    = str(table_id)
                if table_id not in m_tables:continue
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                gv_xml  = c_id
                c   = int(c_id.split('_')[2])
                table_col_phs.setdefault((table_id, c), {})[ph]   = table_col_phs.setdefault((table_id, c), {}).get(ph, 0) +1
                table_ids[table_id]   = 1
                comp    = ''
                #if gcom == 'Y' or ngcom == 'Y':
                #    comp    = 'Y'
                taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'rid':row_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows})['ks'].append((table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id, xml_id))
                if (deal_id == '221' and table_type=='RBG') or (deal_id == '44' and table_type == 'OS'):
                    c_ph        = ph
                    c_tlabel    = tlabel
                else:
                    c_ph        = str(c)
                    c_tlabel    = ''
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, ph)
                else:
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, ph)
                if vgh_group == 'N':
                    taxo_d[taxo_id]['l_change'][xml_id]  = 1
                
                x    = self.clean_xmls(xml_id)
                if x:
                    x    = x.split(':@:')[0].split('#')[0]
                    table_xml_d.setdefault(table_id, {})[(int(x.split('_')[1]), int(x.split('_')[0].strip('x')))]   = 1
            conn.close()
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
        f_ar        = []
        done_d      = {}
        #for dd in f_taxo_arr:
        tmptable_col_phs    = {}
        for k, v in table_col_phs.items():
            phs = v.keys()
            phs.sort(key=lambda x:v[x], reverse=True)
            tmptable_col_phs[k] = phs[0]
        row_ids = taxo_d.keys()
        row_ids.sort(key=lambda x:len(taxo_d[x]['ks']), reverse=True)
        f_taxo_arr   = []
        for row_id in row_ids:
            dd  = {'t_l':[str(row_id)], 'ks':map(lambda x:(str(x[0]), str(x[1])), taxo_d[row_id]['ks'])}
            f_taxo_arr.append(dd)
        import create_table_seq
        obj = create_table_seq.TableSeq()
        ph_d    = {}
        phcsv_d = {}
        dphs = report_year_sort.year_sort(dphs.keys())
        dphs.reverse()
        table_ids   = table_ph_d.keys()
        #try:
        #table_ids.sort(key=lambda x:(dphs.index(doc_d[x[0]][0]), x[1]))
        table_ids.sort(key=lambda x:(dphs.index(doc_d[x[0]][0]), sorted(table_xml_d[x[1]].keys())[0]))
        #except:
        #    table_ids.sort(key=lambda x:(x[0], x[1]))
        #    pass
        f_taxo_ar  = self.order_by_table_structure(f_taxo_arr, txn_m, txn, ph_d, phcsv_d, doc_m_d,table_ids)
        new_order   = []
        new_order_d = {}
        for ii, rr in enumerate(f_taxo_ar):
            #print ii,'. ', rr['t_l'][0]
            new_order_d[rr['t_l'][0]] = ii+1
            new_order.append((ii+1, rr['t_l'][0], table_type))
        if ijson.get('update_db', '') == 'Y':# and str(deal_id) in ['219', '214', '221']:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            cur.executemany("update mt_data_builder set order_id=? where taxo_id=? and table_type=?", new_order)
            conn.commit()
            conn.close()
            pass
        res = [{'message':'done', 'order_d':new_order_d}]
        return res

    def clean_xmls(self, xml):
        xml     = xml.replace('@@@', ':@:') 
        fxml    = []
        for x in xml.split(':@:'):
            xml_lst     = []
            for tx in x.split('#'):
                if tx.strip():
                    xml_lst.append(tx)
            x   = str('#'.join(xml_lst))
            fxml.append(x)
        return ':@:'.join(fxml)

    def order_by_table_structure(self, f_taxo_arr, txn_m, txn, ph_d, phcsv_d, doc_m_d, table_ids):
        tmptable_ids   = map(lambda x:x[1], table_ids)
        table_match_d   = {}
        for ti, dd in enumerate(f_taxo_arr):
            ks      = dd['ks']
            for table_id, c_id in ks:
                table_match_d.setdefault(table_id, {}).setdefault(ti, {})[c_id] = 1
        tabl_ids    = table_match_d.keys()
        tabl_ids.sort(key=lambda x:tmptable_ids.index(x))
        #tabl_ids.sort(key=lambda x:len(table_match_d[x].keys()), reverse=True)
        final_arr   = []
        for table_id in tabl_ids:
            inds    = table_match_d[table_id].keys()
            inds.sort()
            #print '\n==========================================================='
            #print table_id, sorted(inds)
            #inds.sort(key=lambda x:int(table_match_d[table_id][x].keys()[0].split('_')[1]))
            inds.sort(key=lambda x:int(sorted(map(lambda x1:int(x1.split('_')[1]), table_match_d[table_id][x].keys()))[0]))
            #print 'Ordered ', inds
            if not final_arr:
                final_arr   = inds
            else:
                m_d = list(sets.Set(final_arr).intersection(sets.Set(inds)))
                deletion    = {}
                tmp_arr     = []
                ftmp_arr     = []
                for t in inds:
                    if t in m_d:
                        ftmp_arr    = []
                        if tmp_arr:
                            deletion[t] = copy.deepcopy(tmp_arr[:])
                            #tmp_arr = []    
                        continue
                    tmp_arr.append(t)
                    ftmp_arr.append(t)
                done_d  = {}
                m_d.sort(key=lambda x:final_arr.index(x))
                
                for t in m_d:
                    if t in deletion:
                        tmp_arr = []
                        for t1 in deletion[t]:
                            if t1 not in done_d:
                                tmp_arr.append(t1)
                                done_d[t1]  = 1
                        index       = final_arr.index(t)
                        final_arr   = final_arr[:index]+tmp_arr+final_arr[index:]    
                
                if ftmp_arr:
                    final_arr   = final_arr+ftmp_arr
            
            #print 'FINAL ', final_arr
        missing = sets.Set(range(len(f_taxo_arr))) - sets.Set(final_arr)
        if len(final_arr) > len(f_taxo_arr):
            print 'Duplicate ', final_arr
            sys.exit()
        if len(missing):
            print 'missing ', list(missing)
            sys.exit()
        f_taxo_arr  = map(lambda x:f_taxo_arr[x], final_arr)
        return f_taxo_arr

    def auto_validate_order(self, ijson):
        deal_ids         = ijson['deal_ids']
        cinfo   = self.read_company_info({"cids":deal_ids})
        order_missmatch = {}
        done            = {}
        all_t           = {}
        for deal_id in deal_ids:
            ijson   = cinfo[int(deal_id)]
            company_name    = ijson['company_name']
            mnumber         = ijson['model_number']
            model_number    = mnumber
            m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
            for ii, table_type in enumerate(rev_m_tables.keys()):
                print 'Running ', ii, ' / ', len(rev_m_tables.keys()), [table_type, ijson['company_name']]
                ijson['table_type'] = table_type
                res                 = self.create_seq_across(ijson)
                phs                 = copy.deepcopy(res[0]['phs'])
                data                = copy.deepcopy(res[0]['data'])
                order               = self.re_order_seq(ijson)[0]['order_d']
                key                 = (table_type,  ijson['company_name'])
                all_t[key]           = len(order.keys())
                tmp_md              = {}
                for ii, rr in enumerate(data):
                    if order[str(rr['t_id'])] != ii+1:
                        tmp_md[rr['t_id']]  = (rr['t_l'], ii+1, order[str(rr['t_id'])])
                if tmp_md:
                    order_missmatch[key]    = tmp_md
                else:
                    done[key]               = 1
        t   = len(all_t.keys())
        ks  = order_missmatch.keys()
        ks.sort(key=lambda x:len(order_missmatch[x].keys()))
        for k in ks:
            v   = order_missmatch[k]
            print k, len(v.keys()), ' / ', all_t[k]
            for k1, v1 in v.items():
                print '\t', k1, v1
        print 'Total ', t
        print 'Matched  ', len(done.keys())
        print 'Not Matched  ', len(order_missmatch.keys())
        
    def validate_cell_data(self, data, phs, actual_v_d):
        dmap = {'0':1, '1':1, '2':1, '3':1, '4':1, '5':1, '6':1, '7':1, '8':1, '9':1}
        invalid_cell_dict = {}
        for ph_dict in phs:
            mkey = ph_dict['k']
            for ddict in data:
                val_map = ddict.get(mkey, {})
                taxo_id = ddict.get('t_id', '')
                if not taxo_id:continue
                if val_map and str(val_map.get('t', '')) not in actual_v_d:
                    cell_txt    = self.convert_html_entity(val_map.get('v', ''))
                    final_cell_txt = ''
                    for t in cell_txt:
                        if t in dmap:
                            final_cell_txt += t
                    
                    if final_cell_txt.strip():
                        clean_value = cell_txt
                        try:
                            clean_value = numbercleanup_obj.get_value_cleanup(cell_txt)
                        except:
                            clean_value = ''
                        if (not clean_value.strip()):
                            invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 1

                    cell_bbox   = val_map.get('bbox', [])
                    if not cell_bbox:
                        if not invalid_cell_dict.get(taxo_id, {}).get(mkey, ''):
                            invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 2
                        else:
                            invalid_cell_dict.setdefault(taxo_id, {})[mkey] = 3
        #print invalid_cell_dict
        return invalid_cell_dict

    def disable_duplicate(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = map(lambda x:str(x), ijson['t_ids'])
        sql             = "update mt_data_builder set isvisible='N' where table_type='%s' and taxo_id in (%s)"%(table_type, ','.join(t_ids))
        cur.execute(sql)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'd_ids':t_ids}]
        return res

    def restore_taxo(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type      = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        t_ids            = map(lambda x:str(x), ijson['t_ids'])
        sql             = "update mt_data_builder set isvisible='Y' where table_type='%s' and taxo_id in (%s)"%(table_type, ','.join(t_ids))
        cur.execute(sql)
        conn.commit()
        conn.close()
        res = [{'message':'done', 'd_ids':t_ids}]
        return res

    def insert_ph_formula_frm_data(self, ijson):    
        dd  = [
                    'Q4=FY-Q1-Q2-Q3',
                    'Q3=FY-Q1-Q2-Q4',
                    'Q2=FY-Q1-Q3-Q4',
                    'Q1=FY-Q2-Q3-Q4',
                    'FY=Q1+Q2+Q3+Q4',
                    'FY=H1+H2',
                    'H2=FY-H1',
                    'H2=FY',
                    'H1=Q1+Q2',
                    'Q2=H1-Q1',
                    'Q1=H1-Q2',
                    'Q3=M9',
                    'Q2=H1',
                    'Q4=FY',
                    'FY=Q4',
                    'Q4=FY-M9',
                    'Q3=M9-H1',
                ]
        ijson['data']   = dd
        ijson['user']   = 'SYSTEM'
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql = 'drop table if exists ph_formula'
        cur.execute(sql)
        conn.commit()
        conn.close()
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
        for table_type in rev_m_tables.keys():
            ijson['table_type'] = table_type
            self.insert_ph_formula(ijson)
        return self.read_ph_formula(ijson)

    def insert_ph_formula(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type  = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql         = 'CREATE TABLE IF NOT EXISTS ph_formula(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type TEXT, ph TEXT, formula TEXT, operands TEXT, user_name TEXT, datetime TEXT)'
        cur.execute(sql)
        sql         = "select ph, formula, operands from ph_formula where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        exists      = {}
        for rr in res:
            ph, formula, operands   = rr
            operands    = operands.split(',')
            operands.sort()
            exists[(ph, formula, tuple(operands))]  = 1
        dtime   = str(datetime.datetime.now()).split('.')[0]
        user    = ijson['user']
        i_ar    = []
        opr     = {
                    '=' : 1,
                    '+' : 1,
                    '-' : 1,
                    '*' : 1,
                    '/' : 1,
                    '(' : 1,
                    ')' : 1,
                    }
        for rr in ijson['data']:
            if not rr.strip():continue
            ph, op_str  = rr.split('=')
            formula = rr
            operands    = []
            tmp_ar  = []
            for c in op_str:
                if c in opr:
                    if tmp_ar:
                        operands.append(''.join(tmp_ar))
                        tmp_ar  = []
                elif c:
                    tmp_ar.append(c)
            if tmp_ar:
                operands.append(''.join(tmp_ar))
            operands.sort()
            operands    = tuple(operands)
            if (ph, formula, operands) not in exists:
                i_ar.append((table_type, ph, formula, ','.join(operands), user, dtime))
        cur.executemany('insert into ph_formula(table_type, ph, formula, operands, user_name, datetime)values(?,?,?,?,?,?)', i_ar)
        conn.commit()
        conn.close()
        return self.read_ph_formula(ijson)
    
    def read_ph_formula(self, ijson, ret_flg=None):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type  = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql         = 'CREATE TABLE IF NOT EXISTS ph_formula(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type TEXT, ph TEXT, formula TEXT, operands TEXT, user_name TEXT, datetime TEXT)'
        cur.execute(sql)
        sql         = "select row_id, ph, formula, operands from ph_formula where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()
        ph_f_map    = {}
        map_d       = {}
        for rr in res:
            row_id, ph, formula, operands   = rr
            map_d[formula]       = row_id
            dd  = {'f':formula, 'op':operands.split(','), 'g_id':row_id, 'ph':ph}
            ph_f_map.setdefault(ph, []).append(dd) 
        phs = ph_f_map.keys()
        phs.sort()
        f_ar    = []
        for ph in phs:
            f_ar    += ph_f_map[ph]
        if ret_flg == 'Y':
            return ph_f_map, f_ar, map_d
        res = [{'message':'done', 'data':f_ar}]
        return res

    def read_ph_config_formula(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type  = ijson['table_type']
        opr     = {
                    '=' : 1,
                    '+' : 1,
                    '-' : 1,
                    '*' : 1,
                    '/' : 1,
                    '(' : 1,
                    ')' : 1,
                    }
        deriv_phs   = self.get_deriv_phs(ijson)
        db_file         = '/mnt/eMB_db/node_mapping.db' #self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql         = 'select period, period_expression from table_type_wise_formula where table_type="%s" and project_name="%s"'%(table_type, ijson.get('project_name', ''))
        try:
            cur.execute(sql)
            res         = cur.fetchall()
        except:
            res  = []
        ph_formula_d    = {}
        for rr in res:
            period, period_expression    = rr
            if period not in deriv_phs:continue
            if period and period_expression:
                formula = period_expression
                if '=' not in formula:
                    formula =period+'='+formula
                #print formula
                operands    = []
                tmp_ar      = []
                tph, op_str  = formula.split('=')
                for c in op_str:
                    #print '\t',[c, c in opr]
                    if c in opr:
                        if tmp_ar:
                            operands.append({'t':'op', 'v':''.join(tmp_ar)})
                            tmp_ar  = []
                        operands.append({'t':'opr', 'v':c})
                    elif c:
                        tmp_ar.append(c)
                #print [tmp_ar]
                if tmp_ar:
                    operands.append({'t':'op', 'v':''.join(tmp_ar)})
                #print operands
                if operands:
                    ph_formula_d.setdefault(period, []).append((formula, operands))
        return ph_formula_d


    def read_ph_user_formula(self, ijson, group_id=None, tph_formula_d={}):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        table_type  = ijson['table_type']
        opr     = {
                    '=' : 1,
                    '+' : 1,
                    '-' : 1,
                    '*' : 1,
                    '/' : 1,
                    '(' : 1,
                    ')' : 1,
                    }
        deriv_phs   = self.get_deriv_phs(ijson)
        #print ijson
        #print deriv_phs
        #sys.exit()
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        if group_id:
            group_id_sp = [group_id]
            if '-' in group_id:
                group_id_sp.append(group_id.split('-')[0])
            group_id_sp = map(lambda x:'"'+x+'"', group_id_sp)
            sql         = 'select row_id, ph, formula, formula_type, formula_str, group_id from ph_derivation where table_type="%s" and group_id in (%s)'%(table_type, ','.join(group_id_sp))
        else:
            sql         = 'select row_id, ph, formula, formula_type, formula_str, group_id from ph_derivation where table_type="%s"'%(table_type)
        try:
            cur.execute(sql)
            res         = cur.fetchall()
        except:
            res  = []
        ph_formula_d    = copy.deepcopy(tph_formula_d)
        for rr in res:
            row_id, ph, formula, formula_type, formula_str, tgroup_id    = rr
            if not group_id and tgroup_id:continue
            row_id  = 'RID-'+str(row_id)
            if formula_type == 'CF':
                sph                 = ph.split('-')
                F_taxoid, CF_cellid, rowflg, colflg, year_diff = formula.split('^') 
                year_diff   = int(year_diff)
                ph_formula_d.setdefault(row_id, {'i':{}, 'v':(F_taxoid, CF_cellid, sph[0], sph[1])})
                ph_formula_d[row_id]['r']  = True if rowflg == '1' else False
                ph_formula_d[row_id]['c']  = True if colflg == '1' else False
                for t_k in formula_str.split('##'):
                    c_sp    = t_k.split('$')
                    taxo_id = c_sp[0]
                    for cell_id in c_sp[1:]:
                        cell_id = cell_id.split('^^')[0]
                        if rowflg == '1':
                            ph_formula_d.setdefault(('CF', taxo_id), {})[sph[0]]   = (row_id, F_taxoid, CF_cellid, sph[1], year_diff)
                        else:
                            ph_formula_d.setdefault(('CF', taxo_id), {})[cell_id]   = (row_id, F_taxoid, CF_cellid, sph[1], year_diff)
                        ph_formula_d[row_id]['i'][(taxo_id, cell_id)] = 1
                        
                continue
            elif formula_type == 'SCALE':
                for t_k in formula_str.split('##'):
                    c_sp    = t_k.split('$')
                    taxo_id = c_sp[0]
                    for cell_id in c_sp[1:]:
                        cell_id = cell_id.split('^^')[0]
                        ph_formula_d.setdefault(('S', taxo_id), {})[cell_id]    = (row_id, formula)
                continue
            elif formula_type == 'PH':
                #print formula
                operands    = []
                tmp_ar      = []
                tph, op_str  = formula.split('=')
                if tph not in deriv_phs:continue
                for c in op_str:
                    #print '\t',[c, c in opr]
                    if c in opr:
                        if tmp_ar:
                            operands.append({'t':'op', 'v':''.join(tmp_ar)})
                            tmp_ar  = []
                        operands.append({'t':'opr', 'v':c})
                    elif c:
                        tmp_ar.append(c)
                #print [tmp_ar]
                if tmp_ar:
                    operands.append({'t':'op', 'v':''.join(tmp_ar)})
                #print operands
                if operands:
                    for t_k in formula_str.split('##'):
                        c_sp    = t_k.split('$')
                        taxo_id = c_sp[0]
                        for cell_id in c_sp[1:]:
                            cell_id = cell_id.split('^^')[0]
                            #print [taxo_id, cell_id, formula, operands]
                            ph_formula_d.setdefault(taxo_id, {}).setdefault(cell_id, []).append((formula, operands))
                            ph_formula_d[taxo_id][('rid', cell_id)] = row_id
            elif formula_type == 'FORMULA':
                #print ph_formula
                operands    = []
                done_taxo   = {}
                for topr in formula.split('$$'):
                    tid, operator, t_type, g_id, ftype = topr.split('@@')
                    if tid in done_taxo:
                        continue
                    done_taxo[tid]  = 1
                    operands.append({'txid':tid, 'type':ftype, 't_type':t_type, 'g_id':g_id, 'op':operator})
                #print operands
                if operands:
                    for t_k in formula_str.split('##'):
                        c_sp    = t_k.split('$')
                        taxo_id = c_sp[0]
                        for cell_id in c_sp[1:]:
                            ph_formula_d[('F', taxo_id)]  = (row_id, operands)
            elif formula_type == 'SFORMULA':
                #print ph_formula
                operands    = []
                r_rid   = ''
                done_taxo   = {}
                for topr in formula.split('$$'):
                    tid, operator, t_type, g_id, ftype = topr.split('@@')
                    if operator == '=':
                        r_rid   = tid
                    if tid in done_taxo:
                        continue
                    done_taxo[tid]  = 1
                    operands.append({'txid':tid, 'type':ftype, 't_type':t_type, 'g_id':g_id, 'op':operator})
                if operands and r_rid:
                    ph_formula_d[('SYS F', r_rid)]  = (row_id, operands)
                    for t_k in formula_str.split('##'):
                        c_sp    = t_k.split('$')
                        taxo_id = c_sp[0]
                        for cell_id in c_sp[1:]:
                            cell_id = cell_id.split('^^')[0]
                            if cell_id.split('^^')[1:]:
                                ph_formula_d.setdefault(("OP", row_id), {})[(taxo_id, cell_id)]  = cell_id.split('^^')[1:]
                    
            elif formula_type == 'CFFORMULA':
                rph, rowflg = ph.split('^') 
                #print ph
                operands    = []
                for topr in formula.split('$$'):
                    tid, operator, t_type, g_id, ftype = topr.split('@@')
                    tid, phk, tph   = tid.split('^')
                    year_diff   = int(rph[-4:]) - int(tph[-4:])
                    if rowflg == '1':
                        operands.append({'txid':tid, 'type':ftype, 't_type':t_type, 'g_id':g_id, 'op':operator, 'period':tph[:-4], 'yd':year_diff})
                    else:
                        operands.append({'txid':tid, 'type':ftype, 't_type':t_type, 'g_id':g_id, 'op':operator, 'k':phk})
                #print operands
                if operands:
                    for t_k in formula_str.split('##'):
                        c_sp    = t_k.split('$')
                        taxo_id = c_sp[0]
                        for cell_id in c_sp[1:]:
                            cell_id = cell_id.split('^^')[0]
                            if rowflg == '1':
                                ph_formula_d.setdefault(('CF_F', taxo_id), {})[rph[:-4]]  = (row_id, rowflg, operands)
                            else:
                                ph_formula_d.setdefault(('CF_F', taxo_id), {})[cell_id]  = (row_id, rowflg, operands)
        return ph_formula_d

    ## EVALUATION 
    def get_formula_evaluation(self, formula, taxo_id_dict, phs, ph_map_d={}, year=None, run_all=None, sys_op_d={}):
        res     = {} #formula[0]
        opers   = [] #formula[1:]
        for rr in formula:
            if rr['op'] == '=':
                res = rr
            else:
                opers.append(rr)

        res_taxo_id = str(res['txid'])

        def get_eval(taxo_id, ph, res_val, sys_op_inds):
            val_li      = []
            res_txid    = res['txid']
            #res_tt      = res['t_type']
            re_ar       = []
            expr_str    = []
            #print '\n========================'
            op_inds = []
            scale_d = {}
            v_ar        = []
            for oper in opers:
                op_txid    = oper['txid']
                op         = oper['op']
                op_type    = oper['type']
                tph         = ph
                if oper.get('k', ''):
                    tph  = oper['k']
                elif oper.get('period', ''):
                    tph  = ph_map_d.get(oper['period']+str(int(year)+int(oper['yd'])), '')
                if op_type == 'v':
                    pass
                else:
                    op_val = taxo_id_dict[op_txid].get(tph, {'v':0})['v']
                    clean_value = op_val
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                    except:clean_value = ''
                    if clean_value == '':
                        clean_value = '0'
                    clean_value = float(clean_value)
                    v_ar.append(clean_value)
                    phcsv   = taxo_id_dict[op_txid].get(tph, {}).get('phcsv', {}).get('s', '')
                    if phcsv:
                        scale_d[phcsv]  = 1
            tv_ar   = copy.deepcopy(v_ar)
            if res_val :
                v_ar.append(float(res_val))
                f_sig   = tavinash.get_sig(v_ar)
                if f_sig:
                    op_inds = map(lambda x:'+' if x==0 else '-', f_sig[0])
                elif sys_op_inds:
                    op_inds = sys_op_inds
            m_scale = ''
            if len(scale_d.keys()) > 1:
                num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
                scales  = map(lambda x:(num_obj[self.scale_map_d[x]], x), scale_d.keys())
                scales.sort(key=lambda x:x[0])
                m_scale = scales[-1][1]

            n_form  = [copy.deepcopy(res)]
            op_ind  = 0
            scale_d = {}
            conver_arr  = []
            for oper in opers:
                #print oper, op_inds, sys_op_inds
                op_txid    = oper['txid']
                op         = oper['op']
                #op_tt      = oper['t_type']
                op_type    = oper['type']
                tph         = ph
                if oper.get('k', ''):
                    tph  = oper['k']
                elif oper.get('period', ''):
                    #print '\tTT',[oper['period'],year,  oper['yd']]
                    tph  = ph_map_d.get(oper['period']+str(int(year)+int(oper['yd'])), '')
                #print [tph]
                desc        = ''
                if op_type == 'v':
                    op_val  = op_txid
                    desc    = op_txid
                else:
                    if op_inds:
                        op  = op_inds[op_ind]
                    op_ind  += 1
                    op_val = taxo_id_dict[op_txid].get(tph, {'v':0})['v']
                        
                    desc    = taxo_id_dict[op_txid]['t_l']
                    #print [desc, op_val]
                    if taxo_id_dict[op_txid].get(tph, {}).get('x', None) != None:
                        v_d = taxo_id_dict[op_txid].get(tph, {})
                        re_ar.append({'i':len(re_ar), 'x':v_d['x'], 'bbox':v_d['bbox'], 't':v_d['t'], 'd':v_d['d'], 'phcsv':copy.deepcopy(v_d.get('phcsv', {}))})
                        phcsv   = taxo_id_dict[op_txid].get(tph, {}).get('phcsv', {}).get('s', '')
                        clean_value = op_val
                        try:
                            clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                        except:clean_value = ''

                        if phcsv and m_scale:
                            if clean_value == '':
                                clean_value = '0'
                            if phcsv != m_scale:
                                tv, nscale   = self.convert_frm_to(clean_value, self.scale_map_d[phcsv]+' - '+self.scale_map_d[m_scale], phcsv)
                                conver_arr.append((clean_value, tv, self.scale_map_d[phcsv]+' - '+self.scale_map_d[m_scale]))
                                op_val  = str(tv)
                            else:
                                scale_d[m_scale]    = re_ar[-1]
                            
                oper['op']  = op
                n_form.append(copy.deepcopy(oper))
                #print [op_val]
                clean_value = op_val
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                except:clean_value = ''

                if clean_value == '':
                    clean_value = '0'
                expr_str += [str(op), desc] 
                val_li += [str(op), str(clean_value)]
            cell_val = 0
            #print [''.join(val_li)]
            try:
                cell_val = eval(''.join(val_li))
            except:pass
            re_ar.sort(key=lambda v_d: (v_d['i'], 0 if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d'] else 1 ))
            if not re_ar or cell_val == 0:
                return '', {}, [], [], n_form, []
            if re_ar and (m_scale in scale_d):
                re_ar   = [scale_d[m_scale]]
            cell_val    = str(cell_val)
            #print '\n==================='
            #print [cell_val, ''.join(val_li)]
            cell_val    = self.convert_floating_point(cell_val)
            if cell_val == '' or cell_val == '0':
                return '', {}, [], [], n_form, []
            cell_val    = cell_val.replace(',', '')
            dd          = copy.deepcopy(re_ar[0])
            dd['v']     = str(cell_val)
            dd['v_ar']  = tv_ar
            #print 'END', [str(cell_val), dd, val_li, expr_str]
            n_form[0]['conver_arr'] = conver_arr
            return str(cell_val), dd, val_li, expr_str, n_form, conver_arr
           
        val_dict = {}
        form_d  = {}
        for ph in phs:
            if taxo_id_dict[res_taxo_id].get(ph, {}).get('v', '') == '' or run_all=='Y':
                clean_value = taxo_id_dict[res_taxo_id].get(ph, {}).get('v', '')
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                except:clean_value = ''
                cell_val, ref, expr_val, expr_str, n_form, conver_arr = get_eval(res_taxo_id, ph, clean_value, sys_op_d.get((str(res_taxo_id), ph), []))
                form_d[ph]  = n_form
                if cell_val not in ['0', '']:
                    ref['v'] = cell_val
                    ref['fv'] = 'Y'
                    ref['expr_str'] = ''.join(expr_str)+' '+str(conver_arr)
                    ref['expr_val'] = ''.join(expr_val)
                    val_dict.setdefault(res_taxo_id, {})[ph] = ref
        return val_dict, form_d

    def compute_formula(self, f_lst, consider_ph, taxo_d):
        final_d = {}
        for resultant, formula, s_ph in f_lst:
            l   = len(formula) - 1
            new_d   = {}
            for ph in consider_ph.keys():
                expr_lst        = []
                expr_label_lst  = []
                expr_label_lst  = []
                doc_d           = {}
                i   = 0
                fn_all  = 1
                for tt in formula:
                    #'t_l':val_d[taxo_id]['t_l'] ,'t_id':taxo_id, 'ph':ph, 'v':value, 'symbol':sign, 'g_t':r_group_taxo
                    t_id    = tt['t_id']
                    t_l     = taxo_d[t_id]['t_l']
                    sign    = tt['symbol']
                    expr_label_lst.append(t_l)
                    if tt.get('break', '') in ['(']:
                        expr_label_lst[-1]  = ' ( '+expr_label_lst[-1]
                    if tt.get('break', '') in [')']:
                        expr_label_lst[-1]  = expr_label_lst[-1]+' ) '

                    dd  = taxo_d.get(t_id, {}).get(ph, {})
                    if dd.get('v', '') == '' and s_ph != 'Y':
                        fn_all  = 0
                        break
                    v   = self.normalized_txt(dd.get('v', '0'))
                    if not v:
                        v   = 0
                    d   = dd.get('d', '')
                    x   = dd.get('x', '')
                    if d and x:
                        x_sp    = x.split('@')
                        for ti, d1 in enumerate(d.split('@')):
                            doc_d.setdefault(d1, []).append(x_sp[ti])
                    expr_lst.append(str(v))
                    if tt.get('break', '') in ['(']:
                        expr_lst[-1]  = ' ( '+expr_lst
                    if tt.get('break', '') in [')']:
                        expr_lst[-1]  = expr_lst+' )'

                    #print i, l, tt
                    if sign == '%':
                        if i == l:
                            expr_lst        = ['(']+expr_lst+[')']
                            expr_label_lst  = ['(']+expr_label_lst+[')']
                            expr_lst.append('*100')
                            expr_label_lst.append('*100')
                        else:
                            expr_lst.append('*100')
                            expr_label_lst.append('*100')
                    elif sign == '/100':
                        if i == l:
                            expr_lst        = ['(']+expr_lst+[')']
                            expr_label_lst  = ['(']+expr_label_lst+[')']
                            expr_lst.append('/100')
                            expr_label_lst.append('/100')
                        else:
                            expr_lst.append('/100')
                            expr_label_lst.append('/100')
                    elif sign and i != l:
                        expr_lst.append(sign)
                        expr_label_lst.append(sign)
                    i   += 1
                if fn_all  ==0 and s_ph != 'Y':continue
                expr    = ' '.join(expr_lst)
                expr_str= ' '.join(expr_label_lst) +' = '+taxo_d[resultant]['t_l']
                try:
                    r   = self.convert_floating_point(eval(expr))
                except:
                    r   = 0
                d_arr   = []
                x_arr   = []
                for k, v in doc_d.items():
                    d_arr.append(k)
                    x_arr.append('#'.join(v))
                new_d[ph]   = {'v':r, 'd':'@'.join(d_arr), 'x':'@'.join(x_arr), 'expr':expr+' = '+str(r), 'expr_str':expr_str}
            final_d[resultant] = new_d
        return final_d

    def convert_floating_point(self, string, r_num=2):
         if string == '':
             return ''
         try:
             string = float(string)
         except:
             return string
         f_num  = '{0:.10f}'.format(string).split('.')
         #print [string, f_num]
         r_num  = 0
         if f_num[0] in ['0', '-0']:
             if len(f_num) ==2:
                  r_num     = 0 #len(f_num[1])
                  f_value   = 0
                  for num in f_num[1]:
                      r_num += 1
                      if f_value:break
                      if num != '0':
                        f_value = 1
                  if f_value == 0:
                        r_num = 0
         else:
             if len(f_num) ==2:
                  r_num     = 0 #len(f_num[1])
                  #print '\t',[r_num]
                  f_val     = []
                  for i, num in enumerate(f_num[1]):
                      #print '\t\tNUM',[num, r_num]
                      if num != '0':
                        f_val.append(i)
                  f_val.sort()
                  if f_val:
                    if f_val[0] == 0:
                        r_num   = 2
                    else:
                        r_num   = f_val[0]+1

             else:
                r_num   = 2
             if r_num < 0:
                 r_num = 0
             if r_num > 2:
                 r_num = 2
         form_str   = "{0:,."+str(r_num)+"f}"
         return form_str.format(string)


    def eval_ph_col_formula(self, row, col_phk, ops_ar, ph_map_d):
        for ops in ops_ar:
            expr_str    = []
            expr_val    = []
            f           = 1
            re_ar       = []
            #print '\n===================================================='
            for rr in ops:
                typ = rr['t']
                v   = rr['v']
                if typ == 'opr':
                    expr_val.append(v)
                    expr_str.append(v)
                elif typ == 'cons':
                    expr_val.append(v)
                    expr_str.append(v)
                elif typ == 'op':
                    v   = v+year
                    #print [v, ph_map_d.get(v), clean_v_d.get(ph_map_d.get(v))]
                    if v not in ph_map_d:
                        f           = 0
                        break
                    if ph_map_d[v] not in clean_v_d:
                        f           = 0
                        break
                    v_d     = row[ph_map_d[v]]
                    if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d']:
                        re_ar.append({'x':v_d['x'], 'bbox':v_d['bbox'], 't':v_d['t'], 'd':v_d['d']})
                    expr_val.append(clean_v_d[ph_map_d[v]])
                    expr_str.append(rr['v'])
            #print f, ''.join(expr_val)
            if f == 0:
                continue
            v   = ''
            try:
                v   = str(eval(''.join(expr_val)))
            except:
                continue
            #print [v]
            if re_ar:
                ref_d   = re_ar[0]
            else:
                ref_d   = {}
            ref_d['expr_str']   = ''.join(expr_str)
            ref_d['expr_val']   = ''.join(expr_val)
            ref_d['fv']   = 'Y'
            ref_d['v']          = v
            return v, ref_d, ph_gid_map.get(formula, '')
        return '', {}, ''



        
    

    def eval_ph_formula(self, row, ops_ar, ph_map_d,clean_v_d, year, ph_gid_map, ptype):
        for formula, ops in ops_ar:
            expr_str    = []
            expr_val    = []
            f           = 0
            re_ar       = []
            #print '\n===================================================='
            scale_d     = {}
            fy          = 0
            fy_val      = 0
            pt_d        = {}
            nt_f        = 0
            for rr in ops:
                typ = rr['t']
                v   = rr['v']
                if typ == 'op':
                    pt_d[v] = 0
                    v   = v+year
                    if v not in ph_map_d:
                        expr_val.append('0')
                        continue
                    if ph_map_d[v] not in clean_v_d:
                        expr_val.append('0')
                        continue
                    v_d     = row[ph_map_d[v]]
                    phcsv   = v_d['phcsv']['s']
                    pt_d[rr['v']] = 1
                    #if clean_v_d[ph_map_d[v]]:
                    #    pt_d[rr['v']] = 1
                    if phcsv:
                        scale_d[phcsv]  = 1
            #if 0 in pt_d.values():
            #    return '', {}, ''
            #if 'FY' in pt_d and pt_d['FY'] != 1:
            #    return '', {}, ''
            #if ptype == 'Q4' and 'Q1' in pt_d and pt_d['Q1'] != 1:
            #    return '', {}, ''
                
            m_scale = ''
            if len(scale_d.keys()) > 1:
                num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
                scales  = map(lambda x:(num_obj[self.scale_map_d[x]], x), scale_d.keys())
                scales.sort(key=lambda x:x[0])
                m_scale = scales[-1][1]
                 
            expr_str    = []
            expr_val    = []
            f           = 0
            re_ar       = []
                
            conver_arr  = []
            scale_d  = {}
            for rr in ops:
                typ = rr['t']
                v   = rr['v']
                expr_str.append(v)
                if typ == 'opr':
                    expr_val.append(v)
                elif typ == 'cons':
                    expr_val.append(v)
                elif typ == 'op':
                    v   = v+year
                    #print [v, ph_map_d.get(v), clean_v_d.get(ph_map_d.get(v))]
                    if v not in ph_map_d:
                        expr_val.append('0')
                        #f           = 0
                        #break
                        continue
                    if ph_map_d[v] not in clean_v_d:
                        expr_val.append('0')
                        #f           = 0
                        #break
                        continue
                    #print rr, ph_map_d[v]
                    v_d     = row[ph_map_d[v]]
                    #if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d']:
                    re_ar.append({'i':len(re_ar), 'x':v_d['x'], 'bbox':v_d['bbox'], 't':v_d['t'], 'd':v_d['d']})
                    if clean_v_d[ph_map_d[v]] == '':
                        expr_val.append('0')
                    else:
                        f = 1
                        #print v_d
                        if m_scale:
                            phcsv   = v_d['phcsv']['s']
                            if phcsv != m_scale:
                                tv, nscale   = self.convert_frm_to(clean_v_d[ph_map_d[v]], self.scale_map_d[phcsv]+' - '+self.scale_map_d[m_scale], phcsv)
                                conver_arr.append((clean_v_d[ph_map_d[v]], tv, self.scale_map_d[phcsv]+' - '+self.scale_map_d[m_scale]))
                                expr_val.append(str(tv))
                            else:
                                scale_d[m_scale]    = re_ar[-1]
                                expr_val.append(clean_v_d[ph_map_d[v]])
                        else:
                            expr_val.append(clean_v_d[ph_map_d[v]])
            #print f, ''.join(expr_val)
            if f == 0:
                continue
            v   = ''
            try:
                v   = str(eval(''.join(expr_val)))
            except:
                continue
            if v == '' or v == 0.0:continue
            v    = self.convert_floating_point(v)
            #print [v]
            re_ar.sort(key=lambda v_d: (v_d['i'], 0 if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d'] else 1 ))
            if re_ar and (m_scale in scale_d):
                ref_d   = scale_d[m_scale]
            elif re_ar:
                ref_d   = re_ar[0]
            else:
                ref_d   = {}
            ref_d['expr_str']   = ''.join(expr_str)+' -- '+str(conver_arr)
            ref_d['expr_val']   = ''.join(expr_val)
            ref_d['fv']   = 'Y'
            ref_d['PH_D']   = 'Y'
            ref_d['v']          = v
            return v, ref_d, ph_gid_map.get(formula, '')
        return '', {}, ''

    def convert_frm_to(self, value, frm_to, scale):
            
        num     = float(value)
        frm, to = frm_to.split(' - ')
        mscale  = ''
        if scale:
            mscale  = self.scale_map_d[scale]
        if frm  != mscale:
            return '', ''
        num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
        frm_val = num_obj[frm]
        to_val  = num_obj[to]
        div_val = float(frm_val)/float(to_val)
        final_val = float(num * div_val)
        return final_val, to


    def calculate_user_cf_ph_values(self, ti, ph, t_cfs_f, rr, key_value, cfgrps, n_phs, cf_all_grps, res, ph_map_d, taxo_id_dict):
        f   = 0
        if ph['k'] in t_cfs_f:
            t_row_f     = t_cfs_f[ph['k']]
            year    = ph['ph'][-4:]
            #print '\n===================================='
            #print rr['t_l']
            #print t_row_f[2]
            val_dict, form_d = self.get_formula_evaluation(t_row_f[2], taxo_id_dict, [ph['k']], ph_map_d)
            #print  ph['ph'], val_dict
            if val_dict.get(str(rr['t_id']), {}):
                ref_d   = copy.deepcopy(val_dict[str(rr['t_id'])][ph['k']])
                rr[ph['k']] = ref_d
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(ref_d['v'])
                except:
                    clean_value = ''
                    pass
                key_value[ph['k']]       = clean_value
                f_ar    = []
                for ft in t_row_f[2]:
                    if ft['type'] == 'v':
                        dd  = {}
                        dd['clean_value']   = ft['txid']
                    else:
                        tph         = ''
                        if ft.get('k', ''):
                            tph  = ft['k']
                        elif ft.get('period', ''):
                            #print '\tTT',[oper['period'],year,  oper['yd']]
                            tph  = ft['period']+str(int(year)+int(ft['yd']))
                        if ft['op'] == '=':
                            dd  = copy.deepcopy(ref_d)
                        else:
                            dd  = copy.deepcopy(taxo_id_dict[ft['txid']].get(ph_map_d.get(tph, ''), {}))
                        dd['clean_value']   = dd.get('v', '')
                        dd['description']   = taxo_id_dict[ft['txid']]['t_l']
                        dd['ph']            = tph
                        dd['taxo_id']       = ft['txid']
                    dd['operator']      = ft['op']
                    dd['k']      = ph['k']
                    f_ar.append(dd)
                f_ar[0]['R']    = 'Y'
                f_ar[0]['rid']    = t_row_f[0].split('-')[-1]
                ref_d['f_col_CF']  = [f_ar]
                f   = 1
                n_phs[ph['ph']] = 1
                rr[ph['k']]['expr_str']   = 'CF  '+ref_d['expr_str']
                rr[ph['k']]['fv']   = 'Y'
                rr[ph['k']]['PH_D']   = 'Y'
                res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        if f == 0 and ph['ph'][:-4] in t_cfs_f:
            t_row_f     = t_cfs_f[ph['ph'][:-4]]
            year    = ph['ph'][-4:]
            #print '\n===================================='
            #print rr['t_l'], t_row_f[2], ph_map_d
            val_dict, form_d = self.get_formula_evaluation(t_row_f[2], taxo_id_dict, [ph['k']], ph_map_d, ph['ph'][-4:] )
            #print  ph['ph'], val_dict
            if val_dict.get(str(rr['t_id']), {}):
                ref_d   = copy.deepcopy(val_dict[str(rr['t_id'])][ph['k']])
                #print '\n===================================='
                #print rr['t_l']
                #print  ph['ph'], ref_d
                rr[ph['k']] = ref_d
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(ref_d['v'])
                except:
                    clean_value = ''
                    pass
                key_value[ph['k']]       = clean_value
                f_ar    = []
                for ft in t_row_f[2]:
                    if ft['type'] == 'v':
                        dd  = {}
                        dd['clean_value']   = ft['txid']
                    else:
                        tph         = ''
                        if ft.get('k', ''):
                            tph  = oper['k']
                        elif ft.get('period', ''):
                            #print '\tTT',[oper['period'],year,  oper['yd']]
                            tph  = ft['period']+str(int(year)+int(ft['yd']))
                        if ft['op'] == '=':
                            dd  = copy.deepcopy(ref_d)
                        else:
                            dd  = copy.deepcopy(taxo_id_dict[ft['txid']].get(ph_map_d.get(tph, ''), {}))
                        dd['clean_value']   = dd.get('v', '')
                        dd['description']   = taxo_id_dict[ft['txid']]['t_l']
                        dd['ph']            = tph
                        dd['taxo_id']       = ft['txid']
                    dd['operator']      = ft['op']
                    dd['k']      = ph['k']
                    f_ar.append(dd)
                f_ar[0]['R']    = 'Y'
                f_ar[0]['rid']    = t_row_f[0].split('-')[-1]
                ref_d['f_col_CF']  = [f_ar]
                f   = 1
                n_phs[ph['ph']] = 1
                rr[ph['k']]['expr_str']   = 'CF  '+ref_d['expr_str']
                rr[ph['k']]['fv']   = 'Y'
                res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        return f
        

    def calculate_user_ph_values(self, ph, rr, ti, t_cf_f, t_ph_f, t_cfs_f, taxo_value_grp, ph_ti, res, data, f_phs_d, key_value, ph_map_d, cfgrps, cf_all_grps, grps, all_grps, n_phs, ph_f_map, ph_gid_map, d_all_grps, taxo_id_dict, do_null):
        f   = 0
        if ph['k'] in t_cf_f:
            g_id, cf_taxo_id, cf_phk, cf_ph, year_diff = t_cf_f[ph['k']]
            if cf_taxo_id in taxo_value_grp and cf_phk in data[taxo_value_grp[cf_taxo_id]]:
                ref_d   = copy.deepcopy(data[taxo_value_grp[cf_taxo_id]][cf_phk])
                #print '\n===================================='
                #print rr['t_l']
                #print  ph['ph'], t_ph_f[ph['ph'][:-4]]
                rr[ph['k']] = ref_d
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(ref_d['v'])
                except:
                    clean_value = ''
                    pass
                key_value[ph['k']]       = clean_value
                f   = 1
                cfgrps.setdefault(g_id, {})[ph['k']]  = 'Y'
                n_phs[ph['ph']] = 1
                cf_all_grps[g_id]   = 1
                rr[ph['k']]['expr_str']   = 'CF ( '+str([cf_taxo_id, cf_phk, cf_ph])+' )'
                rr[ph['k']]['fv']   = 'Y'
                rr[ph['k']]['PH_D']   = 'Y'
                res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        if f == 0 and ph['ph'][:-4] in t_cf_f:
            g_id, cf_taxo_id, cf_phk, cf_ph, year_diff = t_cf_f[ph['ph'][:-4]]
            if cf_taxo_id in taxo_value_grp:
                cf_ph   = cf_ph+str(int(ph['ph'][-4:])+year_diff)
                cf_phk  = ph_ti.get(cf_ph, {}).get(taxo_value_grp[cf_taxo_id], '')
                #print '\n===================================='
                #print rr['t_l']
                #print  ph['ph'], cf_phk
                if cf_phk:
                    ref_d   = copy.deepcopy(data[taxo_value_grp[cf_taxo_id]][cf_phk])
                    #print '\tGot...',ref_d['v']
                    rr[ph['k']] = ref_d
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(ref_d['v'])
                    except:
                        clean_value = ''
                        pass
                    key_value[ph['k']]       = clean_value
                    f   = 1
                    cfgrps.setdefault(g_id, {})[ph['k']]  = 'Y'
                    n_phs[ph['ph']] = 1
                    cf_all_grps[g_id]   = 1
                    rr[ph['k']]['expr_str']   = 'CF ( '+str([cf_taxo_id, cf_phk, cf_ph])+' )'
                    rr[ph['k']]['fv']   = 'Y'
                    rr[ph['k']]['PH_D']   = 'Y'
                    res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        if f == 0:
            f   = self.calculate_user_cf_ph_values(ti, ph, t_cfs_f, rr, key_value, cfgrps, n_phs, cf_all_grps, res, f_phs_d, taxo_id_dict)
            
        if f == 0 and ph['k'] in t_ph_f:
            #print '\n===================================='
            #print rr['t_l']
            #print  ph['ph'], t_ph_f[ph['k']]
            v, ref_d, g_id    = self.eval_ph_formula(rr, t_ph_f[ph['k']], f_phs_d, key_value, ph['ph'][-4:], ph_gid_map, ph['ph'][:-4])
            if v:
                #print  '\t', ref_d
                rr[ph['k']] = ref_d
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(ref_d['v'])
                except:
                    clean_value = ''
                    pass
                key_value[ph['k']]       = clean_value
                f   = 1
                grps.setdefault(g_id, {})[ph['k']]  = 'Y'
                n_phs[ph['ph']] = 1
                d_all_grps.setdefault(g_id, {})[t_ph_f[('rid', ph['k'])]]  = 1
                res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
        if f == 0 and do_null:
            year    = ph['ph'][-4:]
            for ph_f in ph_f_map.get(ph['ph'][:-4], []):
                f_all   = 1
                #print '\n========================================'
                #print ph_f
                f_fy    = 0
                f_one   = 0
                for op in ph_f['op']:
                    if  op == 'FY' and f_fy == 0:
                        f_fy    = 1
                    op_ph   = op+year
                    #print '\t', [f_phs_d.get(op_ph, 'NOPH'), f_phs_d.get(op_ph, 'NOPH') not in rr]
                    if key_value.get(f_phs_d.get(op_ph, 'NOPH'), None) == None:
                        f_all   = 0
                        break
                    f_one   = 1
                    if  op == 'FY':
                        f_fy    = 2
                #print 'f ', f
                #f_fy    = 0
                if f_all == 1 or (f_one == 1 and f_fy != 1):
                    n_phs[ph['ph']] = 1
                    grps.setdefault(ph_f['g_id'], {})[ph['k']]  = 'N'
                    all_grps.setdefault(ph_f['g_id'], {})[rr['t_id']]  = 1
        return f


    def create_group_ar(self, ijson, txn_m):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        ijson['vids']   = {}
        table_type    = ijson['table_type']
        vgh_id_d, vgh_id_d_all, docinfo_d   = {}, {}, {}
        if 1:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            #print sql
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type    = rr
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                vgh_id_d.setdefault(vgh_text, {})[(table_id, c_id, row_id, doc_id)]        = 1
                vgh_id_d_all[vgh_text]  = 1
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                docinfo_d.setdefault(doc_id, {})[(table_id, c_id, vgh_text)]   = 1
            conn.close()
        g_ar  = self.read_all_vgh_groups(table_type, company_name, model_number,vgh_id_d, vgh_id_d_all, docinfo_d)
        return g_ar

    def create_maturity_date(self, ijson, txn_m):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        ijson['vids']   = {}
        table_type    = ijson['table_type']
        vgh_id_d, vgh_id_d_all, docinfo_d   = {}, {}, {}
        if 1:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            #print sql
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type    = rr
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                vgh_id_d.setdefault(vgh_text, {})[(table_id, c_id, row_id, doc_id)]        = 1
                vgh_id_d_all[vgh_text]  = 1
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                docinfo_d.setdefault(doc_id, {})[(table_id, c_id, vgh_text)]   = 1
            conn.close()
        g_ar  = self.read_all_vgh_groups(table_type, company_name, model_number,vgh_id_d, vgh_id_d_all, docinfo_d)
        mdate           = filter(lambda x:x['n'].lower().split(' - ')[0] == 'maturity date', g_ar)
        if mdate:
            m_d = {}
            for mr in mdate:
                if '-' in mr['grpid']:
                    m_d[mr['grpid'].split('-')[1]]  = mr
                else:
                    m_d['']  = mr
            return m_d
        return {}

            
        

    def create_ph_group(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)


        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        years       = {}
        #c_year      = int(datetime.datetime.now().strftime('%Y'))
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[-4:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs.setdefault(ph, {})[doc_id]        = 1
                years[ph[-4:]]       = {}
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        dphs_ar         = report_year_sort.year_sort(dphs.keys())
        dphs_ar.reverse()
        ignore_doc_ids  = {}
        key_map = {
                        1   : 'actual_value',
                        2   : 'clean_value',
                        3   : 'd',
                        4   : 'pno',
                        5   : 'bbox',
                        6   : 'gv_ph',
                        7   : 'scale',
                        8   : 'value_type',
                        9   : 'currency',
                        10  : 'ph',
                        11  : 'label change',
                        12  : 're_stated',
                        13  : 'period',
                        14  : 'description',
                        15  : 'f_col',
                        16  : 'f_row',
                        17  : 'operator',
                        18  : 'taxo',
                        19  : 'row_id',
                        20  : 'col_id',
                        21  : 'taxo_id',
                        22  : 'maturity_date',
                        23  : 'h_or_t',
                        24  : 'table_id',
                        25  : 'xml_id',
                    }
        key_map_rev     = {}
        key_map_rev_t   = {}
        for k, v in key_map.items():
            key_map_rev[v]  = k
            key_map_rev_t[v]  = v
        ph_f_map, ph_grp_ar, ph_gid_map         = self.read_ph_formula(ijson, 'Y')
        group_id    = ijson.get('grpid', '')
        #if '-' in group_id:
        #    group_id, doc_grpid = group_id.split('-')
        ph_formula_d                            = self.read_ph_user_formula(ijson, '')
        if group_id:
            ph_formula_d                            = self.read_ph_user_formula(ijson, group_id, ph_formula_d)
        ph_filter       = 'P'
        if ph_filter != 'P':
            rem = int(ph_filter.split('-')[1])
            new_phs = dphs_ar[rem:]
            for ph in dphs_ar[:rem]:
                ignore_doc_ids.update(dphs[ph])
                del dphs[ph]

        t_ids       = ijson.get('t_ids', [])
        gen_type    = ijson.get('type','')
        grp_mad_d   = {}
        table_type  = ijson['table_type']
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        #print 'Running ', table_type
        ijson['gen_output']     = 'Y'
        ijson['ignore_doc_ids'] = ignore_doc_ids
        res                 = self.create_seq_across(ijson)
        phs                 = res[0]['phs']
        data                = res[0]['data']
        mdata               = self.create_maturity_date(ijson, txn_m)
        mat_date            = {}
        if mdata:
            if '-' in group_id:
                group_id_sp = group_id.split('-')
                if group_id_sp[1] in mdata:
                    mat_date            = mdata[group_id_sp[1]]
            elif '' in mdata:
                mat_date            = mdata['']
        #print mat_date
        if mat_date:
            mat_date    = self.read_mat_date(ijson, mat_date, txn_m)
        validate_op         = {}
        rc_d                = {}
        consider_ks         = {}
        consider_table      = {}
        consider_rows       = {}
        xml_row_map         = {}
        ph_kindex           = {}
        for ii, ph in enumerate(phs):
            ph_kindex[ph['k']]  = ii
        duplicate_taxonomy  = {}
        phk_data            = {}
        validation_error    = {}
        #print validate_op
        ph_ti               = {}
        taxo_value_grp  = {}
        phk_ind_data        = {}
        clean_v_d   = {}
        row_ind_map = {}
        row                 = 1
        csv_d               = {}
        taxo_id_dict        = {}
        for ti, rr in enumerate(data[:]):
            taxo_value_grp[str(rr['t_id'])]  = ti
            #print '\n====================================='
            #print rr['t_l']
            for ii, ph in enumerate(phs):
                if ph['k'] not in rr:continue
                v_d         = rr[ph['k']]
                t           = v_d['v']
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                if 'f_col' in v_d:
                    del v_d['f_col']
                if 'f_col' in res[0]['data'][ti][ph['k']]:
                    del res[0]['data'][ti][ph['k']]['f_col']
                tk   = self.get_quid(v_d['t']+'_'+v_d['x'])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if c_id:
                    ktup    = (rr['t_id'], v_d['t'], int(c_id.split('_')[1]))
                    #print '\t', ktup, ktup in mat_date
                    if ktup in mat_date:
                        v_d['mdate']    = mat_date[ktup][2]
                        res[0]['data'][ti][ph['k']]['mdate']    = mat_date[ktup][2]
                        v_d.setdefault('phcsv', {})['mdate']    =  mat_date[ktup][2]
                        res[0]['data'][ti][ph['k']].setdefault('phcsv', {})['mdate']    = mat_date[ktup][2]
                clean_v_d.setdefault(ti, {})[ph['k']]   = clean_value
                ph_ti.setdefault(ph['n'], {})[ti]  = ph['k']
                if clean_value == '':continue
                phk_ind_data.setdefault(ph['k'], {})[ti]    = clean_value
                phk_data.setdefault(ph['k'], []).append((ti, clean_value))
                #ph_value_d.setdefault(ph['n'], {}).setdefault(clean_value, {})[ph['k']]          = 1
        #print phs
        f_phs, reported_phs               = self.get_order_phs(phs, dphs, self.report_map_d)
        dphs_all         = report_year_sort.year_sort(dphs.keys())
        col_m               = self.get_column_map(ph_kindex, reported_phs, f_phs, phk_ind_data, data, ph_formula_d, taxo_value_grp)
        #sys.exit()
        f_phs_d             = {}
        done_ph             = {}
        ph_map_d    = {}
        for ph in f_phs:
            f_phs_d[ph['ph']]   = ph['k']
            done_ph[ph['ph']]  = 1
            ph_map_d[ph['ph']]    = ph['k']
        new_phs         = []
        n_d             = {}
        #for ph in dphs_ar:
        #    if ph in done_ph or ph in n_d:continue
        tt_ph_f             = self.read_ph_config_formula(ijson)
        if tt_ph_f:
            remain_phs = tt_ph_f.keys()
        else:
            remain_phs = ph_f_map.keys()
        for ph in remain_phs:
            for year in years.keys():
                tph  = ph+year
                if tph in done_ph or tph in n_d:continue
                n_d[tph]             =1
                new_phs.append({'k':tph, 'ph':tph, 'new':'Y'})
        if new_phs:
            ph_map_d    = {}
            #for k, v in f_phs_d.items():
            #    print k, v
            for ph in f_phs+new_phs:
                #print [ph]
                ph_map_d[ph['ph']]   = ph
                f_phs_d[ph['ph']]   = ph['k']
                done_ph[ph['ph']]  = 1
            #print f_phs_d.keys()
            #print n_phs.keys()
            dphs_ar             = report_year_sort.year_sort(f_phs_d.keys())
            dphs_ar.reverse()
            f_phs               = map(lambda x:ph_map_d[x], dphs_ar)

        rem_phs  = {}
        for ph in res[0]['phs']:
            if ph['k'] not in phk_ind_data:continue
            if ph['n'] not in f_phs_d:
                rem_phs[ph['n']]    = 1
        if rem_phs and 0:
            tnew_phs = []
            for tph in rem_phs.keys():
                if tph in done_ph or tph in n_d:continue
                n_d[tph]             =1
                new_phs.append({'k':tph, 'ph':tph, 'new':'Y'})
            if tnew_phs:
                ph_map_d    = {}
                #for k, v in f_phs_d.items():
                #    print k, v
                for ph in f_phs+tnew_phs:
                    ph_map_d[ph['ph']]   = ph
                    f_phs_d[ph['ph']]   = ph['k']
                    done_ph[ph['ph']]  = 1
                #print f_phs_d.keys()
                #print n_phs.keys()
                dphs_ar             = report_year_sort.year_sort(f_phs_d.keys())
                dphs_ar.reverse()
                f_phs               = map(lambda x:ph_map_d[x], dphs_ar)
            new_phs += tnew_phs
        
        for ti, rr in enumerate(data[:]):
            taxo_id_dict[str(rr['t_id'])]        = rr
            rr['f_taxo']    = str(rr['t_id'])
            t_s_f       = ph_formula_d.get(('S', str(rr['t_id'])), {})
            ph_value_d  = {}
            for ii, ph in enumerate(phs):
                csv_d.setdefault(ti, {})[ph['k']]   = (0, '','', '', '')
                if (ph['k'] not in rr or clean_v_d.get(ti, {}).get(ph['k'], '') == '') and (ti in col_m.get(ph['k'], {})):
                    if col_m[ph['k']][ti] in rr:
                        clean_v_d.setdefault(ti, {})[ph['k']]   = clean_v_d[ti][col_m[ph['k']][ti]]
                        rr[ph['k']] = rr[col_m[ph['k']][ti]]
                        res[0]['data'][ti][ph['k']]    = rr[col_m[ph['k']][ti]]
                if ph['k'] not in rr:continue
                if 'ng_f' in rr[ph['k']]:
                    del rr[ph['k']]['ng_f']
                if 'g_f' in rr[ph['k']]:
                    del rr[ph['k']]['g_f']
                v_d         = rr[ph['k']]
                t           = v_d['v']
                clean_value = clean_v_d[ti][ph['k']]
                ph_value_d.setdefault(ph['n'], {}).setdefault(clean_value, {})[ph['k']]          = 1
                d_al, scale, tph_map, mscale, form   = (0, '', '', '', '')
                key     = v_d['t']+'_'+self.get_quid(v_d['x'])
                tph_map  = txn.get('PH_MAP_'+str(key))
                if tph_map:
                    period_type, period, currency, scale, value_type    = tph_map.split('^')
                if scale:
                    mscale  = self.scale_map_d[scale]
                sgrpid  = ''
                if ph['k'] in t_s_f and clean_value:
                    sgrpid, tform    = t_s_f[ph['k']]
                    v, nscale   = self.convert_frm_to(clean_value, tform, scale)
                    if v:
                        form    = tform
                        v       = str(v)
                        clean_value = v
                        mscale  = nscale
                        rr[ph['k']]['expr_str']   = form+'('+str(rr[ph['k']]['v'])+') = '+str(v)
                        rr[ph['k']]['v']    = v
                        rr[ph['k']]['fv']   = 'Y'
                        rr[ph['k']]['fv']   = 'Y'
                        #scale_d.setdefault(mscale, {})[ph['k']]  = 'Y'
                        #mscale_d.setdefault(mscale, {}).setdefault(scale, {})[ph['k']]  = 'Y'
                        d_al    = 1
                csv_d.setdefault(ti, {})[ph['k']]   = (d_al, scale, mscale, form, sgrpid)
            if t_ids and rr['t_id'] not in t_ids:continue
            row += 1
            row_ind_map[ti] = row
            if ijson.get('restated', '') != 'N':
                for ii, ph in enumerate(f_phs):
                    trs_value    = []
                    if ph['ph'] not in dphs:continue
                    if ph['k'] not in rr or clean_v_d[ti].get(ph['k'], '') == '':
                        other_values            = ph_value_d.get(ph['ph'], {});
                        for v in other_values.keys():
                            if v and v != clean_v_d[ti].get(ph['k'], ''):
                                for key in other_values[v].keys():
                                        if 1:#indx_d[key] > c_index:
                                            trs_value.append(key)
                    else:
                        other_values            = ph_value_d.get(ph['ph'], {})
                        clean_value             = clean_v_d[ti][ph['k']]
                        for v in other_values.keys():
                            if v and v != clean_v_d[ti][ph['k']]:
                                for key in other_values[v].keys():
                                    if 1:#indx_d[key] > c_index:
                                        if  rr[key]['t'] != v_d['t']:
                                            trs_value.append(key)
                    trs_value.sort(key=lambda x:(dphs_all.index(doc_d[rr[x]['d']][0]), rr[x]['d']))
                    if trs_value:
                        prev_v      =  copy.deepcopy(rr.get(ph['k'], {'v':'','x':'','bbox':[], 'd':'', 't':''}))
                        rr[ph['k']] = copy.deepcopy(rr[trs_value[-1]])
                        rr[ph['k']]['rest_ar']  = trs_value[:]
                        rr[ph['k']]['org_d']    = prev_v
                        rr[ph['k']]['x'] = rr[ph['k']]['x']
                        csv_d.setdefault(ti, {})[ph['k']]   = csv_d[ti][trs_value[-1]]
                        clean_v_d[ti][ph['k']]   = clean_v_d[ti][trs_value[-1]]
                        res[0]['data'][ti][ph['k']]    = copy.deepcopy(rr[ph['k']])
                        #print 'RESTATED_ ', [rr['t_l'], ph, ph['ph'] in dphs, trs_value]


        row                 = 1
        remain_phs = ph_f_map.keys()
        all_grps        = {}
        cf_all_grps     = {}
        d_all_grps        = {}
        n_phs           = {}    
        scale_grps      = {}
        d_s_grps        = {}
        xml_row_map     = {}
        row                 = 1
        row_formula         = {}
        tmp_f_phs   = []
        for tr in f_phs:
            tr  = copy.deepcopy(tr) 
            tr['n']     = tr['ph']
            tmp_f_phs.append(tr)  
        self.calculate_missing_values_by_fx(t_ids, res, data, ph_formula_d, taxo_id_dict, table_type, group_id, row_formula, tmp_f_phs, str(deal_id))
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            row += 1
            tmpphs              = [] 
            key_value           = {}
            ph_value_d          = {}
            indx_d              = {}
            xml_col_map         = {}
            ph_map              = {}
            scale_d             = {}
            d_scale_d             = {}
            t_ph_f      = ph_formula_d.get(str(rr['t_id']), {})
            t_s_f       = ph_formula_d.get(('S', str(rr['t_id'])), {})
            t_cf_f      = ph_formula_d.get(('CF', str(rr['t_id'])), {})
            t_cfs_f     = ph_formula_d.get(('CF_F', str(rr['t_id'])), ())
            tmp_sgrps    = {}
            r_key       = {}
            rs_value    = {}
            grps        = {}
            cfgrps        = {}
            d_grps      = {}
            mscale_d    = {}
            for ii, ph in enumerate(phs):
                if ph['k'] in rr:
                    v_d = rr[ph['k']]
                    xml_row_map.setdefault((v_d['t'], v_d['x']), {})[ti]    = 1
                    v_d             = rr[ph['k']]
                    
                if ph['k'] not in rr:continue
                ph_map[ph['k']]              = ph['ph']
                v_d = rr[ph['k']]
                t       = v_d['v']
                xml_col_map[(v_d['t'], v_d['x'])]         = ph['k']
                clean_value = t
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(t)
                except:
                    clean_value = ''
                    pass
                #if clean_value == '':continue
                tmpphs.append(ph)
                #key_value[ph['k']]       = clean_value
                #print [ph['n'], clean_value, ph['k']]
                ph_value_d.setdefault(ph['n'], {}).setdefault(clean_value, {})[ph['k']]          = 1
                indx_d[ph['k']]              = ii
            key_value   = clean_v_d[ti]
            #f_phs       = self.get_order_phs(tmpphs, dphs, report_map_d)
            #print '\n===================================='
            #print rr['t_l']
            #print t_ph_f
            done_s    = {}
            r_key, rs_value, operand_rows, row_form, row_op = self.read_formula(ti, rr, f_phs, txn, xml_row_map, xml_col_map)
            consider_rows[rr['t_id']]       = 1
            if tt_ph_f:
                t_ph_f  = {}
                for ii, ph in enumerate(f_phs):
                    if ph['k'] not in rr or not key_value.get(ph['k'], ''):
                        if ph['ph'][:-4] in tt_ph_f:
                            t_ph_f[ph['k']] = tt_ph_f[ph['ph'][:-4]]
                            t_ph_f[('rid', ph['k'])] = 'RID-0'
            for ii, ph in enumerate(f_phs):
                d_al, scale, mscale, form,sgrpid  = csv_d[ti].get(ph['k'], (0, '', '','', ''))
                if form:
                    tmp_sgrps[sgrpid]  = (form, scale);
                    scale_d.setdefault(mscale, {})[ph['k']]  = 'Y'
                    mscale_d.setdefault(mscale, {}).setdefault(scale, {})[ph['k']]  = 'Y'
                if ph['k'] in rr:
                    consider_ks[ph['k']]         = 1
                if ph['k'] not in rr or not key_value.get(ph['k'], ''):
                    f   = self.calculate_user_ph_values(ph, rr, ti, t_cf_f, t_ph_f, t_cfs_f, taxo_value_grp, ph_ti, res, data, f_phs_d, key_value, ph_map_d, cfgrps, cf_all_grps, grps, all_grps, {}, ph_f_map, ph_gid_map, d_all_grps, taxo_id_dict, 1)
                    if f == 0:
                        continue
                r_key[ph['k']]   = 1
                consider_ks[ph['k']]         = 1
                rs_value[ph['k']]  = []
                #other_values            = ph_value_d[ph['ph']]
                #c_index                 = indx_d[ph['k']]
                v_d                     = rr[ph['k']]
                key     = v_d['t']+'_'+self.get_quid(v_d['x'])
                t                       = v_d['v']
                consider_table[v_d['t']]       =  1
                consider_rows[rr['t_id']]       = 1
                clean_value             = key_value[ph['k']]
                disp_name   = ''
                tmpdd   = {}
                self.add_row_formula(ti, tmpdd, operand_rows, res, data, ph, key_map_rev, key_map_rev_t, row_op, txn, disp_name, row_ind_map, {}, '', {}, {})
                if rr[ph['k']].get('f_col', []):
                    row_formula[str(rr['t_id'])]    = rr[ph['k']]['f_col'][0]
                if d_al == 0 and ph['k'] in csv_d[ti]:
                    scale_d.setdefault(mscale, {})[ph['k']]  = 'N'
                    mscale_d.setdefault(mscale, {}).setdefault(scale, {})[ph['k']]  = 'N'
                trs_value   = v_d.get('rest_ar', [])
                if trs_value:
                    del v_d['rest_ar']
                    self.add_restated_values(ti, rr, v_d['org_d'], {}, res, trs_value, key_map_rev, key_map_rev_t, ph, txn, dphs_ar, doc_d, csv_d)
                    del v_d['org_d']
            rr['mscale_d']  = mscale_d
            rr['scale_d']  = scale_d
            scales  = scale_d.keys()
            scales.sort()
            scales  = tuple(scales)
            if scales:
                if scales in scale_grps:
                    g_idd    = scale_grps[scales][0]
                else:
                    scale_grps[scales]  = ('S-'+str(len(scale_grps.keys())), map(lambda x:x+' - '+str(mscale_d[x].keys()), scales))
                    g_idd    = scale_grps[scales][0]
                s_ids   = {}
                for scale, pks in scale_d.items():
                    s_ids.update(pks)
                rr['s_gids']  = {g_idd:s_ids}
                for grp in tmp_sgrps.keys():
                    d_s_grps.setdefault(grp, {'v':tmp_sgrps[grp]})
                    s_ids   = {}
                    s_ids.update(d_scale_d.get(grp, {}))
                    rr['s_gids'][grp]   = s_ids
                 
            #for ph in remain_phs:
            #    for year in years.keys():
            #        if ph+year in done_ph:continue
            #        f   = 0
            #        tmpph   = ph+year
            #        pd_d    = {'k':tmpph, 'ph':tmpph}
            #        f   = self.calculate_user_ph_values(pd_d, rr, ti, t_cf_f, t_ph_f, t_cfs_f, taxo_value_grp, ph_ti, res, data, f_phs_d, key_value, ph_map_d, cfgrps, cf_all_grps, grps, all_grps, n_phs, ph_f_map, ph_gid_map, d_all_grps, taxo_id_dict, 1)
            
            rr['gids']  = grps
            rr['cfgids']  = cfgrps
            rr['d_gids']  = d_grps
        tmp_ar  = []
        for rr in ph_grp_ar:
            if rr['g_id'] in d_all_grps:
                rr['c'] = len(d_all_grps.get(rr['g_id'], {}).keys())
                rr['done']  = 'Y'
                rr['rid']  = d_all_grps.get(rr['g_id'], {}).keys()[0].split('-')[1]
                tmp_ar.append(rr)
            elif rr['g_id'] in all_grps:
                rr['c'] = len(all_grps.get(rr['g_id'], {}).keys())
                rr['done']  = 'N'
                tmp_ar.append(rr)
        #if not tmp_ar:
        #    res = [{'message':'done', 'grps':[]}]
        #    return res
        s_grp   = []
        sks     = scale_grps.keys()
        sks.sort(key=lambda x:scale_grps[x])
        for k in sks:
            s_grp.append({'ph':'S:'+'-'.join(k), 'op':map(lambda x:str(x), scale_grps[k][1]), 'g_id':scale_grps[k][0]})
        for k, v in d_s_grps.items():
            s_grp.append({'ph':v['v'][0], 'op':[v['v'][1]], 'g_id':k, 'r_id':k, 'done':'Y', 'rid':k.split('-')[1]})
        res[0]['s_grps']  = s_grp

        cf_grp   = []
        cfks     = cf_all_grps.keys()
        for k in cfks:
            cf_t, cf_c, r_ph, cf_ph = ph_formula_d[k]['v']
            cid =  ph_formula_d[k]['i'].keys()
            taxo_id, celid  = cid[0]
            dd  = {'done':"Y",'gid':k, 'r':{'ph':r_ph, 't_id':taxo_id, 't_l':data[taxo_value_grp[taxo_id]]['t_l'], 'k':celid, 'rks':map(lambda x:x[1], cid)}, 'cf':{'ph':cf_ph, 't_id':cf_t, 't_l':data[taxo_value_grp[cf_t]]['t_l'], 'k':cf_c}, 'rid':k.split('-')[1], 'row':ph_formula_d[k]['r'], 'col':ph_formula_d[k]['c']}
            cf_grp.append(dd)
        if not cf_grp:
            cf_grp.append({'r':{},'cf':{},'row':False, 'col':False})
        res[0]['cf_grps']  = cf_grp
            
            
        ph_grp_ar   = tmp_ar
        ph_grp_ar.sort(key=lambda x:(1 if x['done'] == 'Y' else 0, x['c']), reverse=True)
        res[0]['grps']  = ph_grp_ar
        if new_phs:
            ph_map_d    = {}
            #for k, v in f_phs_d.items():
            #    print k, v
            for ph in filter(lambda x:x['k'] in consider_ks, res[0]['phs'])+new_phs:
                #print [ph]
                if ph.get('new', '') == 'Y':
                    ph_map_d[ph['ph']]   = ph
                    ph['n']              = ph['ph']
                else:
                    ph_map_d[ph['n']]   = ph
            #for k, v in n_phs.items():
            #    ph_map_d[k] = {'g':'NEW-'+k,'k':k, 'n':k}
            #print f_phs_d.keys()
            #print n_phs.keys()
            dphs_ar             = report_year_sort.year_sort(f_phs_d.keys()+n_phs.keys())
            dphs_ar.reverse()
            res[0]['phs']       = map(lambda x:ph_map_d[x], dphs_ar)
        else:
            res[0]['phs']       = filter(lambda x:x['k'] in consider_ks, res[0]['phs'])
        f_phs   = res[0]['phs']
        res[0]['data']      = filter(lambda x:x['t_id'] in consider_rows, res[0]['data'])
        res[0]['table_ar']  = filter(lambda x:x['t'] in consider_table, res[0]['table_ar'])
        res[0]['n_phs']     = n_phs
        return res

    def calculate_missing_values_by_fx(self, t_ids, res, data, ph_formula_d, taxo_id_dict, table_type, group_id, row_formula, f_phs, deal_id):
        for ti, rr in enumerate(data[:]):
            if t_ids and rr['t_id'] not in t_ids:continue
            ftype   = ''
            t_row_f     = ph_formula_d.get(('SYS F', str(rr['t_id'])), ())
            op_d    = {}
            if not t_row_f:
                t_row_f     = ph_formula_d.get(('F', str(rr['t_id'])), ())
                if t_row_f:
                    ftype   = 'USER'
            else:
                ftype   = 'SYSTEM'
                op_d    = ph_formula_d.get(("OP", t_row_f[0]), {})
                
            if 0:#not t_row_f and row_formula.get(str(rr['t_id']), []):# and str(deal_id) == '214':
                tmpar   = []
                for tr in row_formula[str(rr['t_id'])]:
                    tmpar.append({'txid':str(tr['taxo_id']), 'type':'t', 't_type':table_type, 'g_id':group_id, 'op':tr['operator']})
                t_row_f = ('NEW F', tmpar)
            #if deal_id == '216':
            #    print '\n========================================='
            #    print rr['t_l'], [t_row_f]
            if t_row_f:
                ph_key_map  = {}
                for tr in f_phs:
                    ph_key_map[tr['k']]  = tr['n']
                val_dict, form_d = self.get_formula_evaluation(t_row_f[1], taxo_id_dict, map(lambda x:x['k'], f_phs), {}, None, 'Y', op_d)
                if val_dict:
                    for k, v in val_dict.get(str(rr['t_id']), {}).items():
                        if rr.get(k, {}).get('v', None) != None:
                            v_d = rr.get(k, {})
                            try:
                                clean_value = numbercleanup_obj.get_value_cleanup(v_d['v'])
                            except:
                                clean_value = '0'
                                pass
                            if clean_value == '':
                                clean_value = '0'
                            clean_value = float(clean_value)
                            n_value     = clean_value - float(v['v'].replace(',', '')) 
                            if n_value != 0.0:
                                rr[k]['c_s']    = n_value
                                res[0]['data'][ti][k]['c_s']   = n_value
                                #if deal_id == '216':
                                #    print [ph_key_map[k], clean_value, ' - ',float(v['v'].replace(',', '')), ' = ',n_value]
                            continue
                        v   = copy.deepcopy(v)
                        rr[k]   = v
                        #print '\t', k, v
                        res[0]['data'][ti][k]   = copy.deepcopy(v)
                        f_ar    = []
                        for ft in form_d[k]:#t_row_f[1]:
                            if ft['type'] == 'v':
                                dd  = {}
                                dd['clean_value']   = ft['txid']
                            else:
                                dd  = copy.deepcopy(taxo_id_dict[ft['txid']].get(k, {}))
                                dd['clean_value']   = dd.get('v', '')
                                dd['description']   = taxo_id_dict[ft['txid']]['t_l']
                                dd['ph']            = ph_key_map[k]
                                dd['taxo_id']       = ft['txid']
                            dd['operator']      = ft['op']
                            dd['k']      = k
                            f_ar.append(dd)
                        f_ar[0]['R']    = 'Y'
                        f_ar[0]['rid']    = t_row_f[0].split('-')[-1]
                        v['f_col']  = [f_ar]
                        rr['f']  = 'Y'
                for ph in f_phs[:]:
                    if ph['k'] in rr:
                        k   = ph['k']
                        v   = rr[ph['k']]
                        #if v.get('f_col', []):continue
                        #print '\tIN', k, v
                        
                        f_ar    = []
                        for ft in form_d[ph['k']]:#t_row_f[1]:
                            if ft['type'] == 'v':
                                dd  = {}
                                dd['clean_value']   = ft['txid']
                            else:
                                dd  = copy.deepcopy(taxo_id_dict[ft['txid']].get(k, {}))
                                dd['clean_value']   = dd.get('v', '')
                                dd['description']   = taxo_id_dict[ft['txid']]['t_l']
                                dd['ph']            = ph_key_map[k]
                                dd['taxo_id']       = ft['txid']
                            dd['operator']      = ft['op']
                            dd['k']      = k
                            f_ar.append(dd)
                        f_ar[0]['R']    = 'Y'
                        f_ar[0]['rid']    = t_row_f[0].split('-')[-1]
                        v['f_col']  = [f_ar]
                        v['fv']  = 'Y'
                        rr['f']  = 'Y'

    ## 33 PH Derivation            
    def save_ph_derivation_data(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        user_name       = ijson['user']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        ph_grps         = ijson['grps']
        table_type      = ijson['table_type']
        group_id        = ijson['grpid']
        df_flg          = int(ijson['df'])
        formula_type    = ijson.get('type', '')
        del_rows        = ijson.get('rids', [])
        saved_rid       = ''

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        try:
            create_stmt = "CREATE TABLE IF NOT EXISTS ph_derivation (row_id INTEGER PRIMARY KEY AUTOINCREMENT, formula VARCHAR(256), table_type VARCHAR(100), group_id VARCHAR(50), ph VARCHAR(10), formula_type VARCHAR(20), formula_str VARCHAR(256), user_name VARCHAR(100));"
            cur.execute(create_stmt)
            conn.commit()
        except:pass

        def delete_formula_rows(del_rows):
            del_row = ["'"+str(e)+"'" for e in del_rows]
            del_stmt = "delete from ph_derivation where row_id in (%s)"%(', '.join(del_row))
            cur.execute(del_stmt)
            conn.commit()
            return 'done'

        if df_flg == 1:
            msg = delete_formula_rows(del_rows)
            conn.close()
            res = [{'message':'done', 'rid':saved_rid}]
            return res

        elif df_flg == 0: 
            del_rows    = []
            for info_dict in ph_grps:
                try:
                    del_rows.append(str(int(info_dict['rid'])))
                except:pass
                
            msg = delete_formula_rows(del_rows)

        data_rows = []
        for info_dict in ph_grps:
            ph              = info_dict['ph']
            formula         = info_dict.get('f', '')
            formula_info    = []
            taxo_dict       = info_dict['tids']
            for taxo_id, cell_dict in taxo_dict.iteritems():
                tmp = [taxo_id]
                for cid, f_cell in cell_dict.iteritems():
                    if not isinstance(f_cell, list):
                        f_cell  = [f_cell]
                    fcell_str = '^^'.join([cid]+f_cell)
                    tmp.append(fcell_str)
                formula_info.append('$'.join(tmp))
            data_rows.append((formula, table_type, group_id, ph, formula_type, '##'.join(formula_info), user_name))

        if df_flg != 2: 
            cur.executemany('insert into ph_derivation (formula, table_type, group_id, ph, formula_type, formula_str, user_name) values(?, ?, ?, ?, ?, ?, ?)', data_rows)
            conn.commit()
            cur.execute('select max(row_id) from ph_derivation;')
            max_row_tup = cur.fetchone()
            saved_rid = str(max_row_tup[0])
        elif df_flg == 2:
            sel_stmt = "select formula_str from ph_derivation where row_id = '%s'"%(del_rows[0])
            cur.execute(sel_stmt)
            drow = cur.fetchone()
            formula_str = ''
            if drow is not None:
                formula_str = str(drow[0]) 
            stored_fstr = ''
            if data_rows and data_rows[0]:
                stored_fstr = data_rows[0][5]
            new_fstr = '##'.join([formula_str, stored_fstr])
            upd_stmt = 'update ph_derivation set formula_str = "%s" where row_id = "%s"'%(new_fstr, del_rows[0])
            cur.execute(upd_stmt)
            conn.commit()
        conn.close()
        res = [{'message':'done', 'rid':saved_rid}]
        return res 

    def create_group_id(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select table_type, vgh_id, group_txt from vgh_group_map"
        cur.execute(sql)
        res             = cur.fetchall()
        dd              = {}
        for rr in res:
            table_type, vgh_id, group_text  = rr
            try:
                group_text  = int(group_text)
            except:
                dd.setdefault((table_type, group_text), {})[vgh_id]              = 1

        sql = "CREATE TABLE IF NOT EXISTS vgh_group_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id VARCHAR(20), table_type VARCHAR(100), group_txt VARCHAR(20), user_name VARCHAR(100), datetime TEXT);"
        cur.execute(sql)
        sql = "select group_id, table_type, group_txt from vgh_group_info"
        cur.execute(sql)
        res = cur.fetchall()
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[(table_type, group_txt)]   = group_id
        dtime   = str(datetime.datetime.now()).split('.')[0]
        i_ar    = []
        with conn:
            sql = "select seq from sqlite_sequence WHERE name = 'vgh_group_info'"
            cur.execute(sql)
            r       = cur.fetchone()
            if r:
                g_id    = int(r[0])+2
            else:
                g_id    = 1
            for grptup, vids in dd.items():
                if grptup in grp_info:continue
                i_ar.append((g_id, )+grptup+('SYSTEM', dtime))
                g_id    += 1
        cur.executemany('insert into vgh_group_info(group_id, table_type, group_txt, user_name, datetime) values(?,?,?,?,?)', i_ar)
        u_ar    = []
        for k, v in grp_info.items():
            u_ar.append((v, )+k)
        cur.executemany("update vgh_group_map set group_txt=? where table_type=? and group_txt=?", u_ar)
        sql         = 'CREATE TABLE IF NOT EXISTS final_output(row_id INTEGER PRIMARY KEY AUTOINCREMENT, gen_id TEXT,table_type TEXT, gen_type TEXT, taxo_ids TEXT, group_text, user_name TEXT, datetime TEXT, error_txt TEXT)'
        cur.execute(sql)
        cur.executemany("update final_output set group_text=? where table_type=? and group_text=?", u_ar)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res
        

    def update_order_frm_backup(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
        #db_file         = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        #conn, cur       = conn_obj.sqlite_connection(db_file)
        display_data    = {}
        path            = '/var/www/html/muthu/FINAL_OUTPUT_%s/'%(deal_id)
        env1        = lmdb.open(path)
        with env1.begin() as txn1:
            for k, v in txn1.cursor():
                if 'DISPLAYOUTPUT_' in k:
                    display_data[k.split('DISPLAYOUTPUT_')[1]]  = eval(v)
        #ijson['table_type'] = 'BS'
        #ijson['update_db'] = 'Y'
        #self.re_order_seq(ijson)
        #sys.exit()
        new_order   = []
        for table_type in rev_m_tables.keys():
            if table_type in ['IS', 'COS', 'RNTL', 'COGS']:continue
            #if table_type != 'BS':continue
            if table_type not in self.order_d:continue
            table_id    = self.order_d[table_type]
            txtpath     = '/root/Muthu/%s/'%(company_name)
            fname       = txtpath+'/'+str(table_id)+'-P.txt'
            if str(table_id)+'-P' in display_data:#os.path.exists(fname):
                ijson['table_type'] = table_type
                res = self.create_seq_across(ijson)
                c_data      = res[0]['data']
                c_od        = {}
                for rr in c_data:
                    c_od[rr['t_id']]    = rr
                old_data    = display_data[str(table_id)+'-P'][0]['data']
                o_od        = {}
                print '\n======================================================'
                print 'table_type ', table_type
                for rr in old_data:
                    tid = rr['t_id']
                    if tid not in c_od:
                        print 'Missing ', tid, [ rr['t_l']]
                        continue
                    c_order = c_od[tid]['order']
                    if rr['t_l'] != c_od[tid]['t_l']:
                        print 'Error', [table_type, rr['t_l'], c_od[tid]['t_l']]
                        sys.exit()
                    if c_order == rr['order']:continue
                    new_order.append((rr['order'], tid, table_type))
                    print [tid, c_order, rr['order'], rr['t_l'], c_od[tid]['t_l']]
                    #o_od[rr['t_id']]    = rr['order']
                continue
                data    = eval(open(fname, 'r').read())
                for k, v in data.items():
                    if k == 'key_map':continue
                    ks  = list(sets.Set(map(lambda x:x[0], v['data'].keys())))
                    ks.sort()
                    for r in ks:
                        print r, v['data'][(r, 0)][1]
                    
            
        
        #db_file         = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        #conn, cur       = conn_obj.sqlite_connection(db_file)

    def read_table_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number, ijson['table_types'])
        res = [{'message':'done', 'table_ids':m_tables.keys()}]
        return res

    def run_ph_csv(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        ijson.update({"eq":"N","O":"Y"})
        if not ijson.get('table_ids', []):
            m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number, ijson.get('table_types', []))
            ijson['table_ids']  = m_tables.keys()
        print 'tables ', len(ijson['table_ids'])
        if ijson.get('print', '') != 'N':
            cmd = "cd /root/Muthu;python table_tree_group.py 7 %s '%s' ;cd - "%(deal_id, json.dumps(ijson))
        else:
            cmd = "cd /root/Muthu &>/dev/null;python table_tree_group.py 7 %s '%s' >/dev/null;cd - &>/dev/null"%(deal_id, json.dumps(ijson))
        print cmd
        os.system(cmd)
        if ijson.get('PH', '') == 'Y':
            self.update_ph(ijson)
        res = [{'message':'done'}]
        return res

    def read_triplet_data(self,ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2)
        txn_trip             = env1.begin()
        #print lmdb_path2

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        #print lmdb_path2
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()
        triplet =  self.show_triplet(ijson['table_id'], txn_trip, ijson['x'], txn_trip_default)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        tk   = self.get_quid(ijson['table_id']+'_'+ijson['x'])
        print 'C_ID ', [txn_m.get("XMLID_MAP_"+tk)]

    def update_vgh_text(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number)
        db_file         = self.get_db_path(ijson)
        print db_file
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_trip         = env.begin()

        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        #print lmdb_path2
        env1             = lmdb.open(lmdb_path2)
        txn_trip_default             = env1.begin()
        if ijson.get('DF', '') == 'Y':
            txn_trip    = txn_trip_default

        conn, cur       = conn_obj.sqlite_connection(db_file)
        vgh_text_d      = {}
        sql             = "select table_type, vgh, vgh_id from vgh_info"
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            table_type, vgh, vgh_id = rr
            vgh_text_d.setdefault(table_type, {})[vgh.lower()] = (vgh_id, vgh)

        table_ids       = map(lambda x:str(x), ijson['table_ids'])
        sql             = "select row_id, table_type, table_id, xml_id from mt_data_builder where isvisible='Y' and table_id in (%s)"%(','.join(table_ids))
        cur.execute(sql)
        res             = cur.fetchall()
        u_ar            = []
        new_vgh_text_d  = {}
        for rr in res:
            row_id, table_type, table_id, xml_id    = rr
            if xml_id:
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id    = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                triplet =  self.read_triplet(table_id, txn_trip, xml_id, txn_trip_default)
                vgh_txt = triplet.get('VGH', {}).keys()
                if not vgh_txt:
                    vgh_txt = [[txn_m.get('COLTEXT_'+table_id+'_'+str(c_id.split('_')[2]))]]
                    print 'NO Triplet ', [table_id, xml_id]
                    sys.exit()
                vgh_txt = ' '.join(' '.join(vgh_txt[0]).strip().split())
                if vgh_txt.lower() in vgh_text_d.get(table_type, {}):
                    vgh_id  = vgh_text_d[table_type][vgh_txt.lower()][0]
                else:
                    vgh_id  = len(vgh_text_d.get(table_type, {}).keys())+1
                    vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                    new_vgh_text_d.setdefault(table_type, {})[vgh_txt.lower()] = (vgh_id, vgh_txt)
                u_ar.append((vgh_id, row_id))

        vgh_groups  = []
        for k, v in new_vgh_text_d.items():
            for t, vtup in v.items():
                vgh_groups.append((k, vtup[1], vtup[0]))
        print vgh_groups
        print 'Total ', len(u_ar)
        cur.executemany("update mt_data_builder set vgh_text=? where row_id=?", u_ar)
        cur.executemany("insert into vgh_info(table_type, vgh, vgh_id)values(?,?, ?)", vgh_groups)
        conn.commit()
        conn.close()
            
    def get_doc_map_info(self, project_id, deal_id):
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_ph_info = DD(set)
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:str(x).strip(), line)
            ph      = line[3]+line[7]
            doc_id  = line[0]
            doc_type= line[2]
            doc_ph_info[doc_id] = '_'.join([ph, doc_type])
        return doc_ph_info

    def get_vgh_txt_info(self, conn, cur, table_type):
        sql         = "select vgh, vgh_id from vgh_info where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        vgh_d       = {}
        for rr in res:
            vgh_text, vgh_id    = rr
            #try:
            #    vgh_text    = binascii.a2b_hex(vgh_text)
            #except:pass
            vgh_d[vgh_id]       = self.convert_html_entity(vgh_text)
        return vgh_d

    def ph_csv_doc_vgh_data(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        table_type      = ijson['table_type']
        taxo_ids        = map(lambda x:str(x), ijson['t_ids'])
        stype           = ijson['type']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        lmdb_path       =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env             = lmdb.open(lmdb_path)
        txn_m           = env.begin()

        if stype == 'doc':
            # DOC PH and DOC TYPE Info
            doc_map_info    = self.get_doc_map_info(project_id, deal_id)

        elif stype == 'vgh':
            # VGH TEXT INFO
            vgh_txt_info = self.get_vgh_txt_info(conn, cur, table_type)

        sql             = "select row_id, taxo_id, doc_id, table_id, xml_id, vgh_text from mt_data_builder where isvisible='Y' and table_type = '%s' and taxo_id in (%s)"%(table_type, ','.join(taxo_ids))
        cur.execute(sql)
        res             = cur.fetchall()
        conn.close()
        
        doc_taxoid_dict = DD(lambda : DD(dict))  # DD represent Default Dict 
        doc_vgh_info_dict = DD(set)
        for rr in res:
            rr = map(str, rr)
            row_id, taxo_id, doc_id, table_id, xml_id, vgh_text    = rr

            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id    = txn_m.get('XMLID_MAP_'+tk)
            tid, row, col = c_id.split('_')

            k = '%s-%s-'%(tid, col)
            if stype == 'doc':
                unique_id = doc_id
            elif stype == 'vgh':
                unique_id = vgh_text
            doc_taxoid_dict[unique_id][taxo_id][k] = row_id
            doc_vgh_info_dict[unique_id] = 1

        res_data = []
        for unique_id in doc_vgh_info_dict.keys():
            tmp = {'d':unique_id, 'taxos':doc_taxoid_dict.get(unique_id, {})}
            if stype == 'doc':
                tmp['disp'] = doc_map_info.get(unique_id, '')
            elif stype == 'vgh':
                tmp['disp'] = vgh_txt_info.get(unique_id, '')
            res_data.append(tmp)
        res_data.sort(key=lambda x:x['d'])
        return [{'message':'done', 'data':res_data}]

    def save_ph_csv_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        table_type      = ijson['table_type']
        row_ids         = map(lambda x:str(x), ijson['r_ids'])
        data_dict       = ijson['data']
        if 0:

            db_file         = self.get_db_path(ijson)
            conn, cur       = conn_obj.sqlite_connection(db_file)

            try:
                sql     = "CREATE TABLE IF NOT EXISTS mt_data_builder(row_id INTEGER PRIMARY KEY AUTOINCREMENT, taxo_id INTEGER, prev_id INTEGER,order_id INTEGER, table_type, taxonomy TEXT, user_taxonomy TEXT, missing_taxo varchar(1), table_id TEXT, c_id TEXT, gcom TEXT, ngcom TEXT, ph TEXT, ph_label TEXT, user_name TEXT, datetime TEXT, isvisible varchar(1), m_rows TEXT, vgh_text TEXT, vgh_group TEXT, doc_id INTEGER, xml_id TEXT, period VARCHAR(50), period_type VARCHAR(50), scale VARCHAR(50), currency VARCHAR(50), value_type VARCHAR(50))"
                cur.execute(sql)
                cur.commit()
            except:pass

            col_info_stmt   = 'pragma table_info(mt_data_builder);'
            cur.execute(col_info_stmt)
            all_cols        = cur.fetchall()
            cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
            coldef          = ["row_id", "taxo_id", "prev_id", "order_id", "table_type", "taxonomy", "user_taxonomy", "missing_taxo", "table_id", "c_id", 
                               "gcom", "ngcom", "ph", "ph_label", "user_name", "datetime", "isvisible", "m_rows", "vgh_text", "vgh_group", "doc_id", 
                               "xml_id", "period", "period_type", "scale", "currency", "value_type"
                              ]
            exists_coldef   = set(coldef)
            new_cols        = list(exists_coldef.difference(cur_coldef))

            col_list = []
            for new_col in coldef:
                if new_col not in new_cols:continue
                col_list.append(' '.join([new_col, 'VARCHAR(50)']))

            for col in col_list:
                alter_stmt = 'alter table mt_data_builder add column %s;'%(col)
                cur.execute(alter_stmt)
            conn.commit()

        # {pt: "Q3", p: "2043", vt: "BNUM", c: "NOK", s: "Ton"}
        period  = data_dict.get('p', None)
        p_type  = data_dict.get('pt', None)
        scale   = data_dict.get('s', None)
        curr    = data_dict.get('c', None)
        value   = data_dict.get('vt', None)
        
        data_rows = []
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select row_id, table_id, xml_id from mt_data_builder where row_id in(%s)"%(', '.join(row_ids))
        cur.execute(sql)
        res = cur.fetchall()
        conn.close()
        r_d = {}
        for rr in res:
            row_id, table_id, xml_id    = rr
            r_d[str(row_id)]    = (table_id, xml_id)
        
        path    =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env1    = lmdb.open(path)
        f_d     = {}
        if period != None:
            f_d[1]  = period
        if p_type != None:
            f_d[0]  = p_type
        if scale != None:
            f_d[3]  = scale
        if curr != None:
            f_d[2]  = curr
        if value != None:
            f_d[4]  = value
        w_d = {}    
        with env1.begin(write=False) as txn1:
            for rid in row_ids:
                table_id, xml_id    = r_d[rid]
                key = table_id+'_'+self.get_quid(xml_id)
                ph_map  = txn.get('PH_MAP_'+str(key))
                if ph_map:
                    ph_map    = ph_map.split('^')
                else:
                    ph_map   = ['', '', '', '', '']
                for ind, v in f_d.items():
                    ph_map[ind] = v
                w_d['PH_MAP_'+str(key)] = '^'.join(ph_map)
                #data_rows.append((period, p_type, scale, curr, value, rid))
        env1        = lmdb.open(path, map_size=2**39)
        with env1.begin(write=True) as txn1:
            for k, v in w_d.items():
                txn1.put(str(k), str(v))
        #cur.executemany('update mt_data_builder set period=?, period_type=?, scale=?, currency=?, value_type=? where row_id = ?', data_rows)
        #conn.commit()
        #conn.close()
        return [{'message':'done'}]

    ## 43 PH Derivation            
    def copy_ph_derivation_groupid_data(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        user_name       = ijson['user']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        table_type      = ijson['table_type']
        group_ids       = ijson['grpids']
        row_id          = ijson.get('rid', '')

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        sel_stmt = "select formula, table_type, group_id, ph, formula_type, formula_str, user_name from ph_derivation where row_id = '%s'"%(row_id)
        cur.execute(sel_stmt)
        row = cur.fetchone()

        data_li = []
        if row and row[0]:
            data_tup = row
            data_li = list(map(str, data_tup))
            
        data_rows = []
        if data_li:
            for grpid in group_ids:
                data_li[2] = grpid
                data_rows.append(tuple(data_li))
            cur.executemany('insert into ph_derivation (formula, table_type, group_id, ph, formula_type, formula_str, user_name) values(?, ?, ?, ?, ?, ?, ?)', data_rows)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res 

    def get_map_data(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        ID      = ijson['uid']
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        sql = "select group_id, table_type, group_txt from vgh_group_info"
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[(table_type, str(group_id))]   = group_txt

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        sql         = "select row_id, table_type, gen_type, taxo_ids, group_text, datetime from final_output where gen_id=%s"%(ID)
        cur.execute(sql)
        res = cur.fetchall()
        final_ar    = []
        sql         = "select table_type, vgh_id, group_txt, table_str from vgh_group_map"
        cur.execute(sql)
        tres        = cur.fetchall()
        vgh_grp     = {}
        for rr in tres:
            table_type, vgh_id, group_txt, table_str   = rr
            table_type  = str(table_type)
            group_txt   = str(group_txt)
            tc_d    = {}
            if table_str:
                for tcid in table_str.split('#'):
                    tc_d[tcid]  = 1
            vgh_grp.setdefault((table_type, group_txt), {})[vgh_id] = tc_d
        grp_ar_d    = {}
        for rr in res:
            row_id, table_type, gen_type, taxo_ids, group_text, tdatetime  = rr
            grp_ar_d.setdefault(table_type, {})[group_text] = 1
        g_d = {}
        for table_type in grp_ar_d.keys():
            ijson['table_type'] = table_type
            g_ar    = self.create_group_ar(ijson, txn_m)
            for rr in g_ar:
                g_d[(table_type, rr['grpid'])] =  rr
        data = []
        for rr in res:
            row_id, table_type, gen_type, taxo_ids, group_text, tdatetime  = rr
            table_type  = str(table_type)
            group_text   = str(group_text)
            ijson_c   = copy.deepcopy(ijson)
            ijson_c['table_type']   = table_type
            ijson_c['type']         = gen_type
            ijson_c['row_id']         = row_id
            ijson_c['print']         = 'Y'
            if taxo_ids:
                ijson_c['t_ids']         = map(lambda x:int(x), taxo_ids.split(','))
            if gen_type == 'group' and  group_text:
                #print (table_type, group_text), (table_type, group_text) not in vgh_grp
                if (table_type, group_text) not in g_d:continue
                ijson_c['data']     = [g_d[(table_type, group_text)]['n']]
                ijson_c['grpid']    = group_text
                ijson_c['vids']     = g_d[(table_type, group_text)]['vids']
            res = self.create_seq_across(ijson_c)
            data    = res[0]['data']
        return data

    # Generate page_coord.txt
    def store_bobox_data(self, ijson):
        company_name        = ijson['company_name']
        project_id          = ijson['project_id']
        deal_id             = ijson['deal_id']
        company_id          = str(project_id) + '_'+str(deal_id)
        #self.output_path    = '/var/www/html/fundamentals_intf/output/'
        lmdb_folder         = os.path.join(self.output_path, company_id, 'doc_page_adj_cords')
        doc_page_dict       = {}

        if os.path.exists(lmdb_folder):
            env = lmdb.open(lmdb_folder)
            with env.begin() as txn:
                cursor = txn.cursor()
                for doc_id, res_str in cursor:
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
                        doc_page_dict[doc_id] = page_dict
        #print doc_page_dict
        fpath = os.path.join('/var/www/html/DB_Model/', company_name)
        if not os.path.exists(fpath):
            os.system('mkdir -p %s'%fpath)

        fname = os.path.join(fpath, 'page_coord.txt')
        with open(fname, 'w') as f:
            f.write(str(doc_page_dict))
        #print fname 
        return 'done'

    ## 44 Delete/Update formula_str PH Derivation
    def update_ph_derivation_formula_str_data(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        user_name       = ijson['user']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        row_id          = ijson.get('rid', '')
        taxok_map       = ijson.get('tids', {})
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        sel_stmt = "select formula_str from ph_derivation where row_id = '%s'"%(row_id)
        cur.execute(sel_stmt)
        drow = cur.fetchone()
        formula_str = ''
        if drow is not None:
            formula_str = str(drow[0])

        #print formula_str
        if formula_str:
            new_formula_li = []
            all_taxos = formula_str.split('##')
            for taxo_str in all_taxos:
                cell_info_li = map(lambda x:x.split('^^')[0], taxo_str.split('$'))
                taxoid = cell_info_li[0]
                if taxoid in taxok_map:
                    k_li = []
                    all_del_k = taxok_map[taxoid]
                    for ek in cell_info_li[1:]:
                        if ek in all_del_k:
                            continue
                        k_li.append(ek)
                    if not k_li:
                        continue
                    else:
                        new_taxo_str = '$'.join([taxoid]+k_li)
                        new_formula_li.append(new_taxo_str)
                else:
                    new_formula_li.append(taxo_str)

            new_fstr = '##'.join(new_formula_li)
            #print new_fstr
            upd_stmt = 'update ph_derivation set formula_str = "%s" where row_id = "%s"'%(new_fstr, row_id)
            #print upd_stmt
            cur.execute(upd_stmt)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    ## TAXO MAPPING STORAGE
    def store_taxonomy_mapping(self):
        fpath = '/tmp/data_builder_tree.txt'
        data_rows = []
        with open(fpath, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                level, desc = line.split('\t')
                data_rows.append((str(level), unicode(desc).strip(), '0'))
        db_file         = '/mnt/eMB_db/node_mapping.db'
        conn, cur       = conn_obj.sqlite_connection(db_file)

        try:
            drop_stmt = 'drop table taxonomy_grouping;'
            cur.execute(drop_stmt)
        except:pass

        try:
            create_stmt = "CREATE TABLE IF NOT EXISTS taxonomy_grouping (row_id INTEGER PRIMARY KEY AUTOINCREMENT, level VARCHAR(256), description TEXT, review_flg VARCHAR(10));"
            cur.execute(create_stmt)
            conn.commit()
        except:pass
       
        cur.executemany('insert into taxonomy_grouping (level, description, review_flg) values(?, ?, ?)', data_rows)
        conn.commit()
        conn.close()
        return 'done'

    ## 45 Tree View for Taxonomy Mapping
    def taxonomy_mapping_tree_view_data(self, ijson):
        db_file         = '/mnt/eMB_db/node_mapping.db'
        conn, cur       = conn_obj.sqlite_connection(db_file)
        table_name      = 'taxonomy_grouping'

        sel_stmt        = 'select * from taxonomy_grouping'
        cur.execute(sel_stmt)
        rows = cur.fetchall()
        conn.close()
        fmap, tmap      = {}, {} 
        for row in rows:
            row = map(str, row)
            rid, level, desc, rflg = row
            tmap[level]  = [rid, desc, rflg]

            lli = level.split('.')
            nkey = '.'.join(lli[:-1])
            if not nkey:
                fmap.setdefault('root', []).append(level)
                continue
            fmap.setdefault(nkey, []).append(level)
        tmap['root'] = ['999', 'Root', '0']
        #print fmap
        #print tmap
        return [{'fmap':fmap, 'tmap':tmap, 'f1':["root"]}]

    def split_row_multi_all(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select row_id, taxo_id, order_id, table_id, xml_id, table_type from mt_data_builder where isvisible='Y'"
        cur.execute(sql)
        res             = cur.fetchall()
        order_d         = {}
        taxo_o_d        = {}
        new_row_id      = {}
        table_taxo_d    = {}
        for rr in res:
            row_id, taxo_id, order_id, table_id, xml_id, table_type         = rr
            taxo_o_d[taxo_id]                           = order_id
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            #if c_id.split('_')[1]   != '5':continue
            gv_xml  = c_id
            new_row_id[str(row_id)]      = 1
            table_taxo_d.setdefault(taxo_id, {}).setdefault((table_type, table_id), {}).setdefault(int(c_id.split('_')[1]), {})[str(row_id)]   = 1
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            for taxo_id, table_d in table_taxo_d.items():
                for table_id, row_d in table_d.items():
                    if len(row_d.keys()) <= 1:continue
                    rows    = row_d.keys()
                    rows.sort() 
                    for r in rows[1:]:
                        sql     = "insert into mt_data_builder(taxo_id)values(-1)"
                        #cur.execute(sql)
                        #conn.commit()
                        sql     = "update mt_data_builder set taxo_id=%s where row_id in (%s)"%(g_id, ', '.join(row_d[r].keys()))
                        print [table_id, sql]
                        #cur.execute(sql)
                        #conn.commit()
                        g_id    += 1


    '''
        ALTER TABLE COLUMNS
        coldef is list of all columns names [new col names also should be there]
        eg. ['row_id', 'taxo_id', ......]
    '''
    def alter_table_coldef(self, conn, cur, table_name, coldef):
        col_info_stmt   = 'pragma table_info(%s);'%table_name
        cur.execute(col_info_stmt)
        all_cols        = cur.fetchall()
        cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
        exists_coldef   = set(coldef)
        new_cols        = list(exists_coldef.difference(cur_coldef))
        col_list = []
        for new_col in coldef:
            if new_col not in new_cols:continue
            col_list.append(' '.join([new_col, 'TEXT']))
        for col in col_list:
            alter_stmt = 'alter table %s add column %s;'%(table_name, col)
            #print alter_stmt
            cur.execute(alter_stmt)
        conn.commit()
        return 'done'

    def get_tt_desc_map(self, conn, cur):
        sel_stmt        = 'select node_name, description from node_mapping'
        cur.execute(sel_stmt)
        rows = cur.fetchall()
        tt_desc_map = {}
        for row in rows:
            row = map(str, row)
            node_name, desc = row
            tt_desc_map[node_name]  = desc
        return tt_desc_map

    # 48 View Excel Sheet Header
    def get_excel_sheet_names(self, ijson):
        company_name    = str(ijson['company_name'])
        model_number    = str(ijson['model_number'])
        url_id          = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = '_'.join([project_id, url_id])

        classification_db_path = '/mnt/eMB_db/node_mapping.db'
        conn, cur       = conn_obj.sqlite_connection(classification_db_path)
        tt_desc_map     = self.get_tt_desc_map(conn, cur)
        cur.close()
        conn.close()

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        mname = '/var/www/html/DB_Model/%s/' %(company_name)

        try:
            sql         = 'CREATE TABLE IF NOT EXISTS final_output(row_id INTEGER PRIMARY KEY AUTOINCREMENT, gen_id TEXT,table_type TEXT, gen_type TEXT, taxo_ids TEXT, group_text, user_name TEXT, datetime TEXT, error_txt TEXT, header_name TEXT)'
            cur.execute(sql)
            conn.commit()
        except:pass

        coldef    = ['row_id', 'gen_id', 'table_type', 'gen_type', 'taxo_ids', 'group_text', 'user_name', 'datetime', 'error_txt', 'header_name']
        alter_msg = self.alter_table_coldef(conn, cur, 'final_output', coldef)

        sel_stmt        = "select gen_id, table_type, gen_type, group_text, header_name from final_output;"
        cur.execute(sel_stmt)
        rows            = cur.fetchall()
        data_rows = []
        for row in rows:
            row = map(str, row)
            sid, tt, gen_type, group_text, new_header_name = row
            if new_header_name == 'None':
                new_header_name = ''
            '''
            fname = '%s-P.txt'%(sid)
            ff = os.path.join(mname, fname)
            if not os.path.exists(ff):
                continue
            fin = open(ff, 'r')
            lines = fin.readlines()
            fin.close()
            '''
            old_header_name = tt_desc_map.get(tt, tt)
            if gen_type == 'group':
                txt_stmt = 'select group_txt from vgh_group_info where group_id = "%s";' % group_text
                cur.execute(txt_stmt)
                txt_tup = cur.fetchone()
                sub_header = ''
                if txt_tup and txt_tup[0]:
                    sub_header = str(txt_tup[0])
                old_header_name = '%s - %s'%(old_header_name, sub_header)
            data_rows.append({'sid':sid, 'tt':tt, 'on':old_header_name, 'nn':new_header_name}) 
        #print data_rows
        conn.close()
        #data_rows.sort(key = lambda x:x['sid'])
        return [{'message':'done', 'data':data_rows}]

    # 49 Save New Excel Sheet Header
    def save_new_excel_sheet_headers(self, ijson):
        company_name    = str(ijson['company_name'])
        model_number    = str(ijson['model_number'])
        url_id          = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = '_'.join([project_id, url_id])

        data_rows       = ijson['data']

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        for data_dict in data_rows:
            sid, new_header_name = data_dict['sid'], data_dict['nn']
            update_stmt = 'update final_output set header_name = "%s" where gen_id = "%s"'%(new_header_name, sid)
            cur.execute(update_stmt)
        conn.commit()
        conn.close()
        return [{'message':'done'}]

    def update_vgh_doc_group_text(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        try:
            cur.execute("alter table vgh_group_map add column table_str TEXT")
        except:
            pass
        conn.commit()
        conn.close()
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = 'select table_type, doc_id, table_id, vgh_id, group_txt from doc_group_map' 
        try:
            cur.execute(sql)
            res             = cur.fetchall()
        except:
            res = []
        if not res:
            res = [{'message':'done'}]
            conn.close()
            return res

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        
        dd              = {}
        all_tables      = {}
        all_vgh         = {}
        for rr in res:
            table_type, doc_id, table_id, vgh_id, group_txt = rr
            k   = (table_type, vgh_id, group_txt)
            dd.setdefault(k, {})[table_id]  = 1
            all_tables[str(table_id)]   = 1
            all_vgh[str(vgh_id)]         = 1
        sql = "select table_id, xml_id, vgh_text from mt_data_builder where table_id in(%s) and vgh_text in (%s)"%(', '.join(all_tables.keys()), ', '.join(all_vgh.keys()))
        cur.execute(sql)
        res = cur.fetchall()
        v_d     = {}
        for rr in res:
            table_id, xml_id, vgh_id    = rr
            table_id    = str(table_id)
            vgh_id      = str(vgh_id)
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            c           = c_id.split('_')[2]
            v_d.setdefault((vgh_id, table_id), {})[c]     = 1
            
        u_ar    = []
        for k, v in dd.items():
            table_type, vgh_id, group_txt   = k
            t_ar    = []
            for table_id in v.keys():
                for c in v_d[(vgh_id, table_id)].keys():
                    t_ar.append(table_id+'-'+c)
            u_ar.append(('#'.join(t_ar), group_txt, vgh_id, table_type))
            #print  u_ar[-1]
        print 'Total Update ', len(u_ar)
        cur.executemany('update vgh_group_map set table_str=? where group_txt=? and vgh_id=? and table_type=?', u_ar)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    ## 51_old Distinguish Taxonomy View
    def get_taxonomy_distinguish_view_data_old(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        #c_year      = int(datetime.datetime.now().strftime('%Y'))
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            try:
                year    = int(ph[-4:])
            except:continue
            if ph and start_year<year:
                doc_id  = line[0]
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):continue
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

        def update_table_coldef(conn, cur):
            try:
                sql     = "CREATE TABLE IF NOT EXISTS mt_data_builder(row_id INTEGER PRIMARY KEY AUTOINCREMENT, taxo_id INTEGER, prev_id INTEGER,order_id INTEGER, table_type, taxonomy TEXT, user_taxonomy TEXT, missing_taxo varchar(1), table_id TEXT, c_id TEXT, gcom TEXT, ngcom TEXT, ph TEXT, ph_label TEXT, user_name TEXT, datetime TEXT, isvisible varchar(1), m_rows TEXT, vgh_text TEXT, vgh_group TEXT, doc_id INTEGER, xml_id TEXT, period VARCHAR(50), period_type VARCHAR(50), scale VARCHAR(50), currency VARCHAR(50), value_type VARCHAR(50), target_taxonomy TEXT, numeric_flg VARCHAR(10), tb_flg VARCHAR(10))"
                cur.execute(sql)
                cur.commit()
            except:pass
            coldef          = ["row_id", "taxo_id", "prev_id", "order_id", "table_type", "taxonomy", "user_taxonomy", "missing_taxo", "table_id", "c_id", 
                               "gcom", "ngcom", "ph", "ph_label", "user_name", "datetime", "isvisible", "m_rows", "vgh_text", "vgh_group", "doc_id", 
                               "xml_id", "period", "period_type", "scale", "currency", "value_type", "target_taxonomy", "numeric_flg", "tb_flg"
                              ]
            alter_msg       = self.alter_table_coldef(conn, cur, 'mt_data_builder', coldef)
            return 'done'

        i_table_type    = ijson['table_type']

        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()
        table_ph_d  = {}
        all_ph_d    = {}
        table_type  = str(i_table_type)
        f_taxo_arr  = [] #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        taxo_d      = {} #json.loads(txn.get('TABLE_RESULTS_'+table_type, "[]"))
        table_ids   = {}
        g_ar        = []
        table_col_phs   = {}
        consider_tables = {}
        if not taxo_d:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)
            alter_msg   = update_table_coldef(conn, cur)
            if not ijson.get('vids', {}):
                sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
                try:
                    cur.execute(sql)
                    res = cur.fetchall()
                except:
                    res = []
                grp_info    = {}
                for rr in res:
                    group_id, table_type, group_txt = rr
                    grp_info[str(group_id)]   = group_txt
                sql         = "select vgh_id, group_txt from vgh_group_map where table_type='%s'"%(table_type)
                try:
                    cur.execute(sql)
                    res         = cur.fetchall()
                except:
                    res = []
                group_d     = {}
                for rr in res:
                    vgh_id, group_txt   = rr
                    group_d.setdefault(group_txt, {})[vgh_id]   = 1
                g_ar    = []
                for k, v in group_d.items():
                    dd  = {'n':grp_info.get(k,k), 'vids':v.keys(),'grpid':k}
                    g_ar.append(dd)
            if not ijson.get('vids', {}):
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, target_taxonomy, numeric_flg from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            else:
                sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, target_taxonomy, numeric_flg from mt_data_builder where table_type='%s' and isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(ijson['vids']))
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, target_taxonomy, numeric_flg    = rr
                doc_id      = str(doc_id)
                if doc_id not in doc_d:continue
                table_id    = str(table_id)
                if table_id not in m_tables:continue
                tk   = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                gv_xml  = c_id
                consider_tables[table_id] = 1
                c   = int(c_id.split('_')[2])
                table_col_phs.setdefault((table_id, c), {})[ph]   = table_col_phs.setdefault((table_id, c), {}).get(ph, 0) +1
                table_ids[table_id]   = 1
                comp    = ''
                #if gcom == 'Y' or ngcom == 'Y':
                #    comp    = 'Y'
                taxo_d.setdefault(taxo_id, {'l_change':{},'order_id':order_id, 'rid':row_id, 'u_label':user_taxonomy, 't_l':taxonomy, 'missing':missing_taxo, 'comp':comp, 'ks':[], 'm_rows':m_rows, 'target_tx':target_taxonomy, 'num_flg':numeric_flg})['ks'].append((table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id, xml_id))
                if vgh_group == 'N':
                    taxo_d[taxo_id]['l_change'][xml_id]  = 1
            conn.close()
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                row_d   = {}
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        #r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
                        row_d.setdefault(r+tr, []).append((c, t, x))
                r_ld[table_id]  = {}
                for r, c_ar in row_d.items():
                    c_ar.sort()
                    txt = []
                    xml = []
                    for tr in c_ar:
                        txt.append(tr[1])
                        xml.append(tr[2])
                    bbox        = self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml))
                    r_ld[table_id][r]  = (' '.join(txt), ':@:'.join(xml), bbox)
        rc_ld    = {}
        if ijson.get('vids', []):
            for table_id in table_ids.keys():
                k       = 'VGH_'+str(table_id)
                ids     = txn_m.get(k)
                if ids:
                    col_d   = {}
                    ids     = ids.split('#')
                    for c_id in ids:
                        r       = int(c_id.split('_')[1])
                        c       = int(c_id.split('_')[2])
                        cs      = int(txn_m.get('colspan_'+c_id))
                        for tr in range(cs): 
                            col_d.setdefault(c+tr, {})[r]   = c_id
                    for c, rows in col_d.items():
                        rs= rows.keys()
                        rs.sort(reverse=True)
                        for r in rs:
                            c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                            if not c_id or not txn_m.get('TEXT_'+c_id):continue
                            x       = txn_m.get('XMLID_'+c_id)
                            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                            t       = ' '.join(t.split())
                            rc_ld.setdefault(table_id, {})[c]   = (x, t, self.get_bbox_frm_xml(txn1, table_id, x))
                            break
                        
        f_ar        = []
        done_d      = {}
        tmptable_col_phs    = {}
        not_found_ph    = {}
        for k, v in table_col_phs.items():
            phs = v.keys()
            phs.sort(key=lambda x:v[x], reverse=True)
            if len(phs) == 1 and phs[0] == '':
                table_id    = k[0]
                tk       = 'VGH_'+str(table_id)
                ids     = txn_m.get(tk)
                if ids:
                    col_d   = {}
                    ids     = ids.split('#')
                    for c_id in ids:
                        r       = int(c_id.split('_')[1])
                        c       = int(c_id.split('_')[2])
                        cs      = int(txn_m.get('colspan_'+c_id))
                        for tr in range(cs): 
                            col_d.setdefault(c+tr, {})[r]   = c_id
                    for c, rows in col_d.items():
                        if c != k[1]:continue
                        rs= rows.keys()
                        rs.sort(reverse=True)
                        for r in rs:
                            c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                            if not c_id or not txn_m.get('TEXT_'+c_id):continue
                            x       = txn_m.get('XMLID_'+c_id)
                            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                            t       = ' '.join(t.split())
                            tmptable_col_phs[k]   = t
                            not_found_ph.setdefault(k[0], {})[k[1]] = 1
                            break
                pass
            if k not in tmptable_col_phs:
                tmptable_col_phs[k] = phs[0]
        row_ids = taxo_d.keys()
        row_ids.sort(key=lambda x:len(taxo_d[x]['ks']), reverse=True)
        #for row_id, dd in taxo_d.items():
        if ijson.get('gen_output', '') == 'Y':
            tmprows = []
            done_d  = {}
            for row_id in row_ids:
                dd      = taxo_d[row_id]
                ks      = dd['ks']
                tmp_arr = []
                for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id in ks:
                    if (table_id, xml_id) in done_d:continue
                    tmp_arr.append((table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id))
                    done_d[(table_id, xml_id)]=1
                if not tmp_arr:continue
                tmprows.append(row_id)
            row_ids = tmprows[:]
                    
        
        for row_id in row_ids:
            dd      = taxo_d[row_id]
            ks      = dd['ks']
            taxos   = dd['t_l'].split(' / ')[0]
            row     = {'t_id':row_id} #'t_l':taxo}
            f_dup   = ''
            label_d = {}
            target_tx   = dd['target_tx']
            numeric_flg = dd['num_flg']
            label_r_d   = {}
            for table_id, c_id, ph, tlabel, gcom, ngcom, doc_id, xml_id in ks:
                #tk   = self.get_quid(table_id+'_'+xml_id)
                #c_id        = txn_m.get('XMLID_MAP_'+tk)
                #if not c_id:continue
                row.setdefault('tids', {})[table_id]    = 1
                table_id    = str(table_id)
                c_id        = str(c_id)
                r           = int(c_id.split('_')[1])
                c           = int(c_id.split('_')[2])
                x           = txn_m.get('XMLID_'+c_id)
                t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                if tlabel:
                    tlabel  = self.convert_html_entity(tlabel)
                #print [taxo, table_id, c_id, t, ph, tlabel]
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    c_ph        = ph
                    c_tlabel    = tlabel
                else:
                    c_ph        = str(c)
                    c_tlabel    = ''
                row[table_id+'-'+c_ph+'-'+c_tlabel]    = {'v':t, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 't':table_id, 'r':r}
                #if c_id in done_d:
                #    f_dup   = 'Y'
                #    row[table_id+'-'+c_ph+'-'+c_tlabel]['m']    = 'Y'
                if gcom == 'Y':
                    row[table_id+'-'+c_ph+'-'+c_tlabel]['g_f']    = 'Y'
                    row['f']    = 'Y'
                if ngcom == 'Y':
                    row[table_id+'-'+c_ph+'-'+c_tlabel]['ng_f']    = 'Y'
                    row['f']    = 'Y'
                done_d[c_id]    = 1
                if deal_id == '221' and table_type=='RBG' or (deal_id == '44' and table_type == 'OS'):
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, ph)
                else:
                    table_ph_d.setdefault((doc_id, table_id), {})[(c_ph, c_tlabel)]   = (c, tmptable_col_phs[(table_id, c)])
                #print table_id, c_id
                txts, xml_ar, bbox    = r_ld[table_id].get(r, ('', '', ''))
                txts        = self.convert_html_entity(txts)
                grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                label_r_d[grm_txts] = 1
                label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id}

                if xml_id not in dd.get('l_change', {}):
                    label_d[grm_txts]['s']  = 'Y'
                col_txt = rc_ld.get(table_id, {}).get(c, ())
                if col_txt:
                    txts        = self.convert_html_entity(col_txt[1])
                    grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                    label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id}
                    if xml_id not in dd.get('l_change', {}):
                        label_d[grm_txts]['s']  = 'Y'
            if len(label_d.keys()) > 1:
                row['lchange']  = 'Y'
                row['ldata']    = label_d.values()
            lble    = label_r_d.keys()
            lble.sort(key=lambda x:len(x), reverse=True)
            row['taxo']  = dd['t_l']
            if dd.get('u_label', ''):
                row['t_l']  = dd.get('u_label', '')
            else:
                row['t_l']  = label_d[lble[0]]['txt']
            xml_ar  = label_d[ lble[0]]['x'].split(':@:')
            if xml_ar and xml_ar[0]:
                    table_id    =  label_d[ lble[0]]['t']
                    doc_id      = label_d[ lble[0]]['d']
                    p_key   = txn.get('TRPLET_HGH_PINFO_'+table_id+'_'+self.get_quid(xml_ar[0]))
                    if p_key:
                        tmp_xar  = []
                        t_ar    = []
                        for pkinfo in p_key.split(':!:'):
                            pxml, ptext = pkinfo.split('^')
                            tmp_xar.append(pxml)
                            t_ar.append(binascii.a2b_hex(ptext))
                        pxml    = ':@:'.join(tmp_xar)
                        row['parent_txt']   = ' '.join(t_ar) 

            row['x']    = label_d[ lble[0]]['x']
            row['bbox'] = label_d[ lble[0]]['bbox']
            row['t']    = label_d[ lble[0]]['t']
            row['d']    = label_d[ lble[0]]['d']
            row['l']    = len(ks)
            row['fd']   = f_dup
            row['order']= dd['order_id']
            row['rid']  = dd['rid']
            row['target_tx'] = target_tx
            row['num_flg'] = numeric_flg
            if dd.get('m_rows', ''):
                row['merge']    = 'Y'
            if dd.get('missing', ''):
                row['missing']  = dd['missing']
            f_ar.append(row)

        f_ar.sort(key=lambda x:(x['order'], x['rid']))
        data = []
        idx = 1
        for data_dict in f_ar:
            #print x['taxo'], '===', x.get('t_l', ''),'>>>', x.get('parent_txt', ''), '===', x['t_id']
            taxo    = data_dict.get('taxo', '')
            num_flg = data_dict.get('num_flg', 0)
            taxo_ar = taxo.split(' / ')

            inflg = False
            if num_flg is not None and int(num_flg):
                inflg = True
            taxos = []
            for tx in taxo_ar:
                fflg = False
                if tx == data_dict['target_tx']:
                    fflg = True
                taxos.append({'t':str(tx), 'f':fflg, 'id':idx})
                idx += 1
            
            x, bbox, t, d = data_dict['x'], data_dict['bbox'], data_dict['t'], data_dict['d']
            tmp     = {'t_l':str(data_dict.get('t_l', '')), 'taxos':taxos, 't_id':data_dict['t_id'], 'in':inflg, 'x':x, 'bbox':bbox, 'd':d, 't':t}
            data.append(tmp)
        return [{'message':'done', 'data':data, 'tb_flg':'tb'}]

    def taxo_grouping(self, conn, cur, table_type, target_taxo, taxo_id):
        tstmt = "select row_id from taxonomy_mapping where target_taxo = '%s' and table_type = '%s';"%(target_taxo, table_type)
        cur.execute(tstmt)
        drows = cur.fetchall()

        grouped_ar = []
        for row in drows:
            row_id = str(row[0])
            grouped_ar.append((taxo_id, row_id))

        if grouped_ar:
            cur.executemany('update taxonomy_mapping set taxo_id = ? where row_id = ?', grouped_ar)
            conn.commit() 
        return 'done'

    def taxo_table_split_merging(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']

        table_type      = ijson['table_type']
        source_data     = ijson['source']
        merge_split_flg = ijson['ms_flg']

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        if merge_split_flg == 'm':
            t_taxo     = ijson['t_taxo']
            stmt = 'select distinct(target_taxo) from taxonomy_mapping where taxo_id = "%s" and table_type = "%s"'%(t_taxo, table_type)
            cur.execute(stmt)
            cur_tar_taxo_tup = cur.fetchone()
            cur_tar_taxo = str(cur_tar_taxo_tup[0])

            data_ar = []
            for taxo_id, table_ar in source_data.iteritems():
                for table in table_ar:
                    data_ar.append((t_taxo, cur_tar_taxo, taxo_id))
            if data_ar:
                cur.executemany('update taxonomy_mapping set taxo_id = ?, target_taxo = ? where row_id = ?', data_ar)
                conn.commit()

        elif merge_split_flg == 's':
            data_ar = []
            new_taxo_id = hashlib.md5(str(datetime.datetime.now())).hexdigest() 
            for taxo_id, table_ar in source_data.iteritems():
                for table in table_ar:
                    data_ar.append((new_taxo_id, '', taxo_id))
            if data_ar:
                cur.executemany('update taxonomy_mapping set taxo_id = ?, target_taxo = ? where row_id = ?', data_ar)
                conn.commit()
        conn.close()
        return [{'message':'done'}]

    ## 52 Save Asigned Taxonomy
    def save_taxonomy_distinguish_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']

        table_type      = ijson['table_type']
        data_ar         = ijson['list']
        tb_flg          = ijson['tb_flg']

        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)

        data_rows       = []
        edit_data_rows  = []
        for dd in data_ar:
            target_taxo = dd['taxo']
            numeric_flg = dd['in']
            taxo_id     = dd['t_id']
            edit_flg    = dd.get('ef', 0)

            #tmsg = self.taxo_grouping(conn, cur, table_type, target_taxo, taxo_id)

            if edit_flg:
                edit_data_rows.append((target_taxo, numeric_flg, taxo_id, table_type))
            else:
                data_rows.append((target_taxo, numeric_flg, taxo_id, table_type))

        #print edit_data_rows
        #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        #print data_rows

        if edit_data_rows:
            updated_edit_data_rows = []
            for ktup in edit_data_rows:
                ktup = list(ktup)
                target_taxo = ktup[0]
                taxo_id = ktup[2]
                tstmt = "select distinct(taxo_str) from taxonomy_mapping where taxo_id = '%s'"%(taxo_id)
                cur.execute(tstmt)
                drows = cur.fetchone()
                taxo_str = ''
                if drows and drows[0]:  
                    taxo_li = drows[0].split('^!!^')
                    taxo_li = list(set(taxo_li))
                    if target_taxo not in taxo_li:
                        taxo_li.append(target_taxo)
                    taxo_str = '^!!^'.join(taxo_li)
                    ktup.insert(0, taxo_str)
                updated_edit_data_rows.append(tuple(ktup[:]))
            cur.executemany('update taxonomy_mapping set taxo_str=?, target_taxo=?, num_flg=? where taxo_id=? and table_type=?', updated_edit_data_rows)
            conn.commit()

        cur.executemany('update taxonomy_mapping set target_taxo=?, num_flg=? where taxo_id = ? and table_type = ?', data_rows)
        conn.commit()

        cur.execute('update taxonomy_mapping set tb_flg = "%s" where table_type = "%s"'%(tb_flg, table_type))
        conn.commit()

        conn.close()
        return [{'message':'done'}]

    # 51
    def get_taxonomy_distinguish_view_data(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        table_type    = ijson['table_type']
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)

        table_name = 'taxonomy_mapping'
        column_list = [('row_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('order_id', 'INTEGER'), ('taxo_id', 'INTEGER'), ('label', 'TEXT'), ('xml_id', 'TEXT'), ('doc_id', 'VARCHAR(256)'), ('table_id', 'VARCHAR(256)'), ('table_type', 'VARCHAR(256)'), ('taxo_str', 'TEXT'), ('bbox', 'TEXT'), ('target_taxo', 'VARCHAR(256)'), ('num_flg', 'INTEGER'), ('tb_flg', 'VARCHAR(256)')]
        try:
            sql     = "CREATE TABLE IF NOT EXISTS taxonomy_mapping ( %s );"%(', '.join([' '.join(list(x)) for x in column_list]))
            cur.execute(sql)
            conn.commit()
        except:pass

        stmt = "select row_id, order_id, taxo_id, label, xml_id, doc_id, table_id, table_type, taxo_str, bbox, target_taxo, num_flg, tb_flg from taxonomy_mapping where table_type = '%s'"%(table_type)
        cur.execute(stmt)

        rows = cur.fetchall()
        taxo_map = {}
        label_map = {}

        global_tb_flg = 'tb'

        for rr in rows:
            row_id, order_id, taxo_id, label, xml_id, doc_id, table_id, table_type, taxo_str, bbox, target_taxonomy, numeric_flg, tb_flg = rr
            global_tb_flg = tb_flg
            unique_key = str(target_taxonomy).strip()
            if not unique_key:
                unique_key = str(taxo_id).strip()
            taxo_map.setdefault(unique_key, {'taxo_id':taxo_id, 'rid':row_id, 'order_id':order_id, label:[], 'tx_str':[], 'ks':[], 'target_tx':target_taxonomy, 'num_flg':numeric_flg})['ks'].append((table_id, doc_id, xml_id, bbox))
            if taxo_str not in taxo_map[unique_key]['tx_str']:
                taxo_map[unique_key]['tx_str'].append(taxo_str)
            if unique_key not in label_map:
                label_map[unique_key] = {}
            if label not in label_map[unique_key]:
                label_map[unique_key][label] = []
            label_map[unique_key][label].append((table_id, doc_id, xml_id, bbox, row_id))

        f_ar = []
        table_id_map = []
        kid = 1
        for unique_key, ddict in taxo_map.iteritems():
            row                 = {}
            row['t_id']         = ddict['taxo_id']
            label_dict          = label_map.get(unique_key, [])
            lmap = []
            for label, t_info_li in label_dict.iteritems():
                tmap = []
                for t_tup in t_info_li:
                    table_id, doc_id, xml_id, bbox, rid = t_tup
                    tmpbbox = [bbox.split('@^@')]
                    try:
                        tmpbbox = eval(bbox)
                    except:pass
                    tmap.append({'t':table_id, 'd':doc_id, 'x':xml_id, 'b':tmpbbox, 'r':rid})
                tmp = {'l':label, 't_info':tmap, 'kid':kid}
                kid += 1
                lmap.append(tmp)
            row['t_l']          = lmap

            taxo_str_li         = ddict['tx_str']
            ftaxo_str_li        = []
            for taxo_str in taxo_str_li:
                li = taxo_str.split('^!!^')
                ftaxo_str_li += li

            row['taxo']         = '^!!^'.join(set(ftaxo_str_li))
            row['num_flg']      = ddict['num_flg']
            row['target_tx']    = ddict['target_tx']
            row['order_id']     = ddict['order_id']
            ks_ar               = ddict['ks']
            row['table_ids']    = []
            for ks in ks_ar:
                table_id, doc_id, xml_id, bbox = ks
                row['t'] = table_id
                row['x'] = xml_id
                tmpbbox = [bbox.split('@^@')]
                try:
                    tmpbbox = eval(bbox)
                except:pass
                    
                row['bbox'] = tmpbbox
                row['d'] = doc_id
                row['table_ids'].append({'t':table_id, 'x':xml_id, 'd':doc_id, 'bbox':tmpbbox})
            f_ar.append(row)

        f_ar.sort(key=lambda x:x['order_id'])
        data = []
        idx = 1
        for data_dict in f_ar:
            taxo    = data_dict.get('taxo', '')
            table_ids    = data_dict.get('table_ids', [])
            num_flg = data_dict.get('num_flg', 0)
            taxo_ar = taxo.split('^!!^')
            label   = data_dict.get('t_l', [])

            inflg = False
            if num_flg is not None and int(num_flg):
                inflg = True
            taxos = []
            f   = 0
            if not data_dict['target_tx']:
                for tx in taxo_ar[:]:
                    fflg = False
                    if tx == data_dict['target_tx']:
                        fflg = True
                        f   = 1
                    taxos.append({'t':str(tx), 'f':fflg, 'id':idx,'ef':False})
                    idx += 1
            if f == 0:
                for tx in [data_dict['target_tx']]:
                    fflg = False
                    if tx == data_dict['target_tx']:
                        fflg = True
                        f   = 1
                    taxos.append({'t':str(tx), 'f':fflg, 'id':idx,'ef':False})
                    idx += 1
            
            
            x, bbox, t, d = data_dict['x'], data_dict['bbox'], data_dict['t'], data_dict['d']
            tmp  = {'t_l':label, 'taxos':taxos, 't_id':data_dict['t_id'], 'in':inflg, 'x':x, 'bbox':[bbox], 'd':d, 't':t, 'table_ids':table_ids}
            data.append(tmp)
        return [{'message':'done', 'data':data, 'tb_flg':global_tb_flg}]


    def compute_hop_score(self, chain):

        chain_li = list(chain)
        first_elm = chain_li[0]
        diff_measure = []

        for e1 in chain_li[1:]:
            cur_diff = e1-first_elm-1

            diff_measure.append(cur_diff)
            first_elm = e1

        #return max(diff_measure)
        return sum(diff_measure)


    def create_computation_groups_multi_new(self, ijson):

        from common.clean_cell_value_by_lang import CleanCellValueByLang
        clean_obj = CleanCellValueByLang()

        pattern_file = '/mnt/eMB_db/GComp_txt_files/1/ExtnGCPatterns.txt'
        formula_file = '/mnt/eMB_db/GComp_txt_files/1/Extnadd_formula.txt'

        ijson['restated'] = 'N'
        #data = self.create_ph_group(ijson)[0]
        data = self.create_seq_across(ijson)[0]
        #data['data']    = filter(lambda x:x['t_id'] in [50985, 50998, 51002, 51007], data['data'])

        #print len(data['data']); sys.exit()

        all_phs = [each['k'] for each in data['phs']]
        ph_wise_data = dd(lambda : {
                                    'xml_ids': [], 
                                    'values': [], 
                                    'actual_values': []
                                })
        rownum_tid_dict = {}

        for row_num, row in enumerate(data['data']):

            tid = row['t_id']
            rownum_tid_dict[row_num] = tid

            for ph in all_phs:

                v = row.get(ph, {}).get('v', '')
                x = row.get(ph, {}).get('x', '')

                clean_value = 0

                if v.strip():
                    try:
                        clean_value = float(clean_obj.get_clean_value_new(v))
                    except:
                        pass

                ph_wise_data[ph]['values'].append(clean_value)
                ph_wise_data[ph]['xml_ids'].append(x)
                ph_wise_data[ph]['actual_values'].append(v)

        #print dict(ph_wise_data); sys.exit()

        # For col wise computation
        all_comp_results = []

        #print 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm', ph_wise_data.keys() 
        if 0:
            for (k, v) in ph_wise_data.items()[:]:
                if '451' not in k: continue 
                print   k, v
                all_comp_results.append(calculate_comp_results((k, v)))
                for v_ind, t_val in enumerate(v['values'][:]):
                    print v_ind, t_val   
                  
        else:
            from multiprocessing import Pool
            pool = Pool(4)
            all_comp_results = pool.map(calculate_comp_results, [[ph, values] for ph, values in ph_wise_data.items()[:]])

        gcomp_resultant_dict = dd(set)
        ng_resultant_dict = dd(set)

        #print 'lllllllllllllllllllll' 
        for (ph, gresults, ngresults) in all_comp_results:
            #print '========================'  
            #print ph  
            for r in gresults:
                #rtup = (tuple(r[0]), tuple(r[1]), r[2])
                #gcomp_resultant_dict[rtup].add(ph)
                   
                rtup = tuple(r[1]), r[2]
                gcomp_resultant_dict[rtup].add((tuple(r[0]), ph))

            '''
            for r in ngresults:
                rtup = (r[0], tuple(r[1]), r[2])
                ng_resultant_dict[rtup].add(ph)
            #'''
                
        #sys.exit()
        gcomp_result_groups_list = []
        nong_result_groups_list = []

        signs = ('+', '-')
        ng_signs = {
                    'a/b' : ('+', '+'),
                    '(a/b)*100' : ('+', '+'),
                    '((a-b)/b)*100' : ('+', '+'),
                    'a/-b' : ('+', '-'),
                    '(a/-b)*100' : ('+', '-'),
                    '((a-b)/-b)*100' : ('+', '-'),
                    '(a/b)*1000000' : ('+', '+'),
                    '(a/-b)*1000000' : ('+', '-'),
                    '(a/b)*1000' : ('+', '+'),
                    '(a/-b)*1000' : ('+', '-')
                    }

        done_dict = dd(set)

        for result, op_matched_phs in gcomp_resultant_dict.iteritems():

            if result[-1]:
                res_index = result[0][0]
            else:
                res_index = result[0][-1]

            empty_value_sequence = [True] * len(result[0])

            matched_phs = set([p[1] for p in op_matched_phs])
            all_operators_set = set([o[0] for o in op_matched_phs])

            #print all_operators_set; continue; #sys.exit()

            for ph, ph_data in ph_wise_data.iteritems():

                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']

                for i, v in enumerate(result[0]):
                    if empty_value_sequence[i] and actual_values[v].strip():
                        empty_value_sequence[i] = False

                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in result[0]):
                        break

            else: # No break

                # Reject if resultant row is all null
                res_pos = 0 if result[-1] else -1
                if empty_value_sequence[res_pos]: continue

                # Get only those rows where atleast one valid value is present
                new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])

                if len(new_result) < 3: continue

                if result[-1]:
                    m, n = 1, len(new_result)
                else:
                    m, n = 0, len(new_result) - 1

                #hop_score = self.compute_hop_score(result[1])
                hop_score = self.compute_hop_score(new_result)
                res_group = []

                valid_operators_set = set()

                empty_value_sequence = empty_value_sequence[m:n]

                for operators in all_operators_set:

                    new_operators = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])

                    # If its already considered
                    if len(new_operators) < 2 or new_operators in done_dict[new_result]: continue

                    valid_operators_set.add(new_operators)
                    done_dict[new_result].add(new_operators)

                for new_operators in valid_operators_set:

                    cur_row = []
                    for s, r in zip(new_operators, new_result[m:n]):
                        cur_row.append({'tid': rownum_tid_dict[r], 's': signs[s]})

                    # Change the resultant sign
                    if result[-1]:
                        cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G'}] + cur_row
                    else:
                        cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G'})

                    res_group.append(cur_row)

                if res_group:
                    gcomp_result_groups_list.append((res_group, hop_score, new_result[0]))

        '''
        for ngresult, matched_phs in ng_resultant_dict.iteritems():
            for ph, ph_data in ph_wise_data.iteritems():

                xml_ids = ph_data['xml_ids']
                res_index = ngresult[1][0]

                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in ngresult[1]):
                        break

            else: # No break

                ftype = ngresult[0]
                cur_row = []

                for (r, s) in zip(ngresult[1][1:], ng_signs[ftype]):
                    cur_row.append({'tid': rownum_tid_dict[r], 's': s})

                # If the resultant is present at the beginning
                if ngresult[-1]:
                    cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'NG', 'ft': ftype}] + cur_row
                else:
                    cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'NG', 'ft': ftype})

                nong_result_groups_list.append((cur_row, hop_score, ngresult[1][0]))
        #'''

        resultant_f = [each[0] for each in sorted(gcomp_result_groups_list, key=lambda x: (x[1], x[2], len(x[0][0])))]

        return resultant_f


    def create_computation_groups_partial(self, ijson):

        from common.clean_cell_value_by_lang import CleanCellValueByLang
        clean_obj = CleanCellValueByLang()

        pattern_file = '/mnt/eMB_db/GComp_txt_files/1/ExtnGCPatterns.txt'
        formula_file = '/mnt/eMB_db/GComp_txt_files/1/Extnadd_formula.txt'

        ijson['restated'] = 'N'
        #data = self.create_ph_group(ijson)[0]
        data = self.create_seq_across(ijson)[0]

        #print data['phs']; sys.exit()

        ph_groups_dict = {}

        for each in data['phs']:
            ph_groups_dict[each['k']] = each['g']

        all_phs = [each['k'] for each in data['phs']]
        ph_wise_data = dd(lambda : {
                                    'xml_ids': [], 
                                    'values': [], 
                                    'actual_values': []
                                })
        rownum_tid_dict = {}
        tid_phvalue_dict = dd(dict)

        for row_num, row in enumerate(data['data']):

            tid = row['t_id']
            rownum_tid_dict[row_num] = tid

            for ph in all_phs:

                v = row.get(ph, {}).get('v', '')
                x = row.get(ph, {}).get('x', '')

                clean_value = 0

                if v.strip():
                    try:
                        clean_value = float(clean_obj.get_clean_value_new(v))
                    except:
                        pass

                ph_wise_data[ph]['values'].append(clean_value)
                ph_wise_data[ph]['xml_ids'].append(x)
                ph_wise_data[ph]['actual_values'].append(v)
                tid_phvalue_dict[tid][ph] = v

        num_rows = len(rownum_tid_dict)

        #print dict(ph_wise_data); sys.exit()

        # For col wise computation
        all_comp_results = []

        #print 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm', ph_wise_data.keys() 
        if 0:
            for (k, v) in ph_wise_data.items()[:]:
                #if '302' not in k: continue 
                #if k.split('-')[0].strip() not in ['302', '437', '451', '149', '559', '767', '899', '1218', '1229', '1271']: continue
                #if k.split('-')[0].strip() not in ['548', '466', '1236', '1195']: continue
                #if k.split('-')[0].strip() not in ['548']: continue
                #if k.split('-')[0].strip() not in ['478']: continue
                #if k.split('-')[0].strip() not in ['1195']: continue
                print   k, v
                all_comp_results.append(calculate_comp_results((k, v)))
                for v_ind, t_val in enumerate(v['values'][:]):
                    print v_ind, t_val   

            print 'lllllllllllllllllllll' 
            for (ph, gresults, ngresults) in all_comp_results:
                print '========================'  
                print ph  
                for r in gresults:
                    print r  
        else:

            '''
            null_index_map = {}                   
            for ph, vals in ph_wise_data.items():
                #print ph, vals
                #sys.exit()     
                values = vals['actual_values']
                for val_it, val in enumerate(values):
                    if val == '':     
                       if val_it not in null_index_map:
                          null_index_map[val_it] = [] 
                       null_index_map[val_it].append(ph)

            for n_k, n_phs in null_index_map.items():
            '''

            from multiprocessing import Pool
            pool = Pool(6)
            all_comp_results = pool.map(calculate_comp_results, [[ph, values] for ph, values in ph_wise_data.items()[:]])

        gcomp_resultant_dict = dd(set)
        ng_resultant_dict = dd(set)

        for (ph, gresults, ngresults) in all_comp_results:
            for r in gresults:
                rtup = tuple(r[1]), r[2]
                gcomp_resultant_dict[rtup].add((tuple(r[0]), ph))
                #print r

            '''
            for r in ngresults:
                rtup = (r[0], tuple(r[1]), r[2])
                ng_resultant_dict[rtup].add(ph)
            #'''

        #sys.exit()
        gcomp_result_groups_list = []
        nong_result_groups_list = []

        signs = ('+', '-')
        ng_signs = {
                    'a/b' : ('+', '+'),
                    '(a/b)*100' : ('+', '+'),
                    '((a-b)/b)*100' : ('+', '+'),
                    'a/-b' : ('+', '-'),
                    '(a/-b)*100' : ('+', '-'),
                    '((a-b)/-b)*100' : ('+', '-'),
                    '(a/b)*1000000' : ('+', '+'),
                    '(a/-b)*1000000' : ('+', '-'),
                    '(a/b)*1000' : ('+', '+'),
                    '(a/-b)*1000' : ('+', '-')
                    }

        done_dict = dd(set)
        done_set = set()

        for result, op_matched_phs in gcomp_resultant_dict.iteritems():
            if result[-1]:
                res_index = result[0][0]
            else:
                res_index = result[0][-1]

            empty_value_sequence = [True] * len(result[0])
            matched_phs = set([p[1] for p in op_matched_phs])
            all_operators_set = set([o[0] for o in op_matched_phs])

            ph_operandsign_dict = dd(list)

            correct_formulas = [] 
            incorrect_formulas = [] 
            break_flg = 0 

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']

                # Find if any valid entry is present
                for i, v in enumerate(result[0]):
                    if empty_value_sequence[i] and actual_values[v].strip():
                        empty_value_sequence[i] = False

            if result[-1]:
               # first one is resultant
               if empty_value_sequence[0] == True:
                  continue  
            else:
               # last one is resultant   
               if empty_value_sequence[-1] == True:
                  continue 

            if True in empty_value_sequence:
               false_count = 0
               for e in empty_value_sequence:
                   if e == False:
                      false_count += 1
               if false_count < 2:
                  # Number of resultant and operands are less than 2
                  continue

               #print ' old: ', result
               #print ' all_operators_set: ', all_operators_set 
               new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])
               result = (new_result, result[-1])
               #print ' new: ', new_result         
               
               new_all_operators_set = []
               for operators in all_operators_set:
                    new_operators = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])
                    new_all_operators_set.append(new_operators)
 
               #print ' new: ', new_all_operators_set 
               result = (new_result, result[-1])
               all_operators_set = new_all_operators_set[:] 

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']
                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    #if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in result[0]):
                    if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                    else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                    if actual_values[res_index].strip() and any(actual_values[x].strip() for x in res_operands[:]):
                        break_flg = 1
                        ##break
                        #print '==================' 
                        #print ' Incorrect reason: ', xml_ids[res_index].strip(), ph
                        #print 'Incorrect ', result[0], ph, ' ==> ', matched_phs 
                        #print actual_values  
                        incorrect_formulas.append(ph)           
                else:
                   if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                   else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                   if actual_values[res_index].strip() and (any(actual_values[x].strip() for x in res_operands[:])): 
                      #Resultant should be non-null
                      correct_formulas.append(ph)          
                   else:
                      break_flg = 1
                          
            correct_tabs = map(lambda x:x.split('-')[0], correct_formulas[:])
            incorrect_tabs = map(lambda x:x.split('-')[0], incorrect_formulas[:])

            ctabs = dd(list)
            ictabs = dd(list)

            for tab in correct_formulas:
                tab_id, col, _ = tab.split('-')
                ctabs[tab_id].append(tab.strip())

            for tab in incorrect_formulas:
                tab_id, col, _ = tab.split('-')
                ictabs[tab_id].append(tab.strip())

            correct_tabs  = list(sets.Set(correct_tabs))
            incorrect_tabs = list(sets.Set(incorrect_tabs))

            if break_flg: 
               s1 = sets.Set(correct_tabs)
               s2 = sets.Set(incorrect_tabs)
                  
               if s1.intersection(s2): 
                  s = s1 - s2
                  correct_tabs = list(s)
                    
                  #continue
               if (len(correct_tabs) < 3):
                  continue
               if 0:    
                  print result 
                  print correct_formulas, correct_tabs 
                  print incorrect_formulas, incorrect_tabs 
                  
                  #sys.exit() 
            #else: # Present in all cols, hence don't include
            #   continue
                    
            if 0:
                # Reject if resultant row is all null
                res_pos = 0 if result[-1] else -1
                if empty_value_sequence[res_pos]: continue

                # Get only those rows where atleast one valid value is present
                new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])

                if len(new_result) < 3: continue

                if result[-1]:
                    m, n = 1, len(new_result)
                else:
                    m, n = 0, len(new_result) - 1

                #hop_score = self.compute_hop_score(result[1])
                hop_score = self.compute_hop_score(new_result)

                valid_operators_set = set()

                empty_value_sequence = empty_value_sequence[m:n]

                for operators in all_operators_set:
                    new_operators = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])

                    # If its already considered
                    if len(new_operators) < 2 or new_operators in done_dict[new_result]: continue

                    #valid_operators_set.add(new_operators)
                    done_dict[new_result].add(new_operators)

            if 1:
                hop_score = self.compute_hop_score(result[0])
                res_group = []

                if result[-1]:
                    m, n = 1, len(result[0])
                else:
                    m, n = 0, len(result[0]) - 1

                for new_operators in all_operators_set:
                    tids = []
                    cur_row = []
                    for s, r in zip(new_operators, result[0][m:n]):
                        cur_row.append({'tid': rownum_tid_dict[r], 's': signs[s]})
                        tids.append(rownum_tid_dict[r])

                    # Change the resultant sign
                    if result[-1]:
                        cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs)}] + cur_row
                        tids = [rownum_tid_dict[res_index]] + tids
                    else:
                        cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs)})
                        tids.append(rownum_tid_dict[res_index])

                    tids = tuple(tids), result[-1]
                    #print result , ' tids: ', tids , tids not in done_set 
                    if tids not in done_set:
                        res_group.append(cur_row)
                        done_set.add(tids)
                        break

                if res_group:
                    gcomp_result_groups_list.append((res_group, hop_score, result[0][0], break_flg))

        '''
        for ngresult, matched_phs in ng_resultant_dict.iteritems():
            for ph, ph_data in ph_wise_data.iteritems():

                xml_ids = ph_data['xml_ids']
                res_index = ngresult[1][0]

                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in ngresult[1]):
                        break

            else: # No break

                ftype = ngresult[0]
                cur_row = []

                for (r, s) in zip(ngresult[1][1:], ng_signs[ftype]):
                    cur_row.append({'tid': rownum_tid_dict[r], 's': s})

                # If the resultant is present at the beginning
                if ngresult[-1]:
                    cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'NG', 'ft': ftype}] + cur_row
                else:
                    cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'NG', 'ft': ftype})

                nong_result_groups_list.append((cur_row, hop_score, ngresult[1][0]))
        #'''

        #print gcomp_result_groups_list; sys.exit()
        #for each in gcomp_result_groups_list:
        #    print each; sys.exit()

        gcomp_result_groups_list = sorted(gcomp_result_groups_list, key=lambda x: (x[1], x[2], len(x[0][0])))

        resultant_f = [(each[0], each[-1]) for each in gcomp_result_groups_list]# if each[-1]==0] + [(each[0], each[-1]) for each in gcomp_result_groups_list if each[-1]==1]

        #print resultant_f; sys.exit()

        return resultant_f

    def create_computation_groups_for_selected_rows(self, ijson, data):
        from common.clean_cell_value_by_lang import CleanCellValueByLang
        clean_obj = CleanCellValueByLang()

        pattern_file = '/mnt/eMB_db/GComp_txt_files/1/ExtnGCPatterns.txt'
        formula_file = '/mnt/eMB_db/GComp_txt_files/1/Extnadd_formula.txt'

        ijson['restated'] = 'N'
        #data = self.create_ph_group(ijson)[0]
        #data = self.create_seq_across(ijson)[0]

        #print data['phs']; sys.exit()

        ph_groups_dict = {}

        for each in data['phs']:
            ph_groups_dict[each['k']] = each['g']

        all_phs = [each['k'] for each in data['phs']]
        ph_wise_data = dd(lambda : {
                                    'xml_ids': [], 
                                    'values': [], 
                                    'actual_values': []
                                })
        rownum_tid_dict = {}
        tid_phvalue_dict = dd(dict)
        sel_rc_cnt = 0
        for row_num, row in enumerate(data['data']):
            if sel_rc_cnt > 10:
                break
            tid = row['t_id']
            rownum_tid_dict[row_num] = tid
            for ph in all_phs:

                v = row.get(ph, {}).get('v', '')
                x = row.get(ph, {}).get('x', '')

                clean_value = 0

                if v.strip():
                    try:
                        clean_value = float(clean_obj.get_clean_value_new(v))
                    except:
                        pass

                ph_wise_data[ph]['values'].append(clean_value)
                ph_wise_data[ph]['xml_ids'].append(x)
                ph_wise_data[ph]['actual_values'].append(v)
                tid_phvalue_dict[tid][ph] = v
            sel_rc_cnt += 1

        if 0 and sel_rc_cnt > 10:
            return 0, []
        
        num_rows = len(rownum_tid_dict)
        # For col wise computation
        all_comp_results = []
        if 0:
            for (k, v) in ph_wise_data.items()[:]:
                #if k.split('-')[0].strip() not in ['302', '437', '451', '149', '559', '767', '899', '1218', '1229', '1271']: continue
                print   k, v
                all_comp_results.append(calculate_comp_results((k, v)))
                for v_ind, t_val in enumerate(v['values'][:]):
                    print v_ind, t_val   

            for (ph, gresults, ngresults) in all_comp_results:
                print '========================'  
                print ph  
                for r in gresults:
                    print r  
        else:

            from multiprocessing import Pool
            pool = Pool(6)
            all_comp_results = pool.map(calculate_comp_results, [[ph, values] for ph, values in ph_wise_data.items()[:]])

        gcomp_resultant_dict = dd(set)
        ng_resultant_dict = dd(set)

        for (ph, gresults, ngresults) in all_comp_results:
            for r in gresults:
                rtup = tuple(r[1]), r[2]
                gcomp_resultant_dict[rtup].add((tuple(r[0]), ph))
                #print r

        #sys.exit()
        gcomp_result_groups_list = []
        nong_result_groups_list = []

        signs = ('+', '-')
        
        done_dict = dd(set)
        done_set = set()

        for result, op_matched_phs in gcomp_resultant_dict.iteritems():
            if result[-1]:
                res_index = result[0][0]
            else:
                res_index = result[0][-1]

            empty_value_sequence = [True] * len(result[0])
            matched_phs = set([p[1] for p in op_matched_phs])
            all_operators_set = set([o[0] for o in op_matched_phs])
            default_operator_signs = tuple([0 for _ in range(len(result[0]))])

            ph_operandsign_dict = dd(list)

            correct_formulas = [] 
            incorrect_formulas = [] 
            break_flg = 0 

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']

                # Find if any valid entry is present
                for i, v in enumerate(result[0]):
                    if empty_value_sequence[i] and actual_values[v].strip():
                        empty_value_sequence[i] = False

            if result[-1]:
               # first one is resultant
               if empty_value_sequence[0] == True:
                  continue  
            else:
               # last one is resultant   
               if empty_value_sequence[-1] == True:
                  continue 

            if True in empty_value_sequence:
               false_count = 0
               for e in empty_value_sequence:
                   if e == False:
                      false_count += 1
               if false_count < 2:
                  # Number of resultant and operands are less than 2
                  continue

               new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])
               result = (new_result, result[-1])
               #print ' new: ', new_result         
               
            #'''
            ## NEW CHANGE
            if result[-1]:
                 m, n = 1, len(result[0])
            else:
                m, n = 0, len(result[0]) - 1

            empty_value_sequence = empty_value_sequence[m:n]

            for (operators, ph) in op_matched_phs:
                new_signs = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])
                if len(new_signs) == len(result[0]) - 1: # # of signs should be one less than # of operands
                    ph_operandsign_dict[ph].append(new_signs)
            #'''

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']
                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    #if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in result[0]):
                    if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                    else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                    if actual_values[res_index].strip() and any(actual_values[x].strip() for x in res_operands[:]):
                        break_flg = 1
                        incorrect_formulas.append(ph)           
                else:
                   if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                   else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                   if actual_values[res_index].strip() and (any(actual_values[x].strip() for x in res_operands[:])): 
                      #Resultant should be non-null
                      correct_formulas.append(ph)          
                   else:
                      break_flg = 1
                          
            correct_tabs = map(lambda x:x.split('-')[0], correct_formulas[:])
            incorrect_tabs = map(lambda x:x.split('-')[0], incorrect_formulas[:])

            ctabs = dd(list)
            ictabs = dd(list)

            '''
            for tab in correct_formulas:
                tab_id, col, _ = tab.split('-')
                ctabs[tab_id].append(tab.strip())

            for tab in incorrect_formulas:
                tab_id, col, _ = tab.split('-')
                ictabs[tab_id].append(tab.strip())
            #'''

            correct_tabs  = list(sets.Set(correct_tabs))
            incorrect_tabs = list(sets.Set(incorrect_tabs))

            if break_flg: 
               s1 = sets.Set(correct_tabs)
               s2 = sets.Set(incorrect_tabs)
                  
               if s1.intersection(s2): 
                  s = s1 - s2
                  correct_tabs = list(s)
                    
               if (len(correct_tabs) < 3):
                  continue

            for tab in correct_formulas:
                tab_id, col, _ = tab.split('-')
                if tab_id in correct_tabs and tab_id not in incorrect_tabs: ctabs[tab_id].append(tab.strip())

            for tab in incorrect_formulas:
                tab_id, col, _ = tab.split('-')
                ictabs[tab_id].append(tab.strip())

            if 0:
                # Reject if resultant row is all null
                res_pos = 0 if result[-1] else -1
                if empty_value_sequence[res_pos]: continue

                # Get only those rows where atleast one valid value is present
                new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])

                if len(new_result) < 3: continue

                if result[-1]:
                    m, n = 1, len(new_result)
                else:
                    m, n = 0, len(new_result) - 1

                #hop_score = self.compute_hop_score(result[1])
                hop_score = self.compute_hop_score(new_result)

                valid_operators_set = set()

                empty_value_sequence = empty_value_sequence[m:n]

                for operators in all_operators_set:
                    new_operators = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])

                    # If its already considered
                    if len(new_operators) < 2 or new_operators in done_dict[new_result]: continue

                    #valid_operators_set.add(new_operators)
                    done_dict[new_result].add(new_operators)

            if 1:
                hop_score = self.compute_hop_score(result[0])
                res_group = []

                if result[-1]:
                    m, n = 1, len(result[0])
                else:
                    m, n = 0, len(result[0]) - 1

                if len(result[0]) < 3: continue

                default_operator_signs = [0] * (n-m)

                #'''
                ## NEW CHANGE
                group_dict = dd(list)

                for ph in all_phs:
                #for ph, operand_signs_list in ph_operandsign_dict.iteritems():
                    operand_signs_list = ph_operandsign_dict.get(ph, [default_operator_signs])

                    group = ph_groups_dict[ph]

                    # Add the operator signs for this set of tids(index) under this ph
                    operand_signs = operand_signs_list[0] # Consider only the first entry

                    tids = []
                    cur_row = []

                    if n-m != len(operand_signs):
                        print n, m, operand_signs
                        continue

                    for s, r in zip(operand_signs, result[0][m:n]):
                        cur_row.append({'tid': rownum_tid_dict[r], 's': signs[s], 'v': tid_phvalue_dict[rownum_tid_dict[r]][ph]})
                        tids.append(rownum_tid_dict[r])

                    # Change the resultant sign
                    if result[-1]:
                        #cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs), 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] }] + cur_row
                        cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] }] + cur_row

                        tids = [rownum_tid_dict[res_index]] + tids

                    else:
                        #cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs), 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] })
                        cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] })

                        tids.append(rownum_tid_dict[res_index])

                    tid_tup = tuple(tids), result[-1]
                    if ph in done_dict[tid_tup]:
                        continue
                    done_dict[tid_tup].add(ph)

                    if len(cur_row) > 2: group_dict[group].append(cur_row)#, hop_score, result[0][0], break_flg))

                if group_dict:
                    gcomp_result_groups_list.append([dict(group_dict), hop_score, result[0][0], len(result[0]), 'G', dict(ctabs), dict(ictabs), break_flg])

                #'''

        ## NEW CHANGE
        gcomp_result_groups_list = sorted(gcomp_result_groups_list, key=lambda x: (x[1], x[2], x[3]))
        resultant_f = [[each[0], each[-4], each[-3], each[-2], each[-1]] for each in gcomp_result_groups_list if each[-1]==0] + [(each[0], each[-4], each[-3], each[-2], each[-1]) for each in gcomp_result_groups_list if each[-1]==1]
        return 1, resultant_f

    def create_computation_groups_partial_new(self, ijson):

        from common.clean_cell_value_by_lang import CleanCellValueByLang
        clean_obj = CleanCellValueByLang()

        pattern_file = '/mnt/eMB_db/GComp_txt_files/1/ExtnGCPatterns.txt'
        formula_file = '/mnt/eMB_db/GComp_txt_files/1/Extnadd_formula.txt'

        ijson['restated'] = 'N'
        #data = self.create_ph_group(ijson)[0]
        data = self.create_seq_across(ijson)[0]

        #print data['phs']; sys.exit()

        ph_groups_dict = {}

        for each in data['phs']:
            ph_groups_dict[each['k']] = each['g']

        all_phs = [each['k'] for each in data['phs']]
        ph_wise_data = dd(lambda : {
                                    'xml_ids': [], 
                                    'values': [], 
                                    'actual_values': []
                                })
        rownum_tid_dict = {}
        tid_phvalue_dict = dd(dict)

        for row_num, row in enumerate(data['data']):

            tid = row['t_id']
            rownum_tid_dict[row_num] = tid

            for ph in all_phs:

                v = row.get(ph, {}).get('v', '')
                x = row.get(ph, {}).get('x', '')

                clean_value = 0

                if v.strip():
                    try:
                        clean_value = float(clean_obj.get_clean_value_new(v))
                    except:
                        pass

                ph_wise_data[ph]['values'].append(clean_value)
                ph_wise_data[ph]['xml_ids'].append(x)
                ph_wise_data[ph]['actual_values'].append(v)
                tid_phvalue_dict[tid][ph] = v

        num_rows = len(rownum_tid_dict)

        # For col wise computation
        all_comp_results = []

        if 1:
            for (k, v) in ph_wise_data.items()[:]:
                #if k.split('-')[0].strip() not in ['302', '437', '451', '149', '559', '767', '899', '1218', '1229', '1271']: continue
                print   k, v
                all_comp_results.append(calculate_comp_results((k, v)))
                for v_ind, t_val in enumerate(v['values'][:]):
                    print v_ind, t_val   

            for (ph, gresults, ngresults) in all_comp_results:
                print '========================'  
                print ph  
                for r in gresults:
                    print r  
        else:

            from multiprocessing import Pool
            pool = Pool(6)
            all_comp_results = pool.map(calculate_comp_results, [[ph, values] for ph, values in ph_wise_data.items()[:]])

        gcomp_resultant_dict = dd(set)
        ng_resultant_dict = dd(set)

        for (ph, gresults, ngresults) in all_comp_results:
            for r in gresults:
                rtup = tuple(r[1]), r[2]
                gcomp_resultant_dict[rtup].add((tuple(r[0]), ph))
                #print r

        #sys.exit()
        gcomp_result_groups_list = []
        nong_result_groups_list = []

        signs = ('+', '-')
        
        done_dict = dd(set)
        done_set = set()

        for result, op_matched_phs in gcomp_resultant_dict.iteritems():
            if result[-1]:
                res_index = result[0][0]
            else:
                res_index = result[0][-1]

            empty_value_sequence = [True] * len(result[0])
            matched_phs = set([p[1] for p in op_matched_phs])
            all_operators_set = set([o[0] for o in op_matched_phs])

            ph_operandsign_dict = dd(list)

            correct_formulas = [] 
            incorrect_formulas = [] 
            break_flg = 0 

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']

                # Find if any valid entry is present
                for i, v in enumerate(result[0]):
                    if empty_value_sequence[i] and actual_values[v].strip():
                        empty_value_sequence[i] = False

            if result[-1]:
               # first one is resultant
               if empty_value_sequence[0] == True:
                  continue  
            else:
               # last one is resultant   
               if empty_value_sequence[-1] == True:
                  continue 

            if True in empty_value_sequence:
               false_count = 0
               for e in empty_value_sequence:
                   if e == False:
                      false_count += 1
               if false_count < 2:
                  # Number of resultant and operands are less than 2
                  continue

               new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])
               result = (new_result, result[-1])
               #print ' new: ', new_result         
               
            #'''
            ## NEW CHANGE
            if result[-1]:
                m, n = 1, len(result[0])
            else:
                m, n = 0, len(result[0]) - 1

            empty_value_sequence = empty_value_sequence[m:n]

            for (operators, ph) in op_matched_phs:
                new_signs = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])
                ph_operandsign_dict[ph].append(new_signs)
            #'''

            for ph, ph_data in ph_wise_data.iteritems():
                xml_ids = ph_data['xml_ids']
                actual_values = ph_data['actual_values']
                # If resultant is present and any operand is valid, then reject the current sequence as its not present in all phs
                if ph not in matched_phs:
                    #if xml_ids[res_index].strip() and any(xml_ids[x].strip() for x in result[0]):
                    if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                    else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                    if actual_values[res_index].strip() and any(actual_values[x].strip() for x in res_operands[:]):
                        break_flg = 1
                        incorrect_formulas.append(ph)           
                else:
                   if result[-1]:
                       # first one is resultant
                       res_operands = result[0][1:]
                   else:
                       # last one is resultant
                       res_operands = result[0][:-1]
                   if actual_values[res_index].strip() and (any(actual_values[x].strip() for x in res_operands[:])): 
                      #Resultant should be non-null
                      correct_formulas.append(ph)          
                   else:
                      break_flg = 1
                          
            correct_tabs = map(lambda x:x.split('-')[0], correct_formulas[:])
            incorrect_tabs = map(lambda x:x.split('-')[0], incorrect_formulas[:])

            ctabs = dd(list)
            ictabs = dd(list)

            for tab in correct_formulas:
                tab_id, col, _ = tab.split('-')
                ctabs[tab_id].append(tab.strip())

            for tab in incorrect_formulas:
                tab_id, col, _ = tab.split('-')
                ictabs[tab_id].append(tab.strip())

            correct_tabs  = list(sets.Set(correct_tabs))
            incorrect_tabs = list(sets.Set(incorrect_tabs))

            if break_flg: 
               s1 = sets.Set(correct_tabs)
               s2 = sets.Set(incorrect_tabs)
                  
               if s1.intersection(s2): 
                  s = s1 - s2
                  correct_tabs = list(s)
                    
               if (len(correct_tabs) < 3):
                  continue
                    
            if 0:
                # Reject if resultant row is all null
                res_pos = 0 if result[-1] else -1
                if empty_value_sequence[res_pos]: continue

                # Get only those rows where atleast one valid value is present
                new_result = tuple([op_index for op_index, is_valid in zip(result[0], empty_value_sequence) if not is_valid])

                if len(new_result) < 3: continue

                if result[-1]:
                    m, n = 1, len(new_result)
                else:
                    m, n = 0, len(new_result) - 1

                #hop_score = self.compute_hop_score(result[1])
                hop_score = self.compute_hop_score(new_result)

                valid_operators_set = set()

                empty_value_sequence = empty_value_sequence[m:n]

                for operators in all_operators_set:
                    new_operators = tuple([op for op, is_valid in zip(operators, empty_value_sequence) if not is_valid])

                    # If its already considered
                    if len(new_operators) < 2 or new_operators in done_dict[new_result]: continue

                    #valid_operators_set.add(new_operators)
                    done_dict[new_result].add(new_operators)

            if 1:
                hop_score = self.compute_hop_score(result[0])
                res_group = []

                if result[-1]:
                    m, n = 1, len(result[0])
                else:
                    m, n = 0, len(result[0]) - 1

                #'''
                ## NEW CHANGE
                group_dict = dd(list)

                for ph, operand_signs_list in ph_operandsign_dict.iteritems():

                    group = ph_groups_dict[ph]

                    # Add the operator signs for this set of tids(index) under this ph
                    operand_signs = operand_signs_list[0] # Consider only the first entry

                    tids = []
                    cur_row = []
                    for s, r in zip(operand_signs, result[0][m:n]):
                        cur_row.append({'tid': rownum_tid_dict[r], 's': signs[s], 'v': tid_phvalue_dict[rownum_tid_dict[r]][ph]})
                        tids.append(rownum_tid_dict[r])

                    # Change the resultant sign
                    if result[-1]:
                        cur_row = [{'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs), 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] }] + cur_row

                        tids = [rownum_tid_dict[res_index]] + tids

                    else:
                        cur_row.append({'tid': rownum_tid_dict[res_index], 's': '=', 'R': 'Y', 'c': 'G', 'ctabs': dict(ctabs), 'ictabs': dict(ictabs), 'ph': ph, 'v': tid_phvalue_dict[rownum_tid_dict[res_index]][ph] })

                        tids.append(rownum_tid_dict[res_index])

                    group_dict[group].append(cur_row)#, hop_score, result[0][0], break_flg))

                if group_dict:
                    gcomp_result_groups_list.append((dict(group_dict), hop_score, result[0][0], len(result[0]), break_flg))

                #'''

        ## NEW CHANGE
        gcomp_result_groups_list = sorted(gcomp_result_groups_list, key=lambda x: (x[1], x[2], x[3]))

        resultant_f = [(each[0], each[-1]) for each in gcomp_result_groups_list]# if each[-1]==0] + [(each[0], each[-1]) for each in gcomp_result_groups_list if each[-1]==1]

        #print resultant_f; sys.exit()

        #for each in resultant_f:
        #    print each; sys.exit()

        return resultant_f


    def store_sys_formula(self, ijson):

        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        res = self.read_doc_info(ijson)[0]
        if res['message'] != 'done':
            return [{'message': 'error', 'data' : []}]

        all_table_types = [each['k'] for each in res['mt_list']]

        #results_path = os.path.join('/tmp/ak_comp/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation')

        '''
        # Remove old results
        try:
            os.remove(os.path.join(results_path, 'data.mdb'))
            os.remove(os.path.join(results_path, 'lock.mdb'))
        except OSError:
            pass
        #'''

        # Make the directory
        try:
            os.makedirs(results_path)
        except OSError:
            pass

        error_dict = {}

        # Make changes to the ijson input, tt and gid and call create_computation_groups_multi_new and store the results
        for tt in all_table_types[:]:

            #print tt
            #if tt != 'BS': continue
        
            new_ijson = copy.deepcopy(ijson)
            new_ijson['table_type'] = tt

            res = self.create_seq_across(new_ijson)[0]

            all_group_ids = [str(each['grpid']) for each in res['g_ar']]

            all_group_ids.append('') # default, no group
            #print all_group_ids

            for gid in all_group_ids:

                new_ijson = copy.deepcopy(ijson)
                new_ijson['table_type'] = tt

                new_ijson['grpid'] = gid

                if 1:
                    result = self.create_computation_groups_multi_new(new_ijson)

                #except Exception as e:
                #    error_dict[(tt, gid)] = e

                if 1:
                    # Store the result into a lmdb
                    with lmdb.open(results_path) as env:
                        with env.begin(write=True) as txn:
                            txn.put(str((tt, gid)), str(result))

        msg = 'error' if error_dict else 'done'

        return [{'message': msg, 'data': error_dict}]


    def store_sys_formula_partial(self, ijson):

        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        res = self.read_doc_info(ijson)[0]
        if res['message'] != 'done':
            return [{'message': 'error', 'data' : []}]

        all_table_types = [each['k'] for each in res['mt_list']]

        #results_path = os.path.join('/tmp/ak_comp_partial/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation')

        '''
        # Remove old results
        try:
            os.remove(os.path.join(results_path, 'data.mdb'))
            os.remove(os.path.join(results_path, 'lock.mdb'))
        except OSError:
            pass
        #'''

        # Make the directory
        try:
            os.makedirs(results_path)
        except OSError:
            pass

        error_dict = {}

        # Make changes to the ijson input, tt and gid and call create_computation_groups_partial and store the results
        for tt in all_table_types[:]:

            #if tt != 'RBS': continue
        
            new_ijson = copy.deepcopy(ijson)
            new_ijson['table_type'] = tt

            res = self.create_seq_across(new_ijson)[0]

            all_group_ids = [str(each['grpid']) for each in res['g_ar']]

            all_group_ids.append('') # default, no group
            #print tt
            #print all_group_ids

            for gid in all_group_ids:

                #if gid != '': continue

                new_ijson = copy.deepcopy(ijson)
                new_ijson['table_type'] = tt

                new_ijson['grpid'] = gid

                if 1:
                    result = self.create_computation_groups_partial(new_ijson)

                #except Exception as e:
                #    error_dict[(tt, gid)] = e

                if 1:
                    # Store the result into a lmdb
                    with lmdb.open(results_path, map_size=30*1024*1024*1024) as env:
                        with env.begin(write=True) as txn:
                            txn.put(str((tt, gid)), str(result))

        msg = 'error' if error_dict else 'done'
        #sys.exit()

        return [{'message': msg, 'data': error_dict}]

    def compute_eq(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        sel_table_type  = ijson['table_type']
        sel_group_id    = ijson['grpid']
        company_id      = "%s_%s"%(project_id, deal_id)
        res = self.read_doc_info(ijson)[0]
        if res['message'] != 'done':
            return [{'message': 'error', 'data' : []}]
        all_table_types = [each['k'] for each in res['mt_list'] if each['k'] == sel_table_type]
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_new')
        # Make the directory
        try:
            os.makedirs(results_path)
        except OSError:
            pass
        error_dict = {}
        result = []
        # Make changes to the ijson input, tt and gid and call create_computation_groups_partial and store the results
        for tt in all_table_types[:]:
            new_ijson = copy.deepcopy(ijson)
            new_ijson['table_type'] = tt
            res = self.create_seq_across(new_ijson)[0]
            all_group_ids = list(set([''] + [str(each1['grpid']) for each1 in res['g_ar']]))
            all_group_ids = [e for e in all_group_ids if e == sel_group_id]
            for gid in all_group_ids:
                new_ijson = copy.deepcopy(ijson)
                new_ijson['table_type'] = tt
                new_ijson['grpid'] = gid
                rflg, cres = self.create_computation_groups_for_selected_rows(new_ijson, res)
                if rflg and cres:
                    result = []
                    for each in cres:
                        cur_formula = {}
                        rtid = -1
                        res_loc = ''
                        for doc_group, ph_results_list in each[0].iteritems():
                            ctabs = {}
                            ictabs = {}
                            ctype = 'G'
                            if ph_results_list:
                                rrows = [{} for _ in range(len(ph_results_list[0]))]
                            else:
                                continue
                            if len(rrows) < 3: continue
                            for ph_data in ph_results_list[:]:
                                res_pos = 0 if 'R' in ph_data[0] else -1
                                res_loc = 'F' if not res_pos else 'L'
                                res_element = ph_data[res_pos]
                                #res_element['ps_f'] = each[-1]
                                rtid = res_element['tid']
                                cur_ph = res_element['ph']
                                df = []
                                for i, op in enumerate(ph_data):
                                    cur_ph_data = {}
                                    cur_ph_data['s'] = op['s']
                                    cur_ph_data['v'] = op['v']
                                    rrows[i][cur_ph] = cur_ph_data
                                    rrows[i]['tid'] = op['tid']
                                    df.append(op['tid'])
                                rrows[res_pos][cur_ph]['R'] = 'Y'
                            cur_formula[doc_group] = rrows
                        if cur_formula:# and (int(each[-1]) == 0):
                            df = '@@'.join(map(str, df)) + '^^' + res_loc
                            result.append({'formula': cur_formula, 'tid_str': df, 'rtid': rtid, 'ctabs': each[2], 'ictabs': each[3], 'ctype': each[1], 'ps_f': each[-1]});

                    msg = 'done'

        try:
            sFormula_data = self.get_sFormula_data(ijson)
        except:
            sFormula_data = []

        return [{'message': 'done', 'data': result, 'sformula':sFormula_data}]
        
    def store_sys_formula_partial_new(self, ijson):

        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        res = self.read_doc_info(ijson)[0]
        if res['message'] != 'done':
            return [{'message': 'error', 'data' : []}]

        all_table_types = [each['k'] for each in res['mt_list']]

        #results_path = os.path.join('/tmp/ak_comp_partial/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_new')

        '''
        # Remove old results
        try:
            os.remove(os.path.join(results_path, 'data.mdb'))
            os.remove(os.path.join(results_path, 'lock.mdb'))
        except OSError:
            pass
        #'''

        # Make the directory
        try:
            os.makedirs(results_path)
        except OSError:
            pass

        error_dict = {}

        # Make changes to the ijson input, tt and gid and call create_computation_groups_partial and store the results
        for tt in all_table_types[:]:

            #if tt != 'RBS': continue
        
            new_ijson = copy.deepcopy(ijson)
            new_ijson['table_type'] = tt

            res = self.create_seq_across(new_ijson)[0]

            all_group_ids = [str(each['grpid']) for each in res['g_ar']]

            all_group_ids.append('') # default, no group
            #print tt
            #print all_group_ids

            for gid in all_group_ids:

                #if gid != '': continue

                new_ijson = copy.deepcopy(ijson)
                new_ijson['table_type'] = tt

                new_ijson['grpid'] = gid

                if 1:
                    result = self.create_computation_groups_partial_new(new_ijson)

                #except Exception as e:
                #    error_dict[(tt, gid)] = e

                if 1:
                    # Store the result into a lmdb
                    with lmdb.open(results_path, map_size=30*1024*1024*1024) as env:
                        with env.begin(write=True) as txn:
                            txn.put(str((tt, gid)), str(result))

        msg = 'error' if error_dict else 'done'
        #sys.exit()

        return [{'message': msg, 'data': error_dict}]


    def get_stored_sys_formula(self, ijson):

        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        tt = str(ijson['table_type'])
        gid = str(ijson.get('grpid', ''))

        #results_path = os.path.join('/tmp/ak_comp/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation')
        result = []

        if not os.path.exists(results_path):
            msg = 'Results not found'

        else:
            with lmdb.open(results_path) as env:
                with env.begin(write=False) as txn:
                    res = eval(txn.get(str((tt, gid)), '[]'))
                    msg = 'done'
                    result = [each[0] for each in res]

        try:
            sFormula_data = self.get_sFormula_data(ijson)
        except:
            sFormula_data = []

        return [{'message': 'done', 'data': result, 'sformula':sFormula_data}]


    def get_stored_sys_formula_partial(self, ijson):

        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        tt = str(ijson['table_type'])
        gid = str(ijson.get('grpid', ''))
        partial_flag = ijson.get('ps_f', 0)

        #results_path = os.path.join('/tmp/ak_comp_partial/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation')
        result = []

        if not os.path.exists(results_path):
            msg = 'Results not found'

        else:
            with lmdb.open(results_path) as env:
                with env.begin(write=False) as txn:
                    res = eval(txn.get(str((tt, gid)), '[]'))
                    result = []
                    for each in res:
                        for op in each[0][0]:
                            if 'R' in op:
                                op['ps_f'] = each[-1]
                                break

                        result.append(each[0][0])
                    msg = 'done'

                    '''
                    sform = []
                    pform = []
                    for each in res:
                        for op in each[0][0]:
                            if 'R' in op:
                                op['ps_f'] = each[-1]
                                break
                        if each[-1]: pform.append(each[0][0])
                        else: sform.append(each[0][0])

                    result = sform + pform

                    #'''

        try:
            sFormula_data = self.get_sFormula_data(ijson)
        except:
            sFormula_data = []

        return [{'message': 'done', 'data': result, 'sformula':sFormula_data}]

    def index_group_taxonomy_partial_data(self, ijson):
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        #tt = str(ijson['table_type'])
        gid = str(ijson.get('grpid', ''))
        partial_flag = ijson.get('ps_f', 0)

        #results_path = os.path.join('/tmp/ak_comp_partial/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_new')
        result = []
        partial_indexing_data_dict = {}
        if not os.path.exists(results_path):
            msg = 'Results not found'

        else:
            with lmdb.open(results_path) as env:
                with env.begin(write=False) as txn:
                    for gkey, res in txn.cursor():
                        res = eval(res)
                        org_group_key = gkey
                        gkey = eval(gkey)
                        tt, gid = gkey
                        partial_indexing_data_dict[org_group_key] = {}
                        result = []
                        for each in res:
                            #print len(res)
                            cur_formula = {}
                            rtid = -1
                            res_loc = ''
                            for doc_group, ph_results_list in each[0].iteritems():

                                ctabs = {}
                                ictabs = {}
                                ctype = 'G'

                                if ph_results_list:
                                    rrows = [{} for _ in range(len(ph_results_list[0]))]
                                else:
                                    continue

                                if len(rrows) < 3: continue

                                for ph_data in ph_results_list[:]:

                                    res_pos = 0 if 'R' in ph_data[0] else -1
                                    res_loc = 'F' if not res_pos else 'L'
                                    res_element = ph_data[res_pos]
                                    #res_element['ps_f'] = each[-1]
                                    rtid = res_element['tid']
                                    cur_ph = res_element['ph']
                                    df = []
                                    for i, op in enumerate(ph_data):
                                        cur_ph_data = {}
                                        cur_ph_data['s'] = op['s']
                                        cur_ph_data['v'] = op['v']
                                        rrows[i][cur_ph] = cur_ph_data
                                        rrows[i]['tid'] = op['tid']
                                        df.append(op['tid'])
                                    rrows[res_pos][cur_ph]['R'] = 'Y'
                                    #ctabs = res_element['ctabs']
                                    #ictabs = res_element['ictabs']
                                    #ctype = res_element['c']

                                cur_formula[doc_group] = rrows
                            
                            if cur_formula:
                                rtid = str(rtid)
                                if rtid not in partial_indexing_data_dict[org_group_key]:
                                    partial_indexing_data_dict[org_group_key][rtid] = [] 
                                df = '@@'.join(map(str, df)) + '^^' + res_loc
                                partial_indexing_data_dict[org_group_key][rtid].append({'formula': cur_formula, 'tid_str': df, 'rtid': rtid, 'ctabs': each[2], 'ictabs': each[3], 'ctype': each[1], 'ps_f': each[-1]})
        ###################################################################################
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_indxdata') 
        print results_path
        with lmdb.open(results_path, map_size=30*1024*1024*1024) as env:
            with env.begin(write=True) as txn:
                for k, v in partial_indexing_data_dict.items():
                    txn.put(k, str(v))
        ###################################################################################

    def get_stored_sys_formula_partial_new_sel(self, ijson):
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        tt = str(ijson['table_type'])
        gid = str(ijson.get('grpid', ''))
        partial_flag = ijson.get('ps_f', 0)
        sel_tid = str(ijson.get('tid', ''))
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_indxdata')
        result = []
        partial_indexing_data_dict = {}
        if not os.path.exists(results_path):
            msg = 'Results not found'
        else:
            with lmdb.open(results_path) as env:
                with env.begin(write=False) as txn:
                    tdict = eval(txn.get(str((tt, gid)), '{}'))
                    result = tdict.get(sel_tid, [])
                    new_result = []
                    for ar in result:
                        if int(ar['ps_f']) == 0:
                            cnt = 100
                        else:
                            cnt = len(ar['ctabs'].keys())
                        new_result.append((cnt, ar))
                    new_result.sort()
                    new_result.reverse()
                    result = map(lambda x:x[1], new_result[:])
        try:
            sFormula_data = self.get_sFormula_data(ijson)
        except:
            sFormula_data = []
        return [{'message': 'done', 'data': result, 'sformula':sFormula_data}]


    def get_stored_sys_formula_partial_new(self, ijson):

        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        tt = str(ijson['table_type'])
        gid = str(ijson.get('grpid', ''))
        partial_flag = ijson.get('ps_f', 0)

        #results_path = os.path.join('/tmp/ak_comp_partial/', company_id)
        results_path = os.path.join(self.output_path, company_id, 'grid_computation_new')
        result = []

        if not os.path.exists(results_path):
            msg = 'Results not found'

        else:
            with lmdb.open(results_path) as env:
                with env.begin(write=False) as txn:
                    res = eval(txn.get(str((tt, gid)), '[]'))
                    result = []
                    for each in res:
                        #print len(res)
                        cur_formula = {}
                        rtid = -1
                        res_loc = ''
                        for doc_group, ph_results_list in each[0].iteritems():

                            ctabs = {}
                            ictabs = {}
                            ctype = 'G'

                            if ph_results_list:
                                rrows = [{} for _ in range(len(ph_results_list[0]))]
                            else:
                                continue

                            if len(rrows) < 3: continue

                            for ph_data in ph_results_list[:]:

                                res_pos = 0 if 'R' in ph_data[0] else -1
                                res_loc = 'F' if not res_pos else 'L'
                                res_element = ph_data[res_pos]
                                #res_element['ps_f'] = each[-1]
                                rtid = res_element['tid']

                                cur_ph = res_element['ph']
                                df = []

                                for i, op in enumerate(ph_data):
                                    cur_ph_data = {}
                                    cur_ph_data['s'] = op['s']
                                    cur_ph_data['v'] = op['v']
                                    rrows[i][cur_ph] = cur_ph_data
                                    rrows[i]['tid'] = op['tid']
                                    df.append(op['tid'])

                                rrows[res_pos][cur_ph]['R'] = 'Y'
                                #ctabs = res_element['ctabs']
                                #ictabs = res_element['ictabs']
                                #ctype = res_element['c']

                            cur_formula[doc_group] = rrows
                        if len(result) > 200:
                            break
                        if cur_formula and (int(each[-1]) == 0):
                            df = '@@'.join(map(str, df)) + '^^' + res_loc
                            result.append({'formula': cur_formula, 'tid_str': df, 'rtid': rtid, 'ctabs': each[2], 'ictabs': each[3], 'ctype': each[1], 'ps_f': each[-1]});
                        #break

                    msg = 'done'


        new_result = []
        for ar in result:
            if int(ar['ps_f']) == 0:
                cnt = 100
            else:
                cnt = len(ar['ctabs'].keys())
            new_result.append((cnt, ar))
        new_result.sort()
        new_result.reverse()
        result = map(lambda x:x[1], new_result[:])
  
        try:
            sFormula_data = self.get_sFormula_data(ijson)
        except:
            sFormula_data = []

        return [{'message': 'done', 'data': result, 'sformula':sFormula_data}]


    def alter_sel_table(self, company_names, table_name, col_names):
        model_number = '1'
        for company_name in company_names:
            db_file         = self.get_db_path(ijson)
            conn, cur   = conn_obj.sqlite_connection(db_file)

            for col_name in col_names:
                sql         = "alter table %s add column %s TEXT"%(table_name, col_name)
                try:
                    cur.execute(sql)
                except:pass
            conn.commit()
            conn.close()
        res = [{'message':'done'}]
        return res

    def alter_table_data(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        table_name      = ijson['t']
        cols            = ijson['c']
        self.alter_sel_table([company_name], table_name, cols)


    def alter_table(self, ijson):
        if 0:
            return self.alter_table_data(ijson)
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        if 1:
            sql         = "alter table vgh_group_map add column doc_vgh TEXT"
            try:
                cur.execute(sql)
            except:pass
            sql         = "alter table vgh_group_map add column table_str TEXT"
            try:
                cur.execute(sql)
            except:pass
            try:
                sql = "update vgh_group_map set doc_vgh='VGH'"
                cur.execute(sql)
            except:pass
        sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        cur.execute(sql)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res



    def validate_csv(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        table_type      = ijson['table_type']
        company_id      = "%s_%s"%(project_id, deal_id)
        res = self.read_doc_info(ijson)[0]
        if res['message'] != 'done':
            return [{'message': 'error', 'data' : []}]
        #################################################################################
        data = self.create_seq_across(ijson)[0]
        all_phs = [each['k'] for each in data['phs']]
        #taxo_wise_currency_dict = {}
        #taxo_wise_scale_dict = {}
        gbl_col_dict = {}
        high_ligh_lst = []
        for row_num, row in enumerate(data['data']):
            tid = row['t_id']
            currency_dict = {}
            scale_dict = {}
            for ph in all_phs:
                ph_info = row.get(ph, {}).get('phcsv', {})
                v = row.get(ph, {}).get('v', {})
                x = row.get(ph, {}).get('x', {})
                c = ph_info.get('c', '').strip()
                s = ph_info.get('s', '').strip()
                if 1:
                    if ph not in gbl_col_dict:
                        gbl_col_dict[ph] = {}
                    gbl_col_dict[ph][tid] = row.get(ph, {})
                if c:
                    if c not in currency_dict:
                        currency_dict[c] = []
                    currency_dict[c].append((ph, x, tid, c))
                if s:
                    if s not in scale_dict:
                        scale_dict[s] = []
                    scale_dict[s].append((ph, x, tid, s))
            
            if len(currency_dict.keys()) > 1:
                high_ligh_lst += self.get_diff_refs(currency_dict)
            if len(scale_dict.keys()) > 1:
                high_ligh_lst += self.get_diff_refs(scale_dict)
        ###############################################################################
        for ph, tx_dict in gbl_col_dict.items():
            currency_dict = {}
            scale_dict = {}
            for tid, row_info in tx_dict.items():
                x = row_info.get('x', '')
                c = row_info.get('c', '')
                s = row_info.get('s', '')
                ph_info = row_info.get('phcsv', {})
                if c:
                    if c not in currency_dict:
                        currency_dict[c] = []
                    currency_dict[c].append((ph, x, tid, c))
                if s:
                    if s not in scale_dict:
                        scale_dict[s] = []
                    scale_dict[s].append((ph, x, tid, s))
            
            if len(currency_dict.keys()) > 1:
                high_ligh_lst += self.get_diff_refs(currency_dict)
            if len(scale_dict.keys()) > 1:
                high_ligh_lst += self.get_diff_refs(scale_dict)
        mdict_new = {}
        for k in high_ligh_lst:
            if k[2] not in mdict_new:
                mdict_new[k[2]] = {}
            mdict_new[k[2]][k[0]] = 'N' 
            
        return [{'message': 'done', 'data' : mdict_new}]

    def get_diff_refs(self, ddict):
        ddlst = []
        for k, vs in ddict.items():
            #for v in vs:
            n = len(vs)
            ddlst.append((n, k, vs))
        ddlst.sort()
        ddlst.reverse()
        diff_lst = []
        for dtup  in ddlst[1:]:
            xs = dtup[2]
            diff_lst += xs
        return diff_lst


    def create_HGH_group(self, ijson):
        if len(ijson.get('t_ids', {}).keys()) > 1:
            res = [{'message':'Error More than taxonomy not allowed'}]
            return res
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        i_table_type    = ijson['table_type']
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()


        table_type  = str(i_table_type)
        group_d     = {}
        revgroup_d  = {}

        d_group_d     = {}
        d_revgroup_d  = {}

        vgh_id_d    = {}
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        #sql         = "select vgh_id, doc_id, table_id, group_txt from doc_group_map where table_type='%s'"%(table_type)
        #try:
        #    cur.execute(sql)
        #    res = cur.fetchall()
        #except:
        #    res = []
        grp_doc_map_d   = {}
        rev_doc_map_d   = {}
        #for rr in res:
        #    vgh_id, doc_id, table_id, group_txt = rr
        #    grp_doc_map_d.setdefault(group_txt, {})[table_id]   = doc_id #.setdefault(doc_id, {})[table_id]    = 1
        #    rev_doc_map_d.setdefault((doc_id, table_id), {})[group_txt] = 1
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        self.alter_table_coldef(conn, cur, 'mt_data_builder', ['target_taxonomy', 'numeric_flg', 'th_flg'])
        sql = "select vgh_group_id, doc_group_id, group_txt from vgh_doc_map where table_type='%s'"%(table_type)
        cur.execute(sql)
        doc_vgh_map = {}
        res = cur.fetchall()
        for rr in res:
            vgh_group_id, doc_group_id, group_txt   = rr
            doc_vgh_map[(vgh_group_id, doc_group_id)]   = group_txt

        sql = "select group_id, table_type, group_txt from vgh_group_info where table_type='%s'"%(table_type)
        try:
            cur.execute(sql)
            res = cur.fetchall()
        except:
            res = []
        grp_info    = {}
        for rr in res:
            group_id, table_type, group_txt = rr
            grp_info[("GRP", str(group_id))]   = group_txt


        sql         = "select row_id, vgh_id, group_txt, table_str, doc_vgh from vgh_group_map where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        for rr in res:
            row_id, vgh_id, group_txt, table_str, doc_vgh   = rr
            if doc_vgh == 'DOC':
                d_group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                d_revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)
                pass
            else:
                group_d.setdefault(group_txt, {})[vgh_id]   = (row_id, table_str)
                revgroup_d.setdefault(vgh_id, {})[group_txt]    = (row_id, table_str)

        t_ids   = map(lambda x:str(x), ijson.get('t_ids', {}).keys())
        if t_ids:
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s' and taxo_id in (%s)"%(table_type, ', '.join(t_ids))
        else:
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s'"%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        docinfo_d   = {}
        vgh_id_d_all    = {}
        tmp_grpd        = {}
        table_ids       = {}
        all_vgh         = {}
        l_list          = ijson.get('t_ids', {}).values()
        l_list.sort(key=lambda x:len(x), reverse=True)
        f_taxo          = l_list[0]
        for rr in res:
            row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg   = rr
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            doc_id  = str(doc_id)
            vgh_id  = vgh_text
            if str(doc_id) in doc_d and str(table_id) in m_tables:
                all_vgh[vgh_id]         = 1
                table_ids[table_id]       = 1
                tstr    = table_id+'-'+c_id.split('_')[2]
                #print '\n==========================================='
                rr  = (table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg)
                if vgh_id in revgroup_d:
                    for grp_id in revgroup_d.get(vgh_id, {}).keys():
                        t   = {}
                        if not revgroup_d[vgh_id][grp_id][1]:
                            t[tstr]  = doc_id
                        elif tstr in revgroup_d[vgh_id][grp_id][1].split('#'):
                            t[tstr]  = doc_id
                        doc_grpid   = ''
                        if doc_id in d_revgroup_d:
                            doc_grpid   = d_revgroup_d[doc_id].keys()[0]
                        tmp_grpd.setdefault(doc_grpid, {}).setdefault(("GRP", grp_id), {})[rr]   = 1
                        #print doc_grpid, ("GRP", grp_id), c_id
                else:
                    doc_grpid   = ''
                    if doc_id in d_revgroup_d:
                        doc_grpid   = d_revgroup_d[doc_id].keys()[0]
                    tmp_grpd.setdefault(doc_grpid, {}).setdefault(("VGH", vgh_id), {})[rr]   = 1
                    #print doc_grpid, ("VGH", vgh_id), c_id
        sql         = "select vgh, vgh_id from vgh_info where table_type='%s' and vgh_id in (%s)"%(table_type, ', '.join(all_vgh.keys()))
        cur.execute(sql)
        res         = cur.fetchall()
        vgh_d       = {}
        for rr in res:
            vgh_text, vgh_id    = rr
            if vgh_id:
                try:
                    vgh_text    = binascii.a2b_hex(vgh_text)
                except:pass
                grp_info[("VGH", vgh_id)]       = self.convert_html_entity(vgh_text)
        r_ld    = {}
        for table_id in table_ids.keys():
            k       = 'HGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                ids     = ids.split('#')
                row_d   = {}
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    key     = table_id+'_'+self.get_quid(x)
                    t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                    t       = ' '.join(t.split())
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        #r_ld.setdefault(table_id, {}).setdefault(r+tr, []).append((c, t))
                        row_d.setdefault(r+tr, []).append((c, t, x))
                r_ld[table_id]  = {}
                for r, c_ar in row_d.items():
                    c_ar.sort()
                    txt = []
                    xml = []
                    for tr in c_ar:
                        txt.append(tr[1])
                        xml.append(tr[2])
                    bbox        = self.get_bbox_frm_xml(txn1, table_id, ':@:'.join(xml))
                    r_ld[table_id][r]  = (' '.join(txt), ':@:'.join(xml), bbox)
        rc_ld    = {}
        for table_id in table_ids.keys():
            k       = 'VGH_'+str(table_id)
            ids     = txn_m.get(k)
            if ids:
                col_d   = {}
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    cs      = int(txn_m.get('colspan_'+c_id))
                    for tr in range(cs): 
                        col_d.setdefault(c+tr, {})[r]   = c_id
                for c, rows in col_d.items():
                    rs= rows.keys()
                    rs.sort(reverse=True)
                    for r in rs:
                        c_id        = str(table_id)+'_'+str(r)+'_'+str(c)
                        if not c_id or not txn_m.get('TEXT_'+c_id):continue
                        x       = txn_m.get('XMLID_'+c_id)
                        t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
                        t       = ' '.join(t.split())
                        rc_ld.setdefault(table_id, {})[c]   = (x, t, self.get_bbox_frm_xml(txn1, table_id, x))
                        break
        g_i_ar  = []
        grp_ar  = []
        t_ars   = []
        all_table_types = {}
        for doc_grpid, tgrp_info in tmp_grpd.items(): 
            tmp_grp_name    = f_taxo
            doc_name    = ''
            if ("GRP", doc_grpid) in grp_info:
                tmp_grp_name    = f_taxo+' - '+grp_info[("GRP", doc_grpid)]
                doc_name    = doc_grpid
            f_ar    = []
            table_ph_d  = {}
            f_taxo_ar   = []
            tmpt_d      = {}
            oid         = 1
            for grp_id_tup, rows in tgrp_info.items():
                flg, grp_id = grp_id_tup
                grp_name    = grp_info.get(grp_id_tup, ' - '.join(grp_id_tup))
                grp_name    = doc_vgh_map.get((grp_id, doc_grpid), grp_name)    
                ks      = rows
                taxos   = grp_name
                t_id    = '_'.join(grp_id_tup)+' - '+doc_grpid
                row     = {'t_id':t_id} #'t_l':taxo}
                f_dup   = ''
                label_d = {}
                label_r_d   = {}
                ph_cnt      = {}
                ks  = []
                t_ar    = []
                #for rr in rows.keys():
                r_ks    = rows.keys()
                r_ks.sort(key=lambda x:tuple(map(lambda x1:int(x1), x[5].split('_'))))
                for rr in r_ks:
                    table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg    = rr
                    ks.append((table_id, c_id))
                    row.setdefault('tids', {})[table_id]    = 1
                    table_id    = str(table_id)
                    c_id        = str(c_id)
                    r           = int(c_id.split('_')[1])
                    c           = int(c_id.split('_')[2])
                    x           = txn_m.get('XMLID_'+c_id)
                    t           = self.convert_html_entity(binascii.a2b_hex(txn_m.get('TEXT_'+c_id)))
                    tlabel      = ph_label
                    if tlabel:
                        tlabel  = self.convert_html_entity(tlabel)
                    c_ph        = ph
                    c_tlabel    = ''
                    if (table_id, c_ph) in ph_cnt:
                        c_tlabel    = str(ph_cnt[(table_id, c_ph)]+1)
                        ph_cnt[(table_id, c_ph)]    = ph_cnt[(table_id, c_ph)]+1
                    else:   
                        ph_cnt[(table_id, c_ph)]    = 0
                    row[table_id+'-'+c_ph+'-'+c_tlabel]    = {'v':t, 'x':x, 'bbox':self.get_bbox_frm_xml(txn1, table_id, x), 'd':doc_id, 't':table_id, 'r':r}
                    table_ph_d.setdefault((doc_id, table_id), {}).setdefault((c_ph, c_tlabel), {})[(c, ph)] = 1
                    #print table_id, c_id
                    txts, xml_ar, bbox    = r_ld[table_id].get(r, ('', '', ''))
                    txts        = self.convert_html_entity(txts)
                    grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                    #label_r_d[grm_txts] = 1
                    #label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':bbox, 'x':xml_ar, 'd':doc_id, 't':table_id}

                    col_txt = rc_ld.get(table_id, {}).get(c, ())
                    if col_txt:
                        txts        = self.convert_html_entity(col_txt[1])
                        grm_txts    = txts.lower() #self.remove_grm_mrks(txts).lower()
                        label_r_d[grm_txts] = 1
                        label_d.setdefault(grm_txts, {'id':xml_id, 'txt':txts, 'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id, 'v':{}})['v'][doc_d[doc_id][0]]    = {'bbox':col_txt[2], 'x':col_txt[0], 'd':doc_id, 't':table_id}
                    rr      = list(rr)
                    rr[0]   =  'HGHGROUP-'+i_table_type+'-'+'_'.join(t_ids)+'-'+doc_grpid
                    all_table_types['"'+rr[0]+'"'] = {}
                    t_ar.append(rr)
                if len(label_d.keys()) > 1:
                    row['lchange']  = 'Y'
                    row['ldata']    = label_d.values()
                lble    = label_r_d.keys()
                lble.sort(key=lambda x:len(x), reverse=True)
                row['taxo']     = grp_name
                row['t_l']      = grp_name
                xml_ar  = label_d[ lble[0]]['x'].split(':@:')
                if xml_ar and xml_ar[0]:
                        table_id    =  label_d[ lble[0]]['t']
                        doc_id      = label_d[ lble[0]]['d']
                        p_key   = txn.get('TRPLET_HGH_PINFO_'+table_id+'_'+self.get_quid(xml_ar[0]))
                        if p_key:
                            tmp_xar  = []
                            t1_ar    = []
                            for pkinfo in p_key.split(':!:'):
                                pxml, ptext = pkinfo.split('^')
                                tmp_xar.append(pxml)
                                t1_ar.append(binascii.a2b_hex(ptext))
                            pxml    = ':@:'.join(tmp_xar)
                            row['parent_txt']   = ' '.join(t1_ar) 

                row['x']    = label_d[ lble[0]]['x']
                row['bbox'] = label_d[ lble[0]]['bbox']
                row['t']    = label_d[ lble[0]]['t']
                row['d']    = label_d[ lble[0]]['d']
                row['l']    = len(ks)
                row['fd']   = f_dup
                f_taxo_ar.append({'t_l':[t_id], 'ks':ks})
                f_ar.append(row)
                tmpt_d[t_id]  = {'d':t_ar, 'o':oid, 't_l':grp_name}
                oid += 1

            tdphs = report_year_sort.year_sort(dphs.keys())
            tdphs.reverse()
            #alphs = report_year_sort.year_sort(all_ph_d.keys())
            #alphs.reverse()
            table_ids   = table_ph_d.keys()
            try:
                table_ids.sort(key=lambda x:(tdphs.index(doc_d[x[0]][0]), x[1]))
            except:
                table_ids.sort(key=lambda x:(x[0], x[1]))
                pass
            f_taxo_ar  = self.order_by_table_structure_by_col(f_taxo_ar, table_ids)
            order_d = {}
            for ii, rr in enumerate(f_taxo_ar):
                order_d[rr['t_l'][0]]   = ii+1
                tmpt_d[rr['t_l'][0]]['o']    = ii+1
            f_ar.sort(key=lambda x:order_d[x['t_id']])
            t_ars.append(tmpt_d)    
            phs = []
            t_ar    = []
            dtime   = str(datetime.datetime.now()).split('.')[0]
            for table_id in table_ids:
                t_ar.append({'t':table_id[1], 'l':len(table_ph_d[table_id].keys()), 'd':table_id[0], 'dt':doc_d.get(table_id[0], '')})
                all_phs = table_ph_d[table_id].keys()
                for k in table_ph_d[table_id].keys():
                    vs  = table_ph_d[table_id][k].keys()
                    ph_cnt_d    = {}
                    for (c, ph) in vs:
                        ph_cnt_d[ph]    = ph_cnt_d.get(ph, 0)+1
                    tphs    = ph_cnt_d.keys()
                    tphs.sort(key=lambda x:ph_cnt_d[x], reverse=True)
                    table_ph_d[table_id][k] = (vs[0][0], tphs[0])
                all_phs.sort(key=lambda x:table_ph_d[table_id][x][0])
                for ph, tlabel in all_phs:
                    tph = table_ph_d[table_id][(ph, tlabel)][1]
                    phs.append({'k':table_id[1]+'-'+ph+'-'+tlabel, 'n':tph, 'g':table_id[0]+'-'+table_id[1]+' ( '+'_'.join(doc_d.get(table_id[0], []))+' )', 'ph':ph})
            dd  = {'n':tmp_grp_name, 'data':f_ar, 'phs':phs, 'table_ar':t_ar, 'grpid':'_'.join(t_ids)+'-'+doc_grpid}
            g_i_ar.append(('HGHGROUP-'+i_table_type+'-'+'_'.join(t_ids)+'-'+doc_grpid, tmp_grp_name, ijson['user'], dtime))
            grp_ar.append(dd)
        tmpg_i_ar   = []
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'vgh_group_info'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = 1
            if r and r[0]:
                g_id    = int(r[0])+1
            
            sql = "select max(group_id) from vgh_group_info"
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id   = 1
            if r and r[0]:
                tg_id    = int(r[0])+1
            g_id     = max(g_id, tg_id)
            for ng in g_i_ar:
                tmpg_i_ar.append((g_id, )+ng)
                g_id    += 1
        i_ar    = []
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            for t_d in t_ars:
                for t_id, vd in t_d.items():
                    for tup in vd['d']:
                        tup = (g_id, vd['o'])+tuple(tup)
                        tup = tup[:4]+(vd['t_l'], )+tup[5:]
                        i_ar.append(tup)
                    g_id    += 1
        cols    = 'taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg'
        cols_v  =', '.join(map(lambda x:'?', cols.split(',')))
        if 1:#ijson.get('update_db', '') == 'Y':
            sql = "delete from mt_data_builder where table_type in (%s)"%(','.join(all_table_types.keys()))
            #print sql
            cur.execute(sql)
            sql = "delete from vgh_group_info where table_type in (%s)"%(','.join(all_table_types.keys()))
            #print sql
            cur.execute(sql)
            cur.executemany('insert into mt_data_builder(%s) values(%s)'%(cols, cols_v), i_ar)
            cur.executemany('insert into vgh_group_info(group_id, table_type, group_txt, user_name, datetime) values(?,?,?,?,?)', tmpg_i_ar)
            conn.commit()
        conn.close()
        res = [{'message':'done', 'data':grp_ar, 't_ids':ijson.get('t_ids', {})}]
        return res

    def form_HGH_group(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        i_table_type    = ijson['table_type']
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        start_year  = c_year - int(ijson.get('year', 10))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()


        table_type  = str(i_table_type)
        group_d     = {}
        revgroup_d  = {}

        d_group_d     = {}
        d_revgroup_d  = {}

        vgh_id_d    = {}
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        #sql         = "select vgh_id, doc_id, table_id, group_txt from doc_group_map where table_type='%s'"%(table_type)
        #try:
        #    cur.execute(sql)
        #    res = cur.fetchall()
        #except:
        #    res = []
        grp_doc_map_d   = {}
        rev_doc_map_d   = {}
        #for rr in res:
        #    vgh_id, doc_id, table_id, group_txt = rr
        #    grp_doc_map_d.setdefault(group_txt, {})[table_id]   = doc_id #.setdefault(doc_id, {})[table_id]    = 1
        #    rev_doc_map_d.setdefault((doc_id, table_id), {})[group_txt] = 1
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        if ijson.get('vids', {}) and isinstance(ijson.get('vids', {}), list):
            ijson['vids']   = ijson['vids'][0]
        else:
            ijson['vids']   = {}
        g_vids  = ijson.get('vids', {})
        t_ids   = map(lambda x:str(x), ijson.get('t_ids', []))
        self.alter_table_coldef(conn, cur, 'mt_data_builder', ['target_taxonomy', 'numeric_flg', 'th_flg'])
        if ijson.get('vids', {}):
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s' and taxo_id in (%s) and  isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(t_ids),  ', '.join(ijson['vids'].keys()))
        else:
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s' and taxo_id in (%s) and isvisible='Y'"%(table_type, ', '.join(t_ids))
        cur.execute(sql)
        res         = cur.fetchall()
        docinfo_d   = {}
        vgh_id_d_all    = {}
        tmp_grpd        = {}
        table_ids       = {}
        all_vgh         = {}
        f_d             = {}
        f_o_d          = {}
        for rr in res:
            row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg   = rr
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            c   = int(c_id.split('_')[2])
            if g_vids.get(vgh_text, {}) and table_id+'-'+str(c) not in g_vids.get(vgh_text, {}):continue
            rr  = (table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg)
            f_d.setdefault(taxo_id, {})[rr] = 1
            f_o_d[taxo_id]   = order_id
        tmpt_d          = {}
        all_table_types = {}
        for taxo_id, ks in f_d.items():
            t_ar    = []
            for rr in ks:
                rr      = list(rr)
                rr[0]   =  'HGHGROUP-'+i_table_type+'-'+'_'.join(t_ids)+'-HGH'
                all_table_types['"'+rr[0]+'"'] = {}
                t_ar.append(rr)
            tmpt_d[taxo_id]  = {'d':t_ar, 'o':f_o_d[taxo_id]}
        t_ars   = []
        t_ars.append(tmpt_d)    
        g_i_ar  = []
        tmp_grp_name    = ijson['grp_name']
        dtime   = str(datetime.datetime.now()).split('.')[0]
        g_i_ar.append(('HGHGROUP-'+i_table_type+'-'+'_'.join(t_ids)+'-HGH', tmp_grp_name, ijson['user'], dtime))
        tmpg_i_ar   = []
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'vgh_group_info'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = 1
            if r and r[0]:
                g_id    = int(r[0])+1
            
            sql = "select max(group_id) from vgh_group_info"
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id   = 1
            if r and r[0]:
                tg_id    = int(r[0])+1
            g_id     = max(g_id, tg_id)
            for ng in g_i_ar:
                tmpg_i_ar.append((g_id, )+ng)
                g_id    += 1
        i_ar    = []
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            for t_d in t_ars:
                for t_id, vd in t_d.items():
                    #print  t_id, vd
                    for tup in vd['d']:
                        #print '\n\tBB ',tup
                        tup = (g_id, vd['o'])+tuple(tup)
                        #print '\t',tup
                        i_ar.append(tup)
                    g_id    += 1
        cols    = 'taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg'
        cols_v  =', '.join(map(lambda x:'?', cols.split(',')))
        if 1:#ijson.get('update_db', '') == 'Y':
            sql = "delete from mt_data_builder where table_type in (%s)"%(','.join(all_table_types.keys()))
            #print sql
            cur.execute(sql)
            sql = "delete from vgh_group_info where table_type in (%s)"%(','.join(all_table_types.keys()))
            #print sql
            cur.execute(sql)
            #print 'insert into mt_data_builder(%s) values(%s)'%(cols, cols_v)
            #for rr in i_ar:
            #    print rr
            cur.executemany('insert into mt_data_builder(%s) values(%s)'%(cols, cols_v), i_ar)
            cur.executemany('insert into vgh_group_info(group_id, table_type, group_txt, user_name, datetime) values(?,?,?,?,?)', tmpg_i_ar)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def order_by_table_structure_by_col(self, f_taxo_arr, table_ids):
        tmptable_ids   = map(lambda x:x[1], table_ids)
        table_match_d   = {}
        for ti, dd in enumerate(f_taxo_arr):
            ks      = dd['ks']
            for table_id, c_id in ks:
                table_match_d.setdefault(table_id, {}).setdefault(ti, {})[c_id] = 1
        tabl_ids    = table_match_d.keys()
        tabl_ids.sort(key=lambda x:tmptable_ids.index(x))
        #tabl_ids.sort(key=lambda x:len(table_match_d[x].keys()), reverse=True)
        final_arr   = []
        for table_id in tabl_ids:
            inds    = table_match_d[table_id].keys()
            inds.sort()
            #print '\n==========================================================='
            #print table_id, sorted(inds)
            inds.sort(key=lambda x:int(table_match_d[table_id][x].keys()[0].split('_')[2]))
            #print 'Ordered ', inds
            if not final_arr:
                final_arr   = inds
            else:
                m_d = list(sets.Set(final_arr).intersection(sets.Set(inds)))
                deletion    = {}
                tmp_arr     = []
                ftmp_arr     = []
                for t in inds:
                    if t in m_d:
                        ftmp_arr    = []
                        if tmp_arr:
                            deletion[t] = copy.deepcopy(tmp_arr[:])
                            #tmp_arr = []    
                        continue
                    tmp_arr.append(t)
                    ftmp_arr.append(t)
                done_d  = {}
                m_d.sort(key=lambda x:final_arr.index(x))
                
                for t in m_d:
                    if t in deletion:
                        tmp_arr = []
                        for t1 in deletion[t]:
                            if t1 not in done_d:
                                tmp_arr.append(t1)
                                done_d[t1]  = 1
                        index       = final_arr.index(t)
                        final_arr   = final_arr[:index]+tmp_arr+final_arr[index:]    
                
                if ftmp_arr:
                    final_arr   = final_arr+ftmp_arr
            
            #print 'FINAL ', final_arr
        missing = sets.Set(range(len(f_taxo_arr))) - sets.Set(final_arr)
        if len(final_arr) > len(f_taxo_arr):
            print 'Duplicate ', final_arr
            sys.exit()
        if len(missing):
            print 'missing ', list(missing)
            sys.exit()
        f_taxo_arr  = map(lambda x:f_taxo_arr[x], final_arr)
        return f_taxo_arr

    def update_gcom_flg(self, ijson):
        #sys.exit()
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        conn, cur       = conn_obj.sqlite_connection(db_file)
        try:
            sql = "alter table mt_data_builder add column xml_id TEXT"
            cur.execute(sql)
        except:pass
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()
        if ijson.get('table_ids', []):
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder where table_id in (%s)"%(', '.join(map(lambda x:str(x), ijson.get('table_ids', []))))
        elif ijson.get('table_types', []):
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder where table_type in (%s)"%(', '.join(map(lambda x:'"'+str(x)+'"', ijson.get('table_types', []))))
        else:
            sql = "select row_id, c_id, xml_id, table_id, ph from mt_data_builder"
        cur.execute(sql)
        res = cur.fetchall()
        u_ar    = []
        for rr in res:
            row_id, c_id, xml_id, table_id, ph    = rr
            if table_id and xml_id:
                table_id = str(table_id)
                key     = table_id+'_'+self.get_quid(xml_id)
                gcom_f  = 'N'
                nggcom_f  = 'N'
                for rc in ['COL', 'ROW']:
                    ngcom        = txn.get('COM_NG_'+rc+'MAP_'+key)
                    if ngcom:
                        for eq in ngcom.split('|'):
                            nggcom_f    = 'Y'
                            break
                    
                    gcom        = txn.get('COM_G_'+rc+'MAP_'+key)
                    if gcom:
                        for eq in gcom.split('|'):
                            gcom_f  = 'Y'
                            break
                if gcom_f == 'Y' or nggcom_f == 'Y':
                    u_ar.append((nggcom_f, gcom_f, row_id))
        #for rr in u_ar:
        #    print rr
        #sys.exit()
        print 'Total ', len(u_ar)
        cur.executemany("update mt_data_builder set ngcom=?, gcom=? where row_id=?", u_ar)
        conn.commit()
        conn.close()

    ## 57 T / H Save for Excel View
    def save_header_resultant_taxo_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        taxo_ids        = ijson["t_ids"]
        th_flg          = ijson['key']
        grp_id          = ijson['grpid']
        user_name       = ijson['user']
        table_type      = ijson['table_type']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        create_stmt = "CREATE TABLE IF NOT EXISTS ph_derivation (row_id INTEGER PRIMARY KEY AUTOINCREMENT, formula VARCHAR(256), table_type VARCHAR(100), group_id VARCHAR(50), ph VARCHAR(10), formula_type VARCHAR(20), formula_str VARCHAR(256), user_name VARCHAR(100));"
        cur.execute(create_stmt)

        i_ar = []
        d_ar = []
        for tid in taxo_ids:
            i_ar.append((th_flg, table_type, grp_id, tid, 'TH', '', user_name))
            d_ar.append((table_type, grp_id, tid, 'TH'))

        cur.executemany('delete from ph_derivation where table_type = ? and group_id = ? and ph = ? and formula_type = ?', d_ar)

        cur.executemany('insert into ph_derivation (formula, table_type, group_id, ph, formula_type, formula_str, user_name) values (%s)'%(','.join(map(lambda x:'?', i_ar[0]))), i_ar)
        conn.commit()
        conn.close()
        return [{'message':'done'}]

    def validate_table_data(self, table_id, txn):
        lkey        = 'GV_'+table_id
        ids         = txn.get(lkey)
        if not ids:return 'GV NOT FOUND'
        r_d         = {}
        for c_id in ids.split('#'):
            t_id, r, c  = c_id.split('_')
            r               = int(r)
            c               = int(c)
            rs              = int(txn.get('rowspan_'+c_id))
            cs              = int(txn.get('colspan_'+c_id))
            r_d.setdefault(r, {})[c]    = (rs, cs)
            for ri in range(int(rs)):
                r_d.setdefault(r+ri, {})[c]   = (1, 1)
            #for ci in range(int(cs)):
            #    r_d[r][c+ci]   = (1, 1)
        rows    = r_d.keys()
        rows.sort()
        f_d = {}
        for r in rows:
            cols    = r_d[r].keys()
            cols.sort()
            #print r, cols
            f_d.setdefault(tuple(cols),{})[r]    = 1
        if len(f_d.keys()) > 1:
            lkey        = 'HGH_'+table_id
            ids         = txn.get(lkey)
            r_d         = {}
            for c_id in ids.split('#'):
                t_id, r, c  = c_id.split('_')
                r               = int(r)
                c               = int(c)
                text            = binascii.a2b_hex(txn.get('TEXT_'+c_id))
                if text:
                    r_d.setdefault(r, []).append(text)
            tmp_arr = []
            for k, v in f_d.items():
                rows    = v.keys()
                rows.sort()
                txt = ''
                tmpr    = rows[0]
                for r in rows:
                    if r in r_d:
                        txt = r_d[r]
                        tmpr    = r
                        break 
                tmp_arr.append((tmpr, txt, k))
            return sorted(tmp_arr)
        return True

    def validate_normalization(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        #start_year  = c_year - int(ijson['year'])
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            try:
                year    = int(ph[-4:])
            except:continue
            if ph and start_year < year:
                doc_id  = line[0]
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):continue
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        table_type    = ijson.get('table_type', '')
        if table_type:
            m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [table_type])
        else:
            m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        if table_type:
            sql         = "select table_id, doc_id,xml_id from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
        else:
            sql         = "select table_id, doc_id,xml_id from mt_data_builder where isvisible='Y'"
        cur.execute(sql)
        res = cur.fetchall()
        table_ids   = {}
        for rr in res:
            table_id, doc_id,xml_id = rr
            doc_id      = str(doc_id)
            if doc_id not in doc_d:continue
            table_id    = str(table_id)
            if table_id not in m_tables:continue
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            table_ids[(doc_id, table_id)]   = 1
        error_tables    = []
        done_tables     = []
        for doc_id, table_id in table_ids.keys():
            is_valid    = self.validate_table_data(table_id, txn_m)
            if is_valid == True:
                dd  = {'d':doc_id, 't':table_id, 'n':doc_id+'-'+str(table_id)+'_'.join(doc_d[doc_id]), 'error':is_valid}
                done_tables.append(dd)
                continue
            dd  = {'d':doc_id, 't':table_id, 'n':doc_id+'-'+str(table_id)+'_'.join(doc_d[doc_id]), 'error':is_valid}
            error_tables.append(dd)
        res = [{'message':'done','data':error_tables, 'done_tables':len(done_tables), 't':len( table_ids.keys())}]
        return res
            

    ## SFORMULA DATA
    def get_sFormula_data(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        group_id        = ijson["grpid"]
        table_type      = ijson['table_type']

        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
 
        if group_id: 
            sql         = 'select row_id, formula from ph_derivation where table_type="%s" and formula_type="SFORMULA" and group_id="%s"'%(table_type, group_id)
        elif not group_id:
            sql         = 'select row_id, formula, group_id from ph_derivation where table_type="%s" and formula_type="SFORMULA"'%(table_type)
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()
        formula_d   = []
        for rr in res:
            if group_id:
                row_id, formula = map(str, rr)
            elif not group_id:
                row_id, formula, gid = map(str, rr) 
                if gid:
                    continue
            operands    = []
            for topr in formula.split('$$'):
                tid, operator, t_type, g_id, ftype = topr.split('@@')
                tmp = {'tid':tid, 's':operator, 'R':'N'}
                if operator == '=':
                    tmp['rid']  = row_id
                    tmp['R']    = 'Y'
                operands.append(tmp)
            formula_d.append(operands)
        return formula_d

    def print_table_types(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        ijson.update({"eq":"N","O":"Y"})
        m_tables, rev_m_tables, doc_m_d, table_type_m = self.get_main_table_info(company_name, model_number, ijson.get('table_types', []))
        t_types = {}
        for tid in ijson['table_ids']:
            t_types[m_tables[str(tid)]] = 1
        print t_types.keys()
           
    def get_sheet_id_map(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql   = "select sheet_id, node_name from node_mapping where review_flg = 0"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        #print rr, len(tres)
        ddict = dd(set)
        for tr in tres:
            sheet_id, node_name = map(str, tr)
            ddict[sheet_id] = node_name
        return ddict

    def clean_actual_value_tableid_data(self, conn, cur):
        crt_qry = "CREATE TABLE IF NOT EXISTS clean_actual_value_status(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(100), doc_id VARCHAR(100), table_type VARCHAR(100), value_status VARCHAR(20));"
        cur.execute(crt_qry)
        
        read_qry = 'select table_id, table_type from clean_actual_value_status;'
        cur.execute(read_qry)   
        actual_data = [tuple(map(str, tup))for tup in cur.fetchall()]
        return actual_data
    ## 61 Classification Data
    def get_classified_tables_info(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)

        table_type      = ijson['table_type']
        sheet_id_map    = self.get_sheet_id_map()

        db_file     = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn, cur   = conn_obj.sqlite_connection(db_file)

        sql   = "select sheet_id, doc_id, doc_name, table_id from table_group_mapping;"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        dbf = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn1 = sqlite3.connect(dbf)
        cur1 = conn1.cursor()
        actual_tables = self.clean_actual_value_tableid_data(conn1, cur1) 
        conn1.commit()
        conn1.close()
        conn.close()
        data_rows = []
        for row in tres:
            row = map(str, row)
            sheet_id, doc_id, doc_name, table_id_str = row
            table_id_li = table_id_str.split('^!!^')
            for tid in table_id_li:
                if not tid.strip():continue
                tup = tuple([tid, sheet_id_map.get(sheet_id, '')])
                ac = 'N'
                if tup in actual_tables:
                    ac = 'Y'
                data_rows.append({'t':tid, 'd':doc_id, 'tt':sheet_id_map.get(sheet_id, ''), 'dn':doc_name, 'act_f':ac})
        tt_group_dict = self.get_tt_sub_groups_info(ijson)  
        companyConfig_data = self.read_companyConfigTable(ijson)
        formula_lst = self.show_all_table_types_and_formulas_company()
        return [{'message':'done', 'data':data_rows, 'tt_data':tt_group_dict, 'cf_data':companyConfig_data, 'fl':formula_lst}]
        #return [{'message':'done', 'data':data_rows, 'tt_data':tt_group_dict, 'cf_data':companyConfig_data}]

    
    def gen_DB_data_builder(self, ijson):
        if ijson['user'].strip() not in self.gen_users:
            res = [{'message':'Error : Permission Denied for '+ijson['user']}]
            return res
            
        import create_table_seq
        obj = create_table_seq.TableSeq()
        disableprint()
        ijson['re_run'] = 'Y'
        ijson['Taxo'] = 'Y'
        if ijson.get('ph', '') == 'Y':
            ijson['print'] = 'N'
            self.run_ph_csv(copy.deepcopy(ijson))
        if ijson.get('table_types', []):
            obj.index_seq_across_mt_taxo(ijson)
        enableprint()
        res = [{'message':'done'}]
        return res

    def add_new_table_DB(self, ijson):
        if ijson['user'].strip() not in self.gen_users:
            res = [{'message':'Error : Permission Denied for '+ijson['user']}]
            return res
        disableprint()
        ijson['Taxo'] = 'Y'
        ijson['re_run'] = 'Y'
        if ijson.get('ph', '') == 'Y':
            ijson['print'] = 'N'
            self.run_ph_csv(copy.deepcopy(ijson))
        self.add_new_table(ijson)
        enableprint()
        res = [{'message':'done'}]
        return res

    def update_ph_DB(self, ijson):
        if ijson['user'].strip() not in self.gen_users:
            res = [{'message':'Error : Permission Denied for '+ijson['user']}]
            return res
        disableprint()
        ijson['re_run'] = 'Y'
        ijson['PH']     = 'Y'
        ijson['print']  = 'N'
        self.run_ph_csv(ijson)
        enableprint()
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        import model_view.table_tagging_v2 as TableTagging
        Omobj = TableTagging.TableTagging()
        table_id_cell_dict = Omobj.all_user_selected_references(company_name, model_number, company_id)
        d_ar            = []
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        for table_id, xmls in table_id_cell_dict.items():
            table_id    = str(table_id)
            ignore_col  = {}
            k   = 'GV_'+table_id
            ids = txn_m.get(k)
            if not ids:continue
            ids         = ids.split('#')
            col_d   = {}
            for c_id in ids:
                r       = int(c_id.split('_')[1])
                c       = int(c_id.split('_')[2])
                x       = txn_m.get('XMLID_'+str(c_id))
                col_d.setdefault(c, {})[x]  = 1
            for xml_id in xmls:
                xml_id  = '#'.join(filter(lambda x:x, xml_id.split('#')))
                tk      = self.get_quid(table_id+'_'+xml_id)
                c_id    = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                c       = int(c_id.split('_')[2])
                cs      = int(txn_m.get('colspan_'+c_id))
                for c_s in range(cs): 
                    for x in col_d.get(c+c_s, {}).keys():
                        d_ar.append((table_id, x))
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        cur.executemany("update mt_data_builder set isvisible='N' where table_id=? and xml_id=?", d_ar)  
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def gen_taxo_grp(self, ijson):
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        taxo_group_obj.create_taxo_grp(ijson)
    def gen_sh_taxo_grp(self, ijson):
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        if ijson.get('PRINT', '') != 'Y':
            disableprint()
        res = taxo_group_obj.match_SH_group(ijson)
        enableprint()
        return res


    def validate_ph_csv(self, ijson):
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        taxo_group_obj.validate_ph_csv(ijson)

    def gen_taxo_grp_intf(self, ijson):
        if ijson['user'].strip() not in self.gen_users:
            res = [{'message':'Error : Permission Denied for '+ijson['user']}]
            return res
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        disableprint()
        ijson['re_run'] = 'Y'
        ijson['Taxo'] = 'Y'
        if ijson.get('ph', '') == 'Y':
            ijson['print'] = 'N'
            self.run_ph_csv(copy.deepcopy(ijson))
        if ijson.get('table_types', []):
            taxo_group_obj.create_taxo_grp(ijson)
        elif ijson.get('table_ids', []):
            taxo_group_obj.create_taxo_grp(ijson)
        enableprint()
        res = [{'message':'done'}]
        return res

    def gen_taxo_grp_ph(self, ijson):
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        ijson['lgroup'] = 'Y'
        ijson['taxo_flg'] = 1
        ijson['table_types'] = [ijson['table_type']]
        disableprint()
        taxo_group_obj.create_taxo_grp(ijson)
        enableprint()
        res = self.create_seq_across(ijson)
        return res

    def gen_taxo_grp_auto_reorder(self, ijson):
        import data_builder.taxo_group as taxo_group
        taxo_group_obj  = taxo_group.TaxoGroup()
        ijson["lgroup_auto_order"] = 'Y'
        ijson['taxo_flg'] = 1
        ijson['table_types'] = [ijson['table_type']]
        disableprint()
        taxo_group_obj.create_taxo_grp(ijson)
        enableprint()
        res = self.create_seq_across(ijson)
        return res

    def restore_flip_table(self, ijson):
        company_name = ijson["company_name"]
        project_id  = ijson["project_id"]
        deal_id     = ijson["deal_id"]
        model_number= ijson["model_number"]
        machine_id  = '122'
        company_id  = '%s_%s'%(project_id, deal_id)
        table_ids   = ijson['table_ids']
        if not table_ids:
            return [{'message':'Error empty table list'}]
        disableprint()
        import index_all_table
        i_obj = index_all_table.Index()
        notm_d  = i_obj.index_table_norm_info(company_id, list(sets.Set(table_ids)), 'N')
        enableprint()
        for tid in table_ids:
            org_path    = '%s/%s/Table_Htmls/%s_org.html'%(self.output_path, company_id, tid)
            cpath       = '%s/%s/Table_Htmls/%s.html'%(self.output_path, company_id, tid)
            if os.path.exists(org_path):
                os.system("cp %s %s"%(org_path, cpath))
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'N')
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'N')
        ijson['Taxo'] = 'Y'
        ijson['print'] = 'N'
        ijson['re_run'] = 'Y'
        disableprint()
        self.run_ph_csv(copy.deepcopy(ijson))
        enableprint()
        if ijson.get('taxo_flg', '') == 1:
            import data_builder.taxo_group as taxo_group
            taxo_group_obj  = taxo_group.TaxoGroup()
            disableprint()
            #ijson['re_run'] = 'Y'
            if ijson.get('table_types', []):
                taxo_group_obj.create_taxo_grp(ijson)
            elif ijson.get('table_ids', []):
                taxo_group_obj.create_taxo_grp(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res
        else:
            disableprint()
            ijson['Taxo'] = 'Y'
            self.add_new_table(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res

    def flip_table(self, ijson):
        company_name = ijson["company_name"]
        project_id  = ijson["project_id"]
        deal_id     = ijson["deal_id"]
        model_number= ijson["model_number"]
        machine_id  = '122'
        company_id  = '%s_%s'%(project_id, deal_id)
        table_ids   = ijson['table_ids']
        if not table_ids:
            return [{'message':'Error empty table list'}]
        disableprint()
        import index_all_table
        i_obj = index_all_table.Index()
        notm_d  = i_obj.index_table_norm_info(company_id, list(sets.Set(table_ids)), 'Y')
        enableprint()
        for tid in table_ids:
            npath  = "/var/www/html/muthu/Transpose/%s/%s.html"%(company_id, tid)
            if os.path.exists(npath):
                org_path    = '%s/%s/Table_Htmls/%s_org.html'%(self.output_path, company_id, tid)
                cpath       = '%s/%s/Table_Htmls/%s.html'%(self.output_path, company_id, tid)
                if not os.path.exists(org_path):
                    os.system("cp %s %s"%(cpath, org_path))
                os.system("cp %s %s"%(npath, cpath))
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'Y')
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'Y')
        #import model_view.gen_uform_utag_triplet_data as gen_uform_utag_triplet_data
        #tri_obj = gen_uform_utag_triplet_data.Comp_Tag_Triplet_data()
        #i_ar    = []
        #for gtable_id, normalized_data in notm_d.items():
        #    i_ar.append()
        #    #print 
        #    #for ndata in normalized_data:
        #    #    for rr in ndata:
        #    #        print '\t', rr
        #    tri_obj.generate_default_triplet(company_name, model_number, company_id, machine_id, gtable_id, normalized_data)
        ijson['Taxo'] = 'Y'
        ijson['print'] = 'N'
        ijson['re_run'] = 'Y'
        disableprint()
        self.run_ph_csv(copy.deepcopy(ijson))
        enableprint()
        if ijson.get('taxo_flg', '') == 1:
            import data_builder.taxo_group as taxo_group
            taxo_group_obj  = taxo_group.TaxoGroup()
            disableprint()
            #ijson['re_run'] = 'Y'
            if ijson.get('table_types', []):
                taxo_group_obj.create_taxo_grp(ijson)
            elif ijson.get('table_ids', []):
                taxo_group_obj.create_taxo_grp(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res
        else:
            disableprint()
            ijson['Taxo'] = 'Y'
            self.add_new_table(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res

    def restore_merge_table(self, ijson):
        company_name = ijson["company_name"]
        project_id  = ijson["project_id"]
        deal_id     = ijson["deal_id"]
        model_number= ijson["model_number"]
        machine_id  = '122'
        company_id  = '%s_%s'%(project_id, deal_id)
        table_ids   = ijson['table_ids']
        if not table_ids:
            return [{'message':'Error empty table list'}]
        disableprint()
        import index_all_table
        i_obj = index_all_table.Index()
        notm_d  = i_obj.index_table_norm_info(company_id, list(sets.Set(table_ids)), 'N')
        enableprint()
        for tid in table_ids:
            org_path    = '%s/%s/Table_Htmls/%s_org.html'%(self.output_path, company_id, tid)
            cpath       = '%s/%s/Table_Htmls/%s.html'%(self.output_path, company_id, tid)
            if os.path.exists(org_path):
                os.system("cp %s %s"%(org_path, cpath))
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'N')
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'N')
        ijson['Taxo'] = 'Y'
        ijson['print'] = 'N'
        ijson['re_run'] = 'Y'
        disableprint()
        self.run_ph_csv(copy.deepcopy(ijson))
        enableprint()
        if ijson.get('taxo_flg', '') == 1:
            import data_builder.taxo_group as taxo_group
            taxo_group_obj  = taxo_group.TaxoGroup()
            disableprint()
            #ijson['re_run'] = 'Y'
            if ijson.get('table_types', []):
                taxo_group_obj.create_taxo_grp(ijson)
            elif ijson.get('table_ids', []):
                taxo_group_obj.create_taxo_grp(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res
        else:
            disableprint()
            ijson['Taxo'] = 'Y'
            self.add_new_table(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res

    def merge_table(self, ijson):
        company_name = ijson["company_name"]
        project_id  = ijson["project_id"]
        deal_id     = ijson["deal_id"]
        model_number= ijson["model_number"]
        machine_id  = '122'
        company_id  = '%s_%s'%(project_id, deal_id)
        table_ids   = ijson['table_ids']
        if not table_ids:
            return [{'message':'Error empty table list'}]
        disableprint()
        import index_all_table
        i_obj = index_all_table.Index()
        notm_d  = i_obj.index_table_norm_info(company_id, list(sets.Set(table_ids)), 'M')
        enableprint()
        for tid in table_ids:
            npath  = "/var/www/html/muthu/Transpose/%s/%s_merge.html"%(company_id, tid)
            if os.path.exists(npath):
                org_path    = '%s/%s/Table_Htmls/%s_org.html'%(company_id, tid)
                cpath       = '%s/%s/Table_Htmls/%s.html'%(self.output_path, company_id, tid)
                if not os.path.exists(org_path):
                    os.system("cp %s %s"%(cpath, org_path))
                os.system("cp %s %s"%(npath, cpath))
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'M')
        lmdb_path2      =  "/mnt/eMB_db/%s/%s/default_table_phcsv_data"%(company_name, model_number)
        env1             = lmdb.open(lmdb_path2, map_size=2**39)
        with env1.begin(write=True) as txn:
            for tid in table_ids:
                txn.put('FLIP_'+str(tid), 'M')
        #import model_view.gen_uform_utag_triplet_data as gen_uform_utag_triplet_data
        #tri_obj = gen_uform_utag_triplet_data.Comp_Tag_Triplet_data()
        #i_ar    = []
        #for gtable_id, normalized_data in notm_d.items():
        #    i_ar.append()
        #    #print 
        #    #for ndata in normalized_data:
        #    #    for rr in ndata:
        #    #        print '\t', rr
        #    tri_obj.generate_default_triplet(company_name, model_number, company_id, machine_id, gtable_id, normalized_data)
        ijson['Taxo'] = 'Y'
        ijson['print'] = 'N'
        ijson['re_run'] = 'Y'
        disableprint()
        self.run_ph_csv(copy.deepcopy(ijson))
        enableprint()
        if ijson.get('taxo_flg', '') == 1:
            import data_builder.taxo_group as taxo_group
            taxo_group_obj  = taxo_group.TaxoGroup()
            disableprint()
            #ijson['re_run'] = 'Y'
            if ijson.get('table_types', []):
                taxo_group_obj.create_taxo_grp(ijson)
            elif ijson.get('table_ids', []):
                taxo_group_obj.create_taxo_grp(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res
        else:
            disableprint()
            ijson['Taxo'] = 'Y'
            self.add_new_table(ijson)
            enableprint()
            res = [{'message':'done'}]
            return res
            


    def taxoDropDown(self, ijson):
        table_type = ijson["table_type"]
        db_path = '/mnt/eMB_db/TAXO_INFO/taxo_info.db'
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        qry = 'select distinct(taxonomy) from taxo_label_map where table_type="%s"'%(table_type) 
        cur.execute(qry)
        data = cur.fetchall()
        res_lst = []
        for row in data:
            taxonomy = str(row[0])
            rs = {'n':taxonomy} 
            res_lst.append(rs)
        conn.close()
        res = [{'message':'done', 'data':res_lst}]
        return res

    def taxoDropDown_cp(self, ijson):
        table_type = ijson["table_type"]
        sys.path.append('/root/databuilder_train_ui/tenkTraining/Working_Table_Tagging_Training_V2/pysrc/model_view/') 
        import company_docTablePh_details_tableType as py
        obj  = py.Company_docTablePh_details()
        company_name = ijson["company_name"]
        project_id  = ijson["project_id"]
        deal_id     = ijson["deal_id"]
        company_id  = '%s_%s'%(project_id, deal_id)

        try:
            tables = obj.getTableIdLst_passingTableType(company_id)[table_type]
        except:
            tables  = []
            pass
        #################################
        lmdb_path = os.path.join("/var/www/html/fill_table/", company_id , "table_info")
        env = lmdb.open(lmdb_path)
        txn  = env.begin()
        #################################
        res_lst = []
        for table_id in tables:
            getCids = txn.get('HGH_'+table_id)
            if getCids == None:continue 
            for cids in getCids.split('#'):
                getTxtHex = txn.get('TEXT_' + cids)
                unhashedTx = self.gen_taxonomy(binascii.a2b_hex(getTxtHex))
                tx = {'n':unhashedTx}
                if tx not in res_lst:
                    res_lst.append(tx) 
        res = [{'message':'done', 'data':res_lst, 'cnt':len(res_lst)}] 
        return res

    def alter_column_datatype(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        db_file         = self.get_db_path(ijson)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        alter_stmt = 'ALTER TABLE ph_derivation ADD ph TEXT;'
        try:
            cur.execute(alter_stmt)
            conn.commit()
        except:pass
        conn.close()
        return [{'message':'done'}]

    
    def get_selected_class_data_by_key(self, ijson):
        company_name      = ijson["company_name"]
        model_number      = ijson["model_number"]
        project_id        = ijson["project_id"]
        deal_id           = ijson["deal_id"]
        company_id        = '%s_%s'%(project_id, deal_id)
        given_table_id    = ijson["t"]
 
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'tas_company.db')
        conn        = sqlite3.connect(db_file)
        cur         = conn.cursor()
        stmt = "select doc_id, period, reporting_year from company_meta_info" 
        cur.execute(stmt)
        res = cur.fetchall()
        conn.close()
        doc_pdict = {}
        for r in res:
            doc_id, period_type, period = map(str, r[:])
            doc_pdict[doc_id] = (period_type, period)
        ################################################
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'tas_tagging.db')
        conn        = sqlite3.connect(db_file)
        cur         = conn.cursor()
        
        crt_qry     = 'CREATE TABLE IF NOT EXISTS UsrTableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(256), xml_id TEXT, period_type TEXT, period TEXT, currency TEXT, value_type TEXT, scale TEXT, month VARCHAR(20), review_flg INTEGER)'
        cur.execute(crt_qry)

        stmt = "select table_id, xml_id, period, period_type, month, currency, scale, value_type from UsrTableCsvPhInfo where table_id='%s'" %(given_table_id)
        cur.execute(stmt)
        res = cur.fetchall()
        conn.close()
        user_sel_dict = {}
        for r in res:
            table_id, xml_id, period, period_type, month, currency, scale, value_type = map(lambda x:str(x).strip(), r[:]) 
            if table_id not in user_sel_dict:
                user_sel_dict[table_id] = {}
            user_sel_dict[table_id][xml_id] = [period, period_type, month, currency, scale, value_type]
        #####################################################
        fname = os.path.join('/mnt/eMB_db/', 'csv_classes', company_id)
        env = lmdb.open(fname)
        output_lst = []
        sl = 1
        other_table_ids = []
        doc_table_group_data = {}
        given_group_dict = {}
        with env.begin() as txn:
            cursor = txn.cursor()
            for key, vstr in cursor:
                if 'CLS_CNT' in key:continue
                if 'CLS:' in key:continue
                if 'DPC' in key:continue
                col_key = 'yellow'
                if 'RED' in key:
                    col_key = 'red'
                elif 'GREEN' in key:
                    col_key = 'green'
                kstr, prop_lst = eval(vstr)
                doc_id, table_id, xml_id = kstr.split('$::$')
                if table_id != given_table_id:continue
                txt, period, period_type, month, currency, scale, value_type = prop_lst
                doc_key = table_id
                if doc_key not in doc_table_group_data:
                    doc_table_group_data[doc_key] = {}
                if col_key not in doc_table_group_data[doc_key]:
                    doc_table_group_data[doc_key][col_key] = []
                if xml_id not in doc_table_group_data[doc_key][col_key]:
                    doc_table_group_data[doc_key][col_key].append(xml_id)
                if 1:#cls in key:
                    if doc_key not in given_group_dict:
                        given_group_dict[doc_key] = {}
                    doc_period_type, doc_period = doc_pdict.get(doc_id, ('', ''))
                    doc_p = doc_period_type + doc_period
                    given_group_dict[doc_key][xml_id] = {'l':txt, 'p':period, 'pt':period_type, 'm':month, 'c':currency, 's':scale, 'vt':value_type, 'd_pt_p':doc_p, 'x':xml_id, 'd':doc_id, 't':table_id}
        ########################################################### 
        results_dict = {}
        lmdb_path = os.path.join(self.output_path, company_id, 'xml_bbox_map')
        env       = lmdb.open(lmdb_path)
        txn       = env.begin() 
        res_str = txn.get('RST:'+str(given_table_id), '')
        if res_str:
            results_dict = ast.literal_eval(res_str)
        ###########################################################
        output_lst = []
        sl = 1
        apply_cell_dict = {} 
        for table_id, xdict in given_group_dict.items():
            ucell_dict = user_sel_dict.get(table_id, {})
            for xml_id, dd  in xdict.items():
                dd['sn'] = sl
                if xml_id in ucell_dict:
                    dd['p'], dd['pt'], dd['m'], dd['c'], dd['s'], dd['vt'] = ucell_dict[xml_id]
                    dd['df'] = 1
                    apply_cell_dict[xml_id] = dd['df']
                else:
                    dd['df'] = 0
                bbox_lst = []
                for xm in dd['x'].split('#'):
                    bbox = results_dict.get(xm, [])
                    if bbox == []:
                        bbox_lst.append(bbox)
                        continue
                    bbox_lst.append(bbox[0][0])
                dd['bbox'] = bbox_lst 
                output_lst.append(dd)
                sl += 1
        ########################################################### 
        #return 'done', output_lst, doc_table_group_data
        res = [{'message':'done', 'data':output_lst, 't_xdict':doc_table_group_data, 'cdict':apply_cell_dict}]
        return res

    def validate_taxo_ph_scv_info(self, company_name, model_number, company_id, txn_m, txn, table_ids, taxo_dict):
        t_rc_d  = {}
        import model_view.table_tagging_v2 as TableTagging
        Omobj = TableTagging.TableTagging()
        table_id_cell_dict = Omobj.all_user_selected_references(company_name, model_number, company_id)

        ignore_col      = {}
        for table_id, xmls in table_id_cell_dict.items():
            table_id    = str(table_id)
            for xml_id in xmls:
                xml_id  = '#'.join(filter(lambda x:x, xml_id.split('#')))
                #print (table_id, xml_id)
                tk      = self.get_quid(table_id+'_'+xml_id)
                c_id    = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                c       = int(c_id.split('_')[2])
                cs      = int(txn_m.get('colspan_'+c_id))
                for c_s in range(cs):
                    ignore_col.setdefault(table_id, {})[c]  = 1

        for table_id in table_ids.keys():
            k        = 'HGH_'+table_id
            ids      = txn_m.get(k)
            r_ld     = {}
            if ids:
                ids     = ids.split('#')
                for c_id in ids:
                    r       = int(c_id.split('_')[1])
                    c       = int(c_id.split('_')[2])
                    x       = txn_m.get('XMLID_'+c_id)
                    rs      = int(txn_m.get('rowspan_'+c_id))
                    for tr in range(rs): 
                        r_ld.setdefault(r+tr, {})[x]  = 1
            k   = 'GV_'+table_id
            ids = txn_m.get(k)
            if not ids:continue
            ids         = ids.split('#')
            for c_id in ids:
                r       = int(c_id.split('_')[1])
                c       = int(c_id.split('_')[2])
                if ignore_col.get(table_id, {}).get(c, 0) == 1:
                    continue
                x       = txn_m.get('XMLID_'+c_id)
                col     = 0
                if c_id:
                    col     = int(c_id.split('_')[2])
                key     = table_id+'_'+self.get_quid(x)
                x_txts  = r_ld.get(r, {}).keys()
                for x_p in x_txts:
                    t_rc_d.setdefault((table_id, x_p), {})[(col, x)]   = 1
        error_map_dict = {}
        table_gv_info  = {}
        for tid, ks_dict in taxo_dict.iteritems():
            taxo_map_ar = ks_dict['ks']
            matcher = {}
            max_group_len = 0
            max_key = ''
            for idx, tmap in enumerate(taxo_map_ar):
                set_val_type_map = []
                if str(tmap[2]) == 'Parent':continue
                #print '>>>> AA', tmap
                #print tmap[3]
                hgh_xml = txn_m.get('XMLID_'+str(tmap[1]))
                k = (tmap[0], hgh_xml)

                for (col, xml_id) in sorted(t_rc_d.get(k, {}).keys()):
                    key     = tmap[0]+'_'+self.get_quid(xml_id)
                    ph_map  = txn.get('PH_MAP_'+str(key))
                    if ph_map:
                        tperiod_type, tperiod, tcurrency, tscale, tvalue_type    = ph_map.split('^')
                    else:
                        tperiod_type, tperiod, tcurrency, tscale, tvalue_type   = '', '', '', '', ''
                    #print [tmap[3], tmap[0], '==', hgh_xml, '===', xml_id, '==', tvalue_type], '==', set_val_type_map
                    set_val_type_map.append(tvalue_type)
                k = '^!!^'.join([tmap[0], hgh_xml])
                matcher[k] = set_val_type_map
                if len(set(set_val_type_map)) > max_group_len:
                    max_group_len = len(set(set_val_type_map))
                    max_key = k
            if matcher:
                max_group_data = matcher.get(max_key, [])
                for k, sub_group in matcher.iteritems():
                    t, x = k.split('^!!^')
                    table_gv_info[(t, x)] = sub_group
                    if k == max_key:continue
                    if not (set(sub_group).issubset(set(max_group_data))):
                        error_map_dict[(t, x)] = 1
            
        #print error_map_dict
        #print table_gv_info
        return error_map_dict, table_gv_info

    def save_phcsv_data(self, ijson):
        company_name      = ijson["company_name"]
        model_number      = ijson["model_number"]
        project_id        = ijson["project_id"]
        deal_id           = ijson["deal_id"]
        company_id        = '%s_%s'%(project_id, deal_id)
       
        ph_comp_flg = int(ijson.get('ph_comp', '0'))
        self.ph_comp_flg = ph_comp_flg
 
        row = ijson['row']
        rc = ijson.get('rc', [])
        xml_list = ijson.get('xml_list', [])
        sh_list = ijson.get('sh_list', [])

        if not sh_list: 
            msg, apply_cell_dict = self.update_user_csv_row(company_name, model_number, row, rc, xml_list)
        else:
            msg, apply_cell_dict = self.update_user_csv_row_by_sel(company_name, model_number, row, rc, xml_list, sh_list)
        for x, x1 in apply_cell_dict.items():
            if int(x1) == 0:
                apply_cell_dict[x] = 2
            else:
                apply_cell_dict[x] = 1
        return json.dumps([{'message':msg, 'cdict':apply_cell_dict}])


    def validate_cell_ph_info(self, ttup):
        period_type, period, currency, value_type, scale, month = map(lambda x:str(x).strip(), ttup[:])
        vflg = 0
        if self.ph_comp_flg:
            if period_type and period:
                vflg = 1
            return vflg 
   
        if value_type == 'MNUM':
            if period_type and period and currency and scale:
                vflg = 1
        elif value_type == 'Percentage':
            if scale == '1' and period and period_type and (not currency):
                vflg = 1
        elif value_type == 'BNUM':
            if period_type and period and (not currency) and scale:
                vflg = 1
        elif value_type == 'Ratio':
            if (not currency) and scale == '1' and period and period_type:
                vflg = 1
        elif value_type == 'Other':
            if (not currency) and period and period_type:
                vflg = 1    
        return vflg

    def update_user_csv_row(self, company_name, model_number, ijson, rc, xml_list):
        xml_list = map(lambda x:str(x), xml_list[:])
        table_id, doc_id, xml_id = map(str, [ijson['t'], ijson['d'], ijson['x']])
        period_type, period, currency, scale, value_type, month = map(str, [ijson['pt'], ijson['p'], ijson['c'], ijson['s'], ijson['vt'], ijson['m']])
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'tas_tagging.db')
        #conn1, cur1   = self.get_connection(db_file)
        #column_tup = self.create_user_table_schama(conn1, cur1)
        conn1     = sqlite3.connect(db_file)
        cur1      = conn1.cursor()

        crt_qry     = 'CREATE TABLE IF NOT EXISTS UsrTableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(256), xml_id TEXT, period_type TEXT, period TEXT, currency TEXT, value_type TEXT, scale TEXT, month VARCHAR(20), review_flg INTEGER)'
        cur1.execute(crt_qry)

        data = []
        apply_xml_ids = []  
        apply_cell_dict = {}
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'table_tagging_dbs', '%s.db'%table_id)
        #conn, cur   = self.get_connection(db_file)
        conn  = sqlite3.connect(db_file)
        cur   = conn.cursor()

        review_flg = 1
        if rc:
            apply_xml_ids = []
            if len(rc) == 2:
                apply_xml_ids = xml_list
            else:
                stmt = "select nrow, ncol from TableCsvPhInfo where xml_id='%s'" %(xml_id)
                cur.execute(stmt)
                res = cur.fetchone()
                sel_row, sel_col = res[0], res[1]
                for rr in rc:
                    if rr == 'ROW':
                        stmt = "select distinct(xml_id) from TableCsvPhInfo where cell_type='GV' and nrow='%s'" %(sel_row)
                    elif rr == 'COL':
                        stmt = "select distinct(xml_id) from TableCsvPhInfo where cell_type='GV' and ncol='%s'" %(sel_col)
                    cur.execute(stmt)
                    apply_xml_ids += map(lambda x:str(x[0]), cur.fetchall())    
            ################################################################
            apply_xml_ids = list(set(apply_xml_ids))
            rem_xml_ids = []
            for new_xml_id in apply_xml_ids:
                if new_xml_id in xml_list:
                    ttup = (period_type, period, currency, value_type, scale, month)
                    review_flg = self.validate_cell_ph_info(ttup)
                    apply_cell_dict[new_xml_id] = review_flg
                    data.append((table_id, new_xml_id, period_type, period, currency, value_type, scale, month, review_flg))
                else:
                    rem_xml_ids.append(new_xml_id)
            if xml_list:
                apply_xml_ids = list(set(apply_xml_ids) - set(rem_xml_ids))
            else:
                apply_xml_ids = list(set(apply_xml_ids))
            ################################################################   
        else:
            apply_xml_ids = [xml_id]
            ttup = (period_type, period, currency, value_type, scale, month)
            review_flg = self.validate_cell_ph_info(ttup)
            apply_cell_dict[xml_id] = review_flg
            data = [(table_id, xml_id, period_type, period, currency, value_type, scale, month, review_flg)]
        ####### ADD TO USER DATA #################    
        app_xml_str = ', '.join(['"'+e+'"' for e in apply_xml_ids]) 
        table_name = 'UsrTableCsvPhInfo'
        stmt = "delete from UsrTableCsvPhInfo where xml_id in (%s) and table_id='%s'" %(app_xml_str, table_id)
        cur1.execute(stmt)
        #msg = self.qObj.insertIntoLite(conn1, cur1, '', table_name, column_tup, data) 
        cur1.executemany('INSERT INTO UsrTableCsvPhInfo(table_id, xml_id, period_type, period, currency, value_type, scale, month, review_flg) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', data)
        conn1.commit()
        conn1.close()
        stmt = "update TableCsvPhInfo set period_type='%s', period='%s', currency='%s', value_type='%s', scale='%s', usr_value_type='%s' where xml_id in (%s)" %(period_type, period, currency, value_type, scale, month, app_xml_str) 
        cur.execute(stmt)
        conn.commit()
        conn.close()       
        return 'done', apply_cell_dict

    def get_connection(self, db_file):
        conn = sqlite3.connect(db_file)
        cur  = conn.cursor()
        return conn, cur

    def update_user_csv_row_by_sel(self, company_name, model_number, ijson, rc, xml_list, sh_list):
        xml_list = map(lambda x:str(x), xml_list[:])
        table_id, doc_id, xml_id = map(str, [ijson['t'], ijson['d'], ijson['x']])
        period_type, period, currency, scale, value_type, month = map(str, [ijson['pt'], ijson['p'], ijson['c'], ijson['s'], ijson['vt'], ijson['m']])
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'tas_tagging.db')
        conn1, cur1   = self.get_connection(db_file)
        #column_tup = self.create_user_table_schama(conn1, cur1)
        
        crt_qry     = 'CREATE TABLE IF NOT EXISTS UsrTableCsvPhInfo(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(256), xml_id TEXT, period_type TEXT, period TEXT, currency TEXT, value_type TEXT, scale TEXT, month VARCHAR(20), review_flg INTEGER)'
        cur1.execute(crt_qry)
        
        data = []
        apply_xml_ids = []
        apply_cell_dict = {}
        db_file     = os.path.join('/mnt/eMB_db/', company_name, str(model_number), 'table_tagging_dbs', '%s.db'%table_id)
        conn, cur   = self.get_connection(db_file)
        user_sel_flg = 0 
        if rc:
            apply_xml_ids = []
            if len(rc) == 2:
                apply_xml_ids = xml_list[:]
                if not apply_xml_ids:
                    stmt = "select distinct(xml_id) from TableCsvPhInfo where cell_type='GV'"
                    cur.execute(stmt)
                    apply_xml_ids += map(lambda x:str(x[0]), cur.fetchall())    
                    user_sel_flg = 1
            else:
                stmt = "select nrow, ncol from TableCsvPhInfo where xml_id='%s'" %(xml_id)
                cur.execute(stmt)
                res = cur.fetchone()
                sel_row, sel_col = res[0], res[1]
                for rr in rc:
                    if rr == 'ROW':
                        stmt = "select distinct(xml_id) from TableCsvPhInfo where cell_type='GV' and nrow='%s'" %(sel_row)
                    elif rr == 'COL':
                        stmt = "select distinct(xml_id) from TableCsvPhInfo where cell_type='GV' and ncol='%s'" %(sel_col)
                    cur.execute(stmt)
                    apply_xml_ids += map(lambda x:str(x[0]), cur.fetchall())    
            ################################################################
            if xml_list:
                apply_xml_ids = [x for x in list(set(apply_xml_ids)) if x in xml_list]
            else:
                apply_xml_ids = list(set(apply_xml_ids))
            ################################################################   
        else:
            apply_xml_ids = [xml_id]
        ########################################################################
        app_xml_str = ', '.join(['"'+e+'"' for e in apply_xml_ids]) 
        stmt = "select org_row, org_col, nrow, ncol, cell_type, txt, xml_id, period_type, period, currency, value_type, scale, usr_value_type from TableCsvPhInfo where xml_id in (%s)" %(app_xml_str)
        cur.execute(stmt)
        res = cur.fetchall()
        mms = ['pt', 'p', 'c', 'vt', 's', 'm']
        sel_poss = []
        for x in sh_list:
            sel_poss.append((mms.index(x), str(ijson[x])))
        ################################################   
        new_ph_csv_data = [] 
        review_flg = 1
        for row in res:
            org_row, org_col, nrow, ncol, cell_type, txt, cell_ref, speriod_type, speriod, scurrency, svalue_type, sscale, smonth = map(str, row[:])
            mm = [speriod_type, speriod, scurrency, svalue_type, sscale, smonth, review_flg]   
            for (p, cc) in sel_poss:
                mm[p] = cc
            ttup = tuple(mm[:-1])
            review_flg = self.validate_cell_ph_info(ttup)
            mm[-1] = review_flg
            apply_cell_dict[cell_ref] = review_flg
            dtup = tuple([table_id, cell_ref] + mm[:]) 
            data.append(dtup)
            new_ph_csv_data.append((org_row, org_col, nrow, ncol, cell_type, txt, cell_ref, mm[0], '', mm[1], '', mm[2], '', mm[3], mm[5], mm[4], ''))
        ####### ADD TO USER DATA ###############################################    
        table_name = 'UsrTableCsvPhInfo'
        stmt = "delete from UsrTableCsvPhInfo where xml_id in (%s) and table_id='%s'" %(app_xml_str, table_id)
        cur1.execute(stmt)
        #msg = self.qObj.insertIntoLite(conn1, cur1, '', table_name, column_tup, data) 
        cur1.executemany('INSERT INTO UsrTableCsvPhInfo(table_id, xml_id, period_type, period, currency, value_type, scale, month, review_flg) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', data)
        conn1.commit()
        conn1.close()
        #####################ADD TO SYSTEM ######################################
        new_table_name = 'TableCsvPhInfo'
        stmt = "delete from TableCsvPhInfo where xml_id in (%s)" %(app_xml_str)
        cur.execute(stmt)
        #new_column_tup = ('org_row', 'org_col', 'nrow', 'ncol', 'cell_type', 'txt', 'xml_id', 'period_type', 'usr_period_type', 'period', 'usr_period', 'currency', 'usr_currency', 'value_type', 'usr_value_type', 'scale', 'usr_scale')
        #msg = self.qObj.insertIntoLite(conn, cur, '', new_table_name, new_column_tup, new_ph_csv_data) 
        cur.executemany('INSERT INTO TableCsvPhInfo(org_row, org_col, nrow, ncol, cell_type, txt, xml_id, period_type, usr_period_type, period, usr_period, currency, usr_currency, value_type, usr_value_type, scale, usr_scale) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', new_ph_csv_data)
        conn.commit()
        conn.close()       
        return 'done', apply_cell_dict

    ## EXCEL CONFIG GROUP INFO
    def get_tt_sub_groups_info(self, ijson):
        company_name      = ijson["company_name"]
        model_number      = ijson["model_number"]
        project_id        = ijson["project_id"]
        deal_id           = ijson["deal_id"]
        company_id        = '%s_%s'%(project_id, deal_id)

        db_file           = self.get_db_path(ijson)
        conn, cur         = conn_obj.sqlite_connection(db_file)

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        c_year      = self.get_cyear(lines)
        start_year  = c_year - int(ijson.get('year', 5))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            try:
                year    = int(ph[-4:])
            except:continue
            if ph and start_year < year:
                doc_id  = line[0]
                if ijson.get('ignore_doc_ids', {}) and doc_id in  ijson.get('ignore_doc_ids', {}):continue
                doc_d[doc_id]   = (ph, line[2])

        group_info_dict = OD()

        tt_data = self.read_doc_info(ijson)[0]['mt_list']
        table_types = [x['k'] for x in tt_data]
        for table_type in table_types:
            vgh_id_d, vgh_id_d_all, docinfo_d = {},{},{}
            m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [table_type])
            sql         = "select row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, ph, ph_label,gcom, ngcom, doc_id, m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type from mt_data_builder where table_type='%s' and isvisible='Y'"%(table_type)
            try:
                cur.execute(sql)
                res         = cur.fetchall()
            except:
                res = []
            for rr in res:
                row_id, taxo_id, order_id, taxonomy, user_taxonomy, missing_taxo, table_id, gv_xml, ph, ph_label,gcom, ngcom, doc_id,m_rows, vgh_text, vgh_group, xml_id, period, period_type, scale, currency, value_type    = rr
                doc_id      = str(doc_id)
                if doc_id not in doc_d:continue
                table_id    = str(table_id)
                if table_id not in m_tables:continue
                tk          = self.get_quid(table_id+'_'+xml_id)
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                if not c_id:continue
                c   = int(c_id.split('_')[2])
                key     = table_id+'_'+self.get_quid(xml_id)
                vgh_id_d.setdefault(vgh_text, {})[(table_id, c_id, row_id, doc_id)]        = 1
                vgh_id_d_all[vgh_text]  = 1
                doc_id      = str(doc_id)
                table_id    = str(table_id)
                docinfo_d.setdefault(doc_id, {})[(table_id, c_id, vgh_text)]   = 1
 
            group_info_dict[table_type] = self.read_all_vgh_groups(table_type, company_name, model_number,vgh_id_d, vgh_id_d_all, docinfo_d) 
        conn.close()
        return group_info_dict

    ## 83 Config Save
    def saveCompanyConfigTable(self, ijson):
        company_name     = ijson["company_name"]
        model_number     = ijson["model_number"]
        project_id       = ijson["project_id"]
        deal_id          = ijson["deal_id"]
        company_id       = '%s_%s'%(project_id, deal_id)
        gbl              = int(ijson["gbl"])
        proj_info_li     = ijson['p_list']
        all_companies    = int(ijson["all"])

        db_file           = self.get_db_path(ijson)
        self.gbl_update(ijson,db_file)
        self.saveTableType_wiseFormula_company(ijson, db_file)
        db_file ='/mnt/eMB_db/node_mapping.db'
        self.saveTableType_wiseFormula_company(ijson, db_file)
        if gbl:
            self.gbl_update(ijson,db_file)
        if all_companies:
            dct_all_companies = self.get_list_all_companies()
            for comp_id, comp_name in dct_all_companies.items():
                if comp_name == company_name:continue
                db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(comp_name, model_number)
                self.gbl_update_all(company_name, company_id, db_file, proj_info_li)
        companyConfig_data = self.read_companyConfigTable(ijson)
        proj_info = self.getProjectId_details_comp(ijson)
        return [{'message':'done','cf_data':companyConfig_data, 'proj_info':proj_info}]

    def gbl_update(self, ijson,db_file):
        
        company_name     = ijson["company_name"]
        model_number     = ijson["model_number"]
        project_id       = ijson["project_id"]
        deal_id          = ijson["deal_id"]
        company_id       = '%s_%s'%(project_id, deal_id)
        gbl              = int(ijson["gbl"])
        proj_info_li     = ijson['p_list']
        conn, cur         = conn_obj.sqlite_connection(db_file)

        crt_qry          = "CREATE TABLE IF NOT EXISTS company_config(row_id INTEGER PRIMARY KEY AUTOINCREMENT, company_id VARCHAR(20), company_name TEXT, project_id INTEGER, project_name TEXT, reporting_type VARCHAR(256), total_years INTEGER, derived_ph VARCHAR(256), periods VARCHAR(256), ind_group_flg INTEGER, taxo_flg INTEGER, excel_config_str TEXT, rsp_period TEXT, rsp_year TEXT, rfr TEXT, restated_period TEXT)"
        cur.execute(crt_qry)
        
        self.alter_table_coldef(conn, cur, 'company_config', ['rsp_period', 'rsp_year', 'rfr', 'restated_period'])

        def get_max_pid():
            slt_qry  = "select max(project_id) from company_config;"
            cur.execute(slt_qry)
            slt_data = cur.fetchone()
            p_id = 1
            if slt_data and slt_data[0]:
                p_id = int(slt_data[0]) + 1
            return p_id

        data_rows   = []
        update_rows = []
        PID         = 0
        for proj_info_dict in proj_info_li:
            project_name, reporting_type, total_years   = proj_info_dict['n'], proj_info_dict['st'], proj_info_dict['yr']
            derived_ph_lst, periods_lst                 = proj_info_dict['pt'], proj_info_dict['phd']
            ind_group_flg, taxo_flg, cur_pid            = proj_info_dict['rp'], proj_info_dict['tx'], proj_info_dict.get('pid', '')
            excel_config_map                            = proj_info_dict['tt_list']
            rsp_period, rsp_year                        = proj_info_dict['re_p'], proj_info_dict['re_yr']
            rfr, restated_period                        = proj_info_dict['rfr'], proj_info_dict['rest_p']

            qry = 'select company_name, project_id from company_config where project_name like "%s"'%(project_name)
            cur.execute(qry)
            daa = cur.fetchone()

            if daa and daa[1]:
                cur_pid = str(daa[1])
            
            derived_ph  = '##'.join(derived_ph_lst)
            periods     = '##'.join(periods_lst)

            parent_child_rel = OD()
            for pdict in excel_config_map:
                parent      = pdict['p']
                child_li    = pdict['c']
                grid        = pdict['grpid']
                edit_p      = str(pdict.get('edit_p', 0))
                pm_header   = pdict.get('mh', '')
                ptaxo_ids_lst = map(str, pdict.get('t_ids', []))
                ptaxo_ids_str = '&&'.join(ptaxo_ids_lst)
                pkey        = '@:@'.join([parent, grid, edit_p, pm_header, ptaxo_ids_str])
                if child_li:
                    for cdict in child_li:
                        ch      = cdict['p']
                        cgrid   = cdict['grpid']
                        main_header = cdict.get('mh', '')
                        ctaxo_ids_lst = map(str, cdict.get('t_ids', []))
                        ctaxo_ids_str = '&&'.join(ctaxo_ids_lst)
                        ckey = '^^'.join([cgrid, main_header, ctaxo_ids_str])
                        parent_child_rel.setdefault(pkey, []).append(ckey)
                else:
                    parent_child_rel[pkey] = []

            excel_config_str = '^!!^'.join(['$$'.join([pmap, '@@'.join(cli)]) for pmap, cli in parent_child_rel.iteritems()])

            if not daa:
                new_max_pid = get_max_pid()
                PID = str(new_max_pid)
                data_rows.append((company_id, company_name, new_max_pid, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period))
            #elif cur_pid != 'new':
            else:
                PID = cur_pid
                update_rows.append((company_id, company_name, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period, project_name))

        if update_rows:
            cur.executemany("UPDATE company_config set company_id=?, company_name=?, project_name=?, reporting_type=?, total_years=?, derived_ph=?, periods=?, ind_group_flg=?, taxo_flg=?, excel_config_str=?, rsp_period=?, rsp_year=?, rfr=?, restated_period=?  WHERE project_name=?", update_rows)
        if data_rows:
            cur.executemany("INSERT INTO company_config(company_id, company_name, project_id, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data_rows)

        conn.commit()
        conn.close()
    def get_list_all_companies(self):
        file_path = '/mnt/eMB_db/dealid_company_info.txt'
        f = open(file_path)
        company_details = f.readlines()
        f.close()
        companyName_companyId_map = {}
        for comp in company_details:
            company_id, company_name, ippadrs = comp.split(':$$:')
            companyName_companyId_map[company_id] = company_name
        return companyName_companyId_map
    def gbl_update_all(self, company_name, company_id, db_file, proj_info_li):
        conn, cur         = conn_obj.sqlite_connection(db_file)

        crt_qry          = "CREATE TABLE IF NOT EXISTS company_config(row_id INTEGER PRIMARY KEY AUTOINCREMENT, company_id VARCHAR(20), company_name TEXT, project_id INTEGER, project_name TEXT, reporting_type VARCHAR(256), total_years INTEGER, derived_ph VARCHAR(256), periods VARCHAR(256), ind_group_flg INTEGER, taxo_flg INTEGER, excel_config_str TEXT, rsp_period TEXT, rsp_year TEXT, rfr TEXT, restated_period TEXT)"
        cur.execute(crt_qry)
        self.alter_table_coldef(conn, cur, 'company_config', ['rsp_period', 'rsp_year', 'rfr', 'restated_period'])

        update_rows = []
        PID         = 0
        for proj_info_dict in proj_info_li:
            project_name, reporting_type, total_years   = proj_info_dict['n'], proj_info_dict['st'], proj_info_dict['yr']
            derived_ph_lst, periods_lst                 = proj_info_dict['pt'], proj_info_dict['phd']
            ind_group_flg, taxo_flg, cur_pid            = proj_info_dict['rp'], proj_info_dict['tx'], proj_info_dict.get('pid', '')
            #excel_config_map                            = proj_info_dict['tt_list']
            rsp_period, rsp_year                        = proj_info_dict['re_p'], proj_info_dict['re_yr']
            rfr, restated_period                        = proj_info_dict['rfr'], proj_info_dict['rest_p'] 
            qry = 'select company_name, project_id from company_config where project_name like "%s"'%(project_name)
            cur.execute(qry)
            daa = cur.fetchone()

            if not daa:
                return 'project name does not exists'

            if daa and daa[1]:
                cur_pid = str(daa[1])
            
            derived_ph  = '##'.join(derived_ph_lst)
            periods     = '##'.join(periods_lst)

            if 1:
                PID = cur_pid
                update_rows.append((reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, rsp_period, rsp_year, rfr, restated_period, project_name))

        if update_rows:
            cur.executemany("UPDATE company_config set reporting_type=?, total_years=?, derived_ph=?, periods=?, ind_group_flg=?, taxo_flg=?, rsp_period=?, rsp_year=?, rfr=?, restated_period=? WHERE project_name=?", update_rows)
        conn.commit()
        conn.close()
        return 'done'
    ## 61 Read Comp Config
    def read_companyConfigTable(self, ijson):
        company_name     = ijson["company_name"]
        model_number     = ijson["model_number"]
        deal_id          = ijson["deal_id"] 
        project_id       = ijson["project_id"]
        company_id       = '%s_%s'%(project_id, deal_id)

        db_file          = self.get_db_path(ijson)
        rs_ls, proj = self.read_gbl_companyConfig(db_file, 0, {}) 

        db_file          = '/mnt/eMB_db/node_mapping.db'
        res_lst, project = self.read_gbl_companyConfig(db_file, 1, proj)
        return rs_ls + res_lst 

    def read_gbl_companyConfig(self, db_file, gbl, ignore_proj):
        
        conn, cur        = conn_obj.sqlite_connection(db_file)
        self.alter_table_coldef(conn, cur, 'company_config', ['rsp_period', 'rsp_year', 'rfr', 'restated_period'])
        read_qry         = "select company_id, company_name, project_id, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period from company_config"
        try:
            cur.execute(read_qry)
            companyConfig_data      = cur.fetchall()
        except:
            companyConfig_data      = []
        conn.close()
        res_lst          = []
        proj = {}
        db_file_gbl = '/mnt/eMB_db/node_mapping.db'
        proj_n_d = self.formula_read_company(db_file_gbl)
        for row in companyConfig_data:
            
            company_id, company_name, project_id, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str, rsp_period, rsp_year, rfr, restated_period = map(str, row)
            if project_name in ignore_proj:continue
            proj[project_name] = 1
            derived_ph_li   = derived_ph.split('##')
            period_li       = periods.split('##')
            tt_list         = []

            excel_config_li = excel_config_str.split('^!!^')
            #IS@:@@:@0@:@IS$$^!!^BS@:@@:@0@:@BS$$^!!^CF@:@@:@0@:@CF$$^!!^RBS@:@@:@0@:@RBS$$18^^RBS@@17^^RBS@@HGHGROUP-RBS-56354-^^RBS@@HGHGROUP-RBS-56410-^^RBS@@HGHGROUP-RBS-56392-^^RBS^!!^RBG@:@@:@0@:@RBG$$
            for e in excel_config_li:
                if e == '':continue
                pstr, groups = e.split('$$')
                try:
                    p, pgrid, edf, pmh, ptids = pstr.split('@:@')
                except:
                    p, pgrid, edf, pmh = pstr.split('@:@')
                    ptids = ''
                cli = []
                if groups:
                    group_li = groups.split('@@')
                    for group in group_li:
                        try:
                            cgrid, cmh, ctids = group.split('^^')
                        except:
                            cgrid, cmh = group.split('^^')
                            ctids = ''
                        t = {'p':'', 'mh':cmh, 'c':[], 'grpid':cgrid, 't_ids':ctids}
                        cli.append(t)
                tmp = {'p':p, 'c':cli, 'edit_p':int(edf), 'grpid':pgrid, 'mh':pmh, 't_ids':ptids}
                tt_list.append(tmp)

            ph_list = proj_n_d.get(project_name, [])
            
            if rsp_period == "None":
                rsp_period = ""
            if rsp_year == "None":
                rsp_year = ""
            if rfr == "None":
                rfr = ""
            if restated_period == "None":
                restated_period = "" 
            res_dct = {'pid':project_id, 'n':project_name, 'st':reporting_type, 'yr':total_years, 'pt':derived_ph_li, 'phd':period_li, 'rp':ind_group_flg, 'tx':taxo_flg, 'ef':0, 'tt_list':tt_list, 'gbl':gbl, 'ph_list':ph_list, 're_p':rsp_period, 're_yr':rsp_year, 'rfr':rfr, 'rest_p':restated_period}
            res_lst.append(res_dct)
        res_lst = sorted(res_lst, key=lambda x:int(x['pid']))
        return res_lst, proj

    ## 84 Delete Comp Config
    def delete_companyConfigTable_pid(self, ijson):
        company_name     = ijson["company_name"]
        model_number     = ijson["model_number"]
        deal_id          = ijson["deal_id"] 
        project_id       = ijson["project_id"]
        company_id       = '%s_%s'%(project_id, deal_id)

        gbl              = ijson["gbl"]
        project_name     = ijson["project_name"]
        p_id             = ijson["pid"]
        db_file          = self.get_db_path(ijson)
        conn, cur        = conn_obj.sqlite_connection(db_file)

        del_stmt = 'delete from company_config where project_name="%s"'%(project_name)
        cur.execute(del_stmt)
        conn.commit()
        conn.close()
        if gbl:
            db_file  = '/mnt/eMB_db/node_mapping.db'
            conn = sqlite3.connect(db_file)
            cur  = conn.cursor()
            del_stmt = 'delete from company_config where project_name="%s"'%(project_name)
            cur.execute(del_stmt)
            conn.commit()
            conn.close()
        return [{'message':'done'}]

    ## 89 Delete HGH group
    def delete_hgh_group(self, ijson):
        company_name     = ijson["company_name"]
        model_number     = ijson["model_number"]
        deal_id          = ijson["deal_id"]
        project_id       = ijson["project_id"]
        company_id       = '%s_%s'%(project_id, deal_id)

        hgh_gid          = ijson["gid"]

        if 'HGHGROUP' not in hgh_gid:
            return [{'message':'done'}]

        table_type       = ijson["table_type"]
        db_file          = self.get_db_path(ijson)
        conn, cur        = conn_obj.sqlite_connection(db_file)

        del_stmt = 'delete from mt_data_builder where table_type = "%s"'%(hgh_gid)
        cur.execute(del_stmt)
        #print '>>MT ', del_stmt

        del_stmt = 'delete from vgh_group_info where table_type = "%s"'%(hgh_gid)
        cur.execute(del_stmt)
        #print '>>VGH ', del_stmt

        conn.commit()
        conn.close()
        return [{'message':'done'}]

    def getProjectId_details(self, ijson):
        company_name = ijson["company_name"]
        model_number = ijson["model_number"]
        project_id   = ijson["project_id"]
        deal_id      = ijson["deal_id"]
        company_id   = "%s_%s"%(project_id, deal_id)
        db_path      = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn         = sqlite3.connect(db_path)
        cur          = conn.cursor() 
        
        comp_qry     = 'select project_id, project_name from company_config;'
        try:
            cur.execute(comp_qry)
            comp_data    = cur.fetchall()
        except:
            comp_data = []
        
        res_lst      = []
        required_lst      = []
        for row in comp_data:
            project_id, project_name = map(str, row)
            res_lst.append({'n':project_name, 'pid':project_id})
            required_lst.append(project_name)

        conn.close()
        
        required_str = ['"'+e+'"' for e in required_lst]
        gdb_path  = '/mnt/eMB_db/node_mapping.db'
        gconn     = sqlite3.connect(gdb_path)
        gcur      = gconn.cursor()
        gbl_qry   = "select project_id, project_name from company_config where project_name not in (%s)"%(', '.join(required_str))
        try:
            gcur.execute(gbl_qry)
            gbl_data  = gcur.fetchall()
        except:
            gbl_data = []
        for row in gbl_data:
            project_id, project_name = map(str, row)
            res_lst.append({'n':project_name, 'pid':project_id})
        gconn.close()    
        return res_lst
    def getProjectId_details_comp(self, ijson):
        company_name = ijson["company_name"]
        model_number = ijson["model_number"]
        project_id   = ijson["project_id"]
        deal_id      = ijson["deal_id"]
        company_id   = "%s_%s"%(project_id, deal_id)
        db_path      = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn         = sqlite3.connect(db_path)
        cur          = conn.cursor() 
        
        comp_qry     = 'select project_id, project_name from company_config;'
        try:
            cur.execute(comp_qry)
            comp_data    = cur.fetchall()
        except:
            comp_data = []
        
        res_lst      = []
        required_lst      = []
        for row in comp_data:
            project_id, project_name = map(str, row)
            res_lst.append({'n':project_name, 'pid':project_id})
            required_lst.append(project_name)

        conn.close()
        
        return res_lst

    def saveCompanyConfigTable_gbl(self, update_rows, data_rows):


        db_path          = '/mnt/eMB_db/node_mapping.db'
        conn             = sqlite3.connect(db_path)      
        cur              = conn.cursor() 

        crt_qry          = "CREATE TABLE IF NOT EXISTS company_config(row_id INTEGER PRIMARY KEY AUTOINCREMENT, company_id VARCHAR(20), company_name TEXT, project_id INTEGER, project_name TEXT, reporting_type VARCHAR(256), total_years INTEGER, derived_ph VARCHAR(256), periods VARCHAR(256), ind_group_flg INTEGER, taxo_flg INTEGER, excel_config_str TEXT)"
        cur.execute(crt_qry)

        if update_rows:
            cur.executemany("UPDATE company_config set company_id=?, company_name=?, project_name=?, reporting_type=?, total_years=?, derived_ph=?, periods=?, ind_group_flg=?, taxo_flg=?, excel_config_str=? WHERE project_id=?", update_rows)
        if data_rows:
            cur.executemany("INSERT INTO company_config(company_id, company_name, project_id, project_name, reporting_type, total_years, derived_ph, periods, ind_group_flg, taxo_flg, excel_config_str) VALUES(?,?,?,?,?,?,?,?,?,?,?)", data_rows)

        conn.commit()
        conn.close()
        return 'done'


    def show_all_table_types_and_formulas_company(self):
        period_wise_formula  = [
                    'Q4=FY-Q1-Q2-Q3',
                    'Q3=FY-Q1-Q2-Q4',
                    'Q2=FY-Q1-Q3-Q4',
                    'Q1=FY-Q2-Q3-Q4',
                    'FY=Q1+Q2+Q3+Q4',
                    'FY=H1+H2',
                    'H2=FY-H1',
                    'H2=FY',
                    'H1=Q1+Q2',
                    'Q2=H1-Q1',
                    'Q1=H1-Q2',
                    'Q3=M9',
                    'Q2=H1',
                    'Q4=FY',
                    'FY=Q4',
                    'Q4=FY-M9',
                    'Q3=M9-H1',
                ]

        formula_lst =  []
        for formula in period_wise_formula:
            period, expression = formula.split('=')
            formula_lst.append({'p':period, 'ex':expression})
        return formula_lst  
    
    def formula_read_company(self, db_path):
        conn    = sqlite3.connect(db_path)
        cur     = conn.cursor()

        crt_qry = "CREATE TABLE IF NOT EXISTS table_type_wise_formula(row_id INTEGER PRIMARY KEY AUTOINCREMENT, project_name VARCHAR(100),table_type VARCHAR(100), period VARCHAR(20), period_expression VARCHAR(100));"
        cur.execute(crt_qry)

        read_qry = 'select project_name, table_type, period, period_expression from table_type_wise_formula'
        cur.execute(read_qry)
        tableData = cur.fetchall()
        conn.close()
        dataLst_comp_dct = {}
        for row in tableData:
            project_name, table_type, period, period_expression = map(str, row)
            dataLst_comp_dct.setdefault(project_name, {}).setdefault(table_type, []).append({'p':period, 'ex':period_expression})
        f_d = {}
        for pname, dd in dataLst_comp_dct.items():
            dataLst_comp = []
            for tt, flst in dd.items():
                dataLst_comp.append({'p':tt, 'c':flst})
            f_d[pname]  = dataLst_comp[:]
        return f_d

    def saveTableType_wiseFormula_company(self, ijson, db_path):
        company_name = ijson["company_name"]
        model_number = ijson["model_number"]
        project_name = ijson["p_list"][0]["n"]
        storeData = ijson["p_list"][0]["ph_list"]
        conn    = sqlite3.connect(db_path)
        cur     = conn.cursor()

        crt_qry = "CREATE TABLE IF NOT EXISTS table_type_wise_formula(row_id INTEGER PRIMARY KEY AUTOINCREMENT, project_name VARCHAR(100),table_type VARCHAR(100), period VARCHAR(20), period_expression VARCHAR(100));"
        cur.execute(crt_qry)

        dlt_qry = 'delete from table_type_wise_formula where project_name="%s";'%(project_name)
        cur.execute(dlt_qry)
        data_rows = []
        for row in storeData:
            table_type, getC = row["p"], row["c"]
            if not getC:continue 
            for data in getC:
                period, period_expression = data["p"], data["ex"]
                tup = (project_name, table_type, period, period_expression)
                data_rows.append(tup)
        cur.executemany("INSERT INTO table_type_wise_formula(project_name, table_type, period, period_expression) VALUES(?, ?, ?, ?)", data_rows)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def copy_line_items(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        i_table_type    = ijson['table_type']
        ph_d        = {}
        path    = "%s/%s/%s/1_1/21/sdata/doc_map.txt"%(self.doc_path, project_id, deal_id)
        if os.path.exists(path):
            fin = open(path, 'r')
            lines   = fin.readlines()
            fin.close()
        else:
            lines   = []
        doc_d       = {}
        dphs        = {}
        c_year      = self.get_cyear(lines)
        start_year  = c_year - int(ijson.get('year', 10))
        for line in lines[1:]:
            line    = line.split('\t')
            if len(line) < 8:continue
            line    = map(lambda x:x.strip(), line)
            ph      = line[3]+line[7]
            if ph and start_year<int(ph[2:]):
                doc_id  = line[0]
                doc_d[doc_id]   = (ph, line[2])
                dphs[ph]        = 1

                    

        i_table_type    = ijson['table_type']
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number, [i_table_type])

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()

        lmdb_path   = '/var/www/html/Rajeev/BBOX/'+str(project_id)+'_'+str(deal_id)
        lmdb_path    = os.path.join(self.bbox_path, company_id, 'XML_BBOX')
        env1    = lmdb.open(lmdb_path)
        txn1    = env1.begin()

        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/ph_csv_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn         = env.begin()


        table_type  = str(i_table_type)
        group_d     = {}
        revgroup_d  = {}

        d_group_d     = {}
        d_revgroup_d  = {}

        vgh_id_d    = {}
        db_file         = self.get_db_path(ijson)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        #sql         = "select vgh_id, doc_id, table_id, group_txt from doc_group_map where table_type='%s'"%(table_type)
        #try:
        #    cur.execute(sql)
        #    res = cur.fetchall()
        #except:
        #    res = []
        grp_doc_map_d   = {}
        rev_doc_map_d   = {}
        #for rr in res:
        #    vgh_id, doc_id, table_id, group_txt = rr
        #    grp_doc_map_d.setdefault(group_txt, {})[table_id]   = doc_id #.setdefault(doc_id, {})[table_id]    = 1
        #    rev_doc_map_d.setdefault((doc_id, table_id), {})[group_txt] = 1
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        if ijson.get('vids', {}) and isinstance(ijson.get('vids', {}), list):
            ijson['vids']   = ijson['vids'][0]
        else:
            ijson['vids']   = {}
        g_vids  = ijson.get('vids', {})
        t_ids   = map(lambda x:str(x), ijson.get('t_ids', []))
        self.alter_table_coldef(conn, cur, 'mt_data_builder', ['target_taxonomy', 'numeric_flg', 'th_flg'])
        if ijson.get('vids', {}):
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s' and taxo_id in (%s) and  isvisible='Y' and vgh_text in (%s)"%(table_type, ', '.join(t_ids),  ', '.join(ijson['vids'].keys()))
        else:
            sql         = "select row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg from mt_data_builder where table_type='%s' and taxo_id in (%s) and isvisible='Y'"%(table_type, ', '.join(t_ids))
        cur.execute(sql)
        res         = cur.fetchall()
        docinfo_d   = {}
        vgh_id_d_all    = {}
        tmp_grpd        = {}
        table_ids       = {}
        all_vgh         = {}
        f_d             = {}
        f_o_d          = {}
        for rr in res:
            row_id, taxo_id, prev_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg   = rr
            tk   = self.get_quid(table_id+'_'+xml_id)
            c_id        = txn_m.get('XMLID_MAP_'+tk)
            if not c_id:continue
            c   = int(c_id.split('_')[2])
            if g_vids.get(vgh_text, {}) and table_id+'-'+str(c) not in g_vids.get(vgh_text, {}):continue
            rr  = (table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, tdatetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg)
            f_d.setdefault(taxo_id, {})[rr] = 1
            f_o_d[taxo_id]   = order_id
        tmpt_d          = {}
        all_table_types = {}
        for taxo_id, ks in f_d.items():
            t_ar    = []
            for rr in ks:
                rr      = list(rr)
                rr[0]   =  ijson['target_tt']
                rr[15]  =  'COPY_'+ijson['table_type']+"_"+str(taxo_id)
                t_ar.append(rr)
            tmpt_d[taxo_id]  = {'d':t_ar, 'o':f_o_d[taxo_id]}
        t_ars   = []
        t_ars.append(tmpt_d)    
        dtime   = str(datetime.datetime.now()).split('.')[0]
        i_ar    = []
        with conn:
            #sql = "select max(taxo_id) from kpi_input"
            sql = "select seq from sqlite_sequence WHERE name = 'mt_data_builder'"
            cur.execute(sql)
            r       = cur.fetchone()
            g_id    = int(r[0])+1
            sql     = "select max(taxo_id) from mt_data_builder" 
            cur.execute(sql)
            r       = cur.fetchone()
            tg_id    = int(r[0])+1
            g_id    = max(g_id, tg_id)
            for t_d in t_ars:
                for t_id, vd in t_d.items():
                    #print  t_id, vd
                    for tup in vd['d']:
                        #print '\n\tBB ',tup
                        tup = (g_id, vd['o'])+tuple(tup)
                        #print '\t',tup
                        i_ar.append(tup)
                    g_id    += 1
        cols    = 'taxo_id, order_id, table_type, taxonomy, user_taxonomy, missing_taxo, table_id, c_id, gcom, ngcom, ph, ph_label, user_name, datetime, isvisible, m_rows, vgh_text, vgh_group, doc_id, xml_id, period, period_type, scale, currency, value_type, target_taxonomy, numeric_flg, th_flg'
        cols_v  =', '.join(map(lambda x:'?', cols.split(',')))
        if 1:#ijson.get('update_db', '') == 'Y':
            cur.executemany('insert into mt_data_builder(%s) values(%s)'%(cols, cols_v), i_ar)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def clean_and_actual_value_status_crt_del(self, ijson):
        company_name = ijson["company_name"]
        model_number = ijson["model_number"]
        project_id   = ijson["project_id"]
        deal_id      = ijson["deal_id"]
        company_id   = "%s_%s"%(project_id, deal_id)
        data_lst     = ijson["list"]        
        del_flg      = int(ijson["del_flg"])

        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn    = sqlite3.connect(db_file)
        cur     = conn.cursor()
       
        # create table clean_actual_value_status
        crt_qry = "CREATE TABLE IF NOT EXISTS clean_actual_value_status(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_id VARCHAR(100), doc_id VARCHAR(100), table_type VARCHAR(100), value_status VARCHAR(20));"
        cur.execute(crt_qry)

        if not del_flg: 
            # preparing data to insert
            read_qry = 'select table_id, table_type from clean_actual_value_status;'
            cur.execute(read_qry)   
            actual_data = [tuple(map(str, tup))for tup in cur.fetchall()]

            data_rows = []
            for data in data_lst:
                table_id, doc_id, table_type = data["t"], data["d"], data["tt"]
                if tuple([table_id, table_type]) not in actual_data:
                    data_rows.append((table_id, doc_id, table_type, 'Y'))
            # inserting data 
            cur.executemany("INSERT INTO clean_actual_value_status(table_id, doc_id, table_type, value_status) VALUES(?, ?, ?, ?)", data_rows)

        elif del_flg:
            for data in data_lst:
                table_id, doc_id, table_type = data["t"], data["d"], data["tt"]
                del_stmt = 'delete from clean_actual_value_status where table_id="%s" and table_type="%s"'%(table_id, table_type)
                cur.execute(del_stmt)

        conn.commit()
        conn.close()
        return [{'message':'done'}]

    def match_main_sub_table(self, ijson, ret_flg=None, res_d={}, i_ar=[]):
        #ijson['table_types']    = ["IS", "BS", "CF"]
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        lmdb_path1  =  "/var/www/html/fill_table/%s_%s/table_info"%(project_id, deal_id)
        env         = lmdb.open(lmdb_path1)
        txn_m       = env.begin()
        m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, model_number)
        if not i_ar:
            i_ar            = []
            for table_type in rev_m_tables.keys():
                if not table_type:continue
                if ijson.get("table_types", []) and table_type not in ijson.get("table_types", []):continue
                ijson['table_type'] = table_type
                g_ar    = self.create_group_ar(ijson, txn_m)
                ijson_c = copy.deepcopy(ijson)
                ijson_c['table_type']   = table_type
                ijson_c['type']         = 'display'
                i_ar.append(ijson_c)
                for rr in g_ar:
                    #g_d[(table_type, rr['grpid'])] =  rr
                    ijson_c = copy.deepcopy(ijson)
                    ijson_c['type']         = 'display'
                    ijson_c['table_type']   = table_type
                    ijson_c['grpid']        = rr['grpid']
                    ijson_c['vids']         = rr['vids']
                    ijson_c['data']         = [rr['n']]
                    i_ar.append(ijson_c)

        data_d  = {}
        i_d     = {}
        v_index_d   = {}
        grp_info_d  = {}
        for ii, ijson_c in enumerate(i_ar):
            #print 'Running ', ii, '/', len(i_ar), [ijson_c['table_type'], ijson_c.get('grpid', ''), ijson_c.get('data', '')]
            ijson_c['NO_FORM']  = 'Y'
            grp_info_d[(ijson_c['table_type'], ijson_c.get('grpid', ''))]  = ijson_c.get('data', [''])[0]
            if (ijson_c['table_type'], ijson_c.get('grpid', '')) in res_d:
                res     = res_d[(ijson_c['table_type'], ijson_c.get('grpid', ''))] #self.create_final_output_with_ph(ijson_c,'P', 'N')
            else:
                res     = self.create_final_output_with_ph(ijson_c,'P', 'N')
            rev_map = {}
            db_ar   = []
            db_d    = {}
            for ti, rr in enumerate(res[0]['data']):
                rows_ar = []
                rows_ar_org = []
                for ph in res[0]['phs'] :
                    v, tph, scale   = '', ph['n'], ''
                    if ph['k'] in rr:
                        v_d = rr[ph['k']]
                        v, scale, vt, cry   = v_d['v'], v_d.get('phcsv', {}).get('s', ''), v_d.get('phcsv', {}).get('vt', ''), v_d.get('phcsv', {}).get('c', '')
                        v_org   = v
                        try:
                            clean_value = numbercleanup_obj.get_value_cleanup(v)
                        except:
                            clean_value = ''
                            pass
                        if clean_value:
                            v_org = str(self.convert_floating_point(float(clean_value)).replace(',', ''))
                            v = str(self.convert_floating_point(abs(float(clean_value))).replace(',', ''))
                        else:
                            v   = ''
                        if v:
                            rows_ar.append((tph, scale, vt, cry , v))
                            db_d.setdefault(ti, {})[tph] = (tph, scale, vt, cry , v_org)
                rev_map[rr['t_id']] = ti
                if not rows_ar:continue
                rows_ar = list(sets.Set(rows_ar))
                rows_ar.sort()
                row_tup    = tuple(rows_ar)
                db_ar.append((ti, rr['t_id'], row_tup))
                i_d.setdefault(row_tup, {}).setdefault((ijson_c['table_type'], ijson_c.get('grpid', '')), {})[(ti, rr['t_id'])]    = 1
                for rtup in row_tup:
                    v_index_d.setdefault(rtup, {})[row_tup]    = 1
            data_d.setdefault(ijson_c['table_type'], {})[ijson_c.get('grpid', '')]  = (res, rev_map, db_ar, db_d) 
        map_d   = {
                    'IS':["IS", "BS","CF"][:1],
                    'BS':["IS","BS", "CF"][:1],
                    'CF':["IS","BS", "CF"][:1],
                    }
        if ijson.get("table_types", []):
            map_d   = {}
            for tt in ijson.get("table_types", []):
                map_d[tt]   = [tt]
        final_d = {}
        match_d = {}
        for ttype in  ['IS', 'BS', 'CF']:
            self.match_db_row(ttype, map_d[ttype], data_d, i_d, v_index_d, grp_info_d, final_d, match_d)
            pass
        ks  = final_d.keys()
        ks.sort(key=lambda x:x[0])
        f_ar    = []
        for (table_type, grp) in ks:
            data_tup    = data_d[table_type][grp]
            #print '\n===================================================='
            #print [table_type, grp], grp_info_d[(table_type, grp)]
            t_ar    = []
            tis     = final_d[(table_type, grp)].keys()
            tis.sort()
            for ti in tis:
                mgrp = final_d[(table_type, grp)][ti]
                #print '\n\tLabel ', [ti], data_tup[0][0]['data'][ti]['t_l']
                tks = mgrp.keys()
                tks.sort()
                phs     = data_tup[3][ti]
                data_ar = []
                t_phs   = copy.deepcopy(data_tup[0][0]['phs'])
                dd      = copy.deepcopy(data_tup[0][0]['data'][ti])
                dd['M'] = 'Y'
                dd['tt']    = table_type
                dd['grp']   = grp_info_d[(table_type, grp)]
                for ph in t_phs:
                    if (ph['k'] not in dd):continue
                    if  (ph['k'] not in phs):
                        if ph['n'] in phs:
                            dd[ph['n']]    = dd[ph['k']]
                        del dd[ph['k']]
                data_ar.append(dd)
                for (m_tt, m_grp) in tks:
                    #print '\n\t\t-------------------------------------------------'
                    #print '\t\t', (m_tt, m_grp), grp_info_d[(m_tt, m_grp)]
                    for tmp_ti, nm_d in mgrp[(m_tt, m_grp)].items():
                        #print '\t\t\tLabel ', [tmp_ti], data_d[m_tt][m_grp][0][0]['data'][tmp_ti]['t_l']
                        dd      = copy.deepcopy(data_d[m_tt][m_grp][0][0]['data'][tmp_ti])
                        dd['tt']    = m_tt
                        dd['grp']   = grp_info_d[(m_tt, m_grp)]
                        for ph in data_d[m_tt][m_grp][0][0]['phs']:
                            if (ph['k'] not in dd):continue
                            if  (ph['k'] not in phs):
                                if ph['n'] in phs:
                                    dd[ph['n']]    = dd[ph['k']]
                                del dd[ph['k']]
                            if ph['n'] in nm_d:
                                dd[ph['n']]['nm']  = 'Y'
                        data_ar.append(dd)
                phs = filter(lambda x:x['n'] in phs, t_phs)
                tmpphs  = []
                for ph in phs:
                    ph['k'] = ph['n']
                    tmpphs.append(ph)
                f_ar.append({'n':table_type+' - '+ grp_info_d[(table_type, grp)], 'phs':tmpphs, 'data':data_ar, 'm_tt':map(lambda x:x[0], tks)})
            #f_ar.append({'n':table_type+' - '+ grp_info_d[(table_type, grp)], 'info':t_ar})
        if ret_flg == 'Y':
            return f_ar
        res = [{"message":'done', 'data':f_ar}]
        return res
                
        

    def match_db_row(self, table_type, ignore, data_d, i_d, v_index_d, grp_info_d, final_d, match_d):
        if table_type not in ignore:
            ignore.append(table_type)
        ignore  = sets.Set(ignore)
        for grp, data_tup in data_d.get(table_type, {}).items():
            for ti, t_id, rs in data_tup[2]:
                m_ar    = []
                m_d = {}
                for r in rs:
                    for rtup in v_index_d.get(r, {}).keys():
                        m_d.setdefault(rtup, {})[r] = 1
                for rtup, mtups in m_d.items():
                    mtups   = mtups.keys()
                    mtups.sort()
                    rem_types   = list(sets.Set(map(lambda x:x[0], i_d[rtup].keys())) - ignore)
                    if rtup == tuple(mtups) and rem_types:
                        m_ar.append((filter(lambda x:x[0] in rem_types, i_d[rtup].keys()), rtup))
                org_rd    = data_tup[3][ti]
                if m_ar:
                    #print '\n===================================================='
                    #print [table_type, grp], [ti, t_id], grp_info_d[(table_type, grp)]
                    #print 'Label ', data_tup[0][0]['data'][ti]['t_l']
                    #print rs, org_rd
                    ktup    = (table_type, grp)
                    for (rem_types, m_r) in m_ar:
                        for (m_tt, m_grp) in rem_types:
                            #print '\n\t-------------------------------------------------'
                            #print '\t', (m_tt, m_grp), grp_info_d[(m_tt, m_grp)]
                            for (tmp_ti, tmp_t_id) in i_d[m_r][(m_tt, m_grp)].keys():
                                #print '\t\tLabel ', (tmp_ti, tmp_t_id), data_d[m_tt][m_grp][0][0]['data'][tmp_ti]['t_l']
                                org_md    = data_d[m_tt][m_grp][3][tmp_ti]
                                all_eq  = 1
                                cks = list(sets.Set(org_rd.keys()).intersection(sets.Set(org_md.keys())))
                                not_c   = {}
                                #print '\t\tcks', cks, org_md
                                for ck in cks:
                                    if org_rd[ck] != org_md[ck]:
                                        all_eq      = 0
                                        not_c[ck]   = (org_rd[ck], org_md[ck])
                                        #break
                                #print '\t\tall_eq ', all_eq, not_c
                                if 1:
                                    final_d.setdefault(ktup, {}).setdefault(ti, {}).setdefault((m_tt, m_grp), {})[tmp_ti]   = not_c
                                    match_d.setdefault((table_type, grp, data_tup[0][0]['data'][ti]['t_id']), {})[(m_tt, m_grp, data_d[m_tt][m_grp][0][0]['data'][tmp_ti]['t_id'])]  = 1 

    def validate_csv(self, ijson):
        res = [{'message':'done'}]
        return res

    def read_data_error_class(self, ijson):
        company_name    = str(ijson['company_name'])
        model_number   = str(ijson['model_number'])
        deal_id         = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = "%s_%s"%(project_id, deal_id)
        #error_type      = str(ijson["error_type"])
        user_name       = str(ijson["user"])

        import model_view.cp_company_docTablePh_details as py 
        obj = py.Company_docTablePh_details()
        table_doc_map  = obj.get_docId_passing_tableId(company_id)        


        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn    = sqlite3.connect(db_file)
        cur     = conn.cursor()
        
        # create table
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        crt_qry = 'CREATE TABLE IF NOT EXISTS error_xmlid_storage(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), table_id VARCHAR(100), error_xmlid TEXT, error_type TEXT, error_msg TEXT, accept_flg VARCHAR(10), datetime TEXT, user_name TEXT);'
        cur.execute(crt_qry)
    
        read_qry = 'select table_type, table_id, error_xmlid, error_type, error_msg from error_xmlid_storage;'
        cur.execute(read_qry)
        table_data = cur.fetchall()

        table_id_wise_dct = {}
        for row in table_data:
            table_type, table_id, error_xmlid, error_type, error_msg = map(str, row)
            
            if table_type not in table_id_wise_dct:
                table_id_wise_dct[table_type] = {}
        
            if table_id not in table_id_wise_dct[table_type]:   
                table_id_wise_dct[table_type][table_id]  = {}
            
            if error_type not in table_id_wise_dct[table_type][table_id]:
                table_id_wise_dct[table_type][table_id][error_type] = {}
        
            if error_msg not in table_id_wise_dct[table_type][table_id][error_type]:
                table_id_wise_dct[table_type][table_id][error_type][error_msg] = []
            table_id_wise_dct[table_type][table_id][error_type][error_msg].append(error_xmlid)
             
        res_lst = []
        for table_type, table_id_dct in table_id_wise_dct.items():
            tt_dct = {'tt':table_type, 't_list':[]}
            for table_id, error_class_dct in table_id_dct.items():
                doc_id    = table_doc_map.get(table_id, '')  
                table_dct = {'t':table_id, 'd':doc_id, 'ecl':[]}
                for error_class, error_msg_dct in error_class_dct.items():
                    ec_dct = {'e_c':error_class, 'eml':[]}
                    for error_msg, xml_lst in error_msg_dct.items():
                        em_dct = {'em':error_msg, 'xml':xml_lst}  
                        ec_dct['eml'].append(em_dct)
                    table_dct['ecl'].append(ec_dct)
                tt_dct['t_list'].append(table_dct)
            res_lst.append(tt_dct)
        return [{'message':'done', 'data':res_lst}]

    def multiclass_error(self, ijson):
        
        import datetime     
        company_name    = str(ijson['company_name'])
        model_number   = str(ijson['model_number'])
        deal_id         = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = "%s_%s"%(project_id, deal_id)
        #error_type      = str(ijson["error_type"])
        user_name       = str(ijson["user"])


        import model_view.phcsv_check as PhCsv_Validata
        obj = PhCsv_Validata.PhCsv_Validata()         
        error_class_elms = obj.validate(company_name, model_number, company_id)
        
        import model_view.cp_company_docTablePh_details as py 
        obj = py.Company_docTablePh_details()
        tableId_tableType_map = obj.get_table_sheet_map(company_name, model_number, company_id)

        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn    = sqlite3.connect(db_file)
        cur     = conn.cursor()
        
        # create table
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        crt_qry = 'CREATE TABLE IF NOT EXISTS error_xmlid_storage(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), table_id VARCHAR(100), error_xmlid TEXT, error_type TEXT, error_msg TEXT, accept_flg VARCHAR(10), datetime TEXT, user_name TEXT);'
        cur.execute(crt_qry)
        
        lmdb_path = '/var/www/html/fill_table/%s/table_info'%(company_id)
        env = lmdb.open(lmdb_path)
        txn = env.begin()
                
        data_rows = []    
        # save error_phcsv data 
        for table_type, error_msg_dct in error_class_elms.items():
            #row = [table_type]
            for error_msg, tableId_Doc_lst in  error_msg_dct.items():
                #print error_msg, tableId_Doc_lst;continue
                for tableId_Doc in tableId_Doc_lst:
                    #print tableId_Doc;continue
                    if type(tableId_Doc) == tuple:
                        table_id, doc_id = tableId_Doc
                        gv_key = 'GV_' + table_id
                        getGVcids = txn.get(gv_key)
                        if getGVcids == None:continue
                        for gcids in getGVcids.split('#'):
                            getXmlid_key = 'XMLID_' + gcids
                            gv_xml  = txn.get(getXmlid_key)
                            if getXmlid_key == None:continue  
                            entry_time = datetime.datetime.now()
                            data = (table_type, table_id, gv_xml, error_type, error_msg,'0', str(entry_time), user_name) 
                            data_rows.append(data)
                    elif type(tableId_Doc) == list:
                        for lst in tableId_Doc:
                            table_id, doc_id = lst
                            gv_key = 'GV_' + table_id
                            getGVcids = txn.get(gv_key)
                            if getGVcids == None:continue
                            for gcids in getGVcids.split('#'):
                                getXmlid_key = 'XMLID_' + gcids
                                gv_xml  = txn.get(getXmlid_key)
                                if getXmlid_key == None:continue  
                                entry_time = datetime.datetime.now()
                                data = (table_type, table_id, gv_xml, error_type, error_msg,'0', str(entry_time), user_name) 
                                data_rows.append(data)
                           
        del_qry = 'delete from error_xmlid_storage where error_type="%s"'%(error_type)
        cur.execute(del_qry)
        
        cur.executemany('INSERT INTO error_xmlid_storage(table_type, table_id, error_xmlid, error_type, error_msg, accept_flg, datetime, user_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?)', data_rows)
    
        conn.commit()
        conn.close()
        return [{'message':'done'}] 

    def phcsv_error_data(self, ijson):
    
        import datetime     
        company_name    = str(ijson['company_name'])
        model_number   = str(ijson['model_number'])
        deal_id         = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = "%s_%s"%(project_id, deal_id)
        #error_type      = str(ijson["error_type"])
        user_name       = str(ijson["user"])

        import model_view.phcsv_check_new as PhCsv_Validata
        obj = PhCsv_Validata.PhCsv_Validata()
        #print company_name, model_number, company_id;sys.exit()
        error_phcsv_dct = obj.validate_csv(company_name, model_number, company_id)

        import model_view.cp_company_docTablePh_details as py 
        obj = py.Company_docTablePh_details()
        tableId_tableType_map = obj.get_table_sheet_map(company_name, model_number, company_id)
        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn    = sqlite3.connect(db_file)
        cur     = conn.cursor()
        
        # create table
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        crt_qry = 'CREATE TABLE IF NOT EXISTS error_xmlid_storage(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), table_id VARCHAR(100), error_xmlid TEXT, error_type TEXT, error_msg TEXT, accept_flg VARCHAR(10), datetime TEXT, user_name TEXT);'
        cur.execute(crt_qry)
                    
        # save error_phcsv data 
        data_rows = []
        for table_id, error_xmlId_dct in error_phcsv_dct.items():
            table_type_lst = tableId_tableType_map.get(table_id, '')
            if not table_type_lst:continue 
            for table_type in table_type_lst:
                for error_xmlid in error_xmlId_dct.keys():
                    entry_time = datetime.datetime.now()
                    data = (table_type, table_id, error_xmlid, 'phcsv_error', 'phcsv_error', '0', str(entry_time), user_name)
                    data_rows.append(data) 
        #print data_rows   
        #sys.exit()
     
        del_qry = 'delete from error_xmlid_storage where error_type="%s"'%(error_type)
        cur.execute(del_qry)
        
        cur.executemany('INSERT INTO error_xmlid_storage(table_type, table_id, error_xmlid, error_type, error_msg, accept_flg, datetime, user_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?)', data_rows)
    
        conn.commit()
        conn.close()
        return [{'message':'done'}] 

        
    def error_classes_wise(self, ijson):
        company_name    = str(ijson['company_name'])
        model_number   = str(ijson['model_number'])
        deal_id         = str(ijson['deal_id'])
        project_id      = str(ijson['project_id'])
        company_id      = "%s_%s"%(project_id, deal_id)
        error_type      = str(ijson["error_type"])
        user_name       = str(ijson["user"])

        import model_view.cp_company_docTablePh_details as py 
        obj = py.Company_docTablePh_details()
        table_doc_map  = obj.get_docId_passing_tableId(company_id)        


        db_file = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn    = sqlite3.connect(db_file)
        cur     = conn.cursor()
        
        # create table
        #sql = "CREATE TABLE IF NOT EXISTS vgh_doc_map(row_id INTEGER PRIMARY KEY AUTOINCREMENT, vgh_group_id TEXT, doc_group_id TEXT, table_type VARCHAR(100), group_txt TEXT, user_name VARCHAR(100), datetime TEXT);"
        crt_qry = 'CREATE TABLE IF NOT EXISTS error_xmlid_storage(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), table_id VARCHAR(100), error_xmlid TEXT, error_type TEXT, error_msg TEXT, group_id TEXT, accept_flg VARCHAR(10), datetime TEXT, user_name TEXT);'
        cur.execute(crt_qry)
    
        read_qry = 'select table_type, table_id, error_xmlid, error_type, error_msg from error_xmlid_storage;'
        cur.execute(read_qry)
        table_data = cur.fetchall()

        table_id_wise_dct = {}
        for row in table_data:
            table_type, table_id, error_xmlid, error_type, error_msg = map(str, row)
            if table_type not in table_id_wise_dct:
                table_id_wise_dct[table_type] = {}
            if table_id not in table_id_wise_dct[table_type]:   
                table_id_wise_dct[table_type][table_id]  = {}
            if error_type not in table_id_wise_dct[table_type][table_id]:
                table_id_wise_dct[table_type][table_id][error_type] = {}
            if error_msg not in table_id_wise_dct[table_type][table_id][error_type]:
                table_id_wise_dct[table_type][table_id][error_type][error_msg] = []
            table_id_wise_dct[table_type][table_id][error_type][error_msg].append(error_xmlid)
            
        res_lst = []
        for table_type, table_id_dct in table_id_wise_dct.items():
            tt_dct = {'tt':table_type, 'error_type':[], 'table_info':[]}
            for table_id, error_type_dct in table_id_dct.items():
                table_dct = {'t':table_id}
                for error_type, error_msg_dct in error_type_dct.items():
                    ec_dct = {error_type:[]}
                    if error_type not in tt_dct['error_type']:
                        tt_dct['error_type'].append(error_type)
                    for error_msg, xml_lst in error_msg_dct.items():
                        ec_dct[error_type].append({'em':error_msg, 'x':xml_lst})
                    table_dct.update(ec_dct)
                tt_dct['table_info'].append(table_dct)
            res_lst.append(tt_dct)
        return [{'message':'done', 'data':res_lst}]



    def table_connection_validation(self, ijson, i_ar=[]):
        ijson['table_types']    = ["IS", "BS", "CF"]
        i_ar    = filter(lambda x:x['table_type'] in ijson['table_types'], i_ar)
        res = self.match_main_sub_table(ijson, 'Y', i_ar)
        dd  = {}
        for rr in res:
            tt1 = rr['n'].split(' - ')[0]
            for tt2 in rr['m_tt']:
                if tt1 == tt2:continue
                tup = [tt1, tt2]
                tup.sort()
                dd[tuple(tup)]  = 1
        missing_conn    = {}
        for tt in ijson['table_types']:
            for tt1 in ijson['table_types']:
                if tt == tt1:continue
                tup = [tt, tt1]
                tup.sort()
                tup = tuple(tup)
                if tup not in dd:
                    missing_conn[tup]    = 1
        print 'Missing Connection ', missing_conn
                    
            

    def db_validation(self, ijson):
        final_ar    = self.gen_final_output(ijson, 'Y')
        tc_d        = {}
        res_d       = {}
        for ii, ijson_c in enumerate(final_ar):
            ijson_c['gen_output'] = 'Y'
            table_type  = ijson_c['table_type']
            print 'Running ', ii, ' / ', len(final_ar), [ijson_c['table_type'], ijson_c['type'], ijson_c.get('data', []), ijson_c.get('grpid', '')]
            if ijson_c['type'] == 'group':
                for vid, t_d in ijson_c['vids'][0].items():
                    for tid in t_d.keys():
                        tc_d.setdefault(table_type, {}).setdefault(tid, {})[ijson_c['grpid']]   = ijson_c['data']
            res                 = self.create_seq_across(ijson_c)
            ph_cd   = {}
            for ph in res[0]['phs']:
                ph_cd.setdefault(ph['g'], {}).setdefault(ph['n'], []).append(ph)
            for g, ph_d in ph_cd.items():
                phs = filter(lambda x:len(ph_d[x])> 1, ph_d.keys())
                if not phs:continue
                print '\n=============================================================='
                print 'More than one column has same PH in table ', (table_type, g)
                for ph in phs:
                    print '\t', ph, map(lambda x:x['k'], ph_d[ph])
            ijson_c['DB_DATA']  = copy.deepcopy(res)
            ijson_c['type'] = 'display'
            res1     = self.create_final_output_with_ph(ijson_c,'P', 'Y')
            if res1[0]['message'] != 'done':
                res1    = res1[0]['res']
            if table_type in ['IS', 'BS', 'CF']:
                ijson_c['NO_FORM']  = 'Y'
                ijson_c['DB_DATA']  = copy.deepcopy(res)
                res0     = self.create_final_output_with_ph(ijson_c,'P', 'N')
                if res0[0]['message'] != 'done':
                    res0    = res0[0]['res']
                res_d[(table_type, ijson_c.get('grpid', ''))]   = res0
        self.table_connection_validation(ijson, final_ar)
                    
                    
        for table_type, t_d in tc_d.items():
                tids    = filter(lambda x:len(t_d[x].keys())> 1, t_d.keys())
                if tids:
                    print '\n========================================================='
                    print 'More than one column in group ', table_type
                    for tid in tids:
                        print '\t',tid, tc_d[tid]
                pass
        

    def phcsv_validation(self, ijson):
        import validation.ph_csv as ph_csv
        ph_csv_obj  = ph_csv.PHCSV()
        res         = ph_csv_obj.validate_phcsv(ijson)

    def run_validation(self, ijson):
        if ijson.get('PRINT', '') != 'Y':
            disableprint()
        self.phcsv_validation(ijson)
        #self.db_validation(ijson)
        enableprint()
        pass
