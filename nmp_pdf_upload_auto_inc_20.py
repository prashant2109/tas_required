import os, sys
import copy 
def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass
def enableprint():
    return
    sys.stdout = sys.__stdout__
    pass

class BDS_Data(object):

    def __init__(self):
        pass
    

    def data_preparation_bds(self, company_name, doc_ids):
        import error_info as ei
        eObj = ei.ErrorInfo()
         
        dbname = 'AECN_INC'
        import view.TestRead.chunk_error_cls as chunk_error_cls 
        chkobj = chunk_error_cls.chunk_error_cls('/root/tas_processing_code/WorkSpaceBuilder_DB/pysrc/dbConfig.ini')
        import view.data_builder as data_builder
        obj = data_builder.view('/root/tas_processing_code/WorkSpaceBuilder_DB/pysrc/dbConfig.ini') 
        data  = obj.get_doc_id_meta_data_doc_wise_dct_20(dbname, doc_ids) 
        
        project_id = '34'
        doc_page_grid_dict = {}
        doc_phlst   = []
        for data_elm in  data:
            doc_phlst.append((data_elm[0], '%s%s'%(data_elm[1]['periodtype'], data_elm[1]['Year']), data_elm[5]+'.pdf'))
            phcsv_data = data_elm[3]
            #print phcsv_data
            kdata = data_elm[2]
            ws_id = '1'
            doc_ppath = chkobj.base_path + str(project_id) + '/' + str(ws_id) + '/pdata/docs/'+str(data_elm[0])
            if 1: 
             for page_no in kdata.keys():
                if str(page_no) not in ['51']: continue
                #print ' data_elm: ', data_elm[3][page_no][1], page_no 
                #print type(page_no)  
                #if str(page_no) != '31': continue
                #if str(page_no) != '26': continue
                cell_info = [str(page_no), doc_ppath+'/CID/',    1, 1] # page_no, cid_path, isdb, isenc
                cell_dict = chkobj.get_cell_dict(cell_info[0], cell_info[1], cell_info[2], cell_info[3])
    

                gids = kdata[page_no].keys()
                for gid in gids:
                    #print '>>>>>>>>>>>>>>>>>', (data_elm[0], page_no, gid) 
                     
                    
                    #if str(gid) != '4': continue
                    if str(gid) != '10': continue
                    if gid > 1000: continue
                    rcount = 10000
                    #print '==================================='
                    #print data_elm[0], page_no, gid
                    xml_d   = phcsv_data.get(int(page_no), {}).get(int(gid), {})
                    ddict =  kdata[page_no][gid]['data']
                    #print ddict
                    #sys.exit()
                    #print 'ddict: ', ddict 
                    mykeys_dict = { 'mdata':[], 'text_lst':[], 'rowspan':1, 'font_prop_lst':[], 'attr':[], 'colspan':1, 'section_type':'', 'text_ids':[], 'isbold':[], 'bbox_lst':[], 'level_info':-1 }
                    # SUNIL PHCSV INFO  { valref: ( ph, csv) }
                    gid_dict = {}
                    r_cs = ddict.keys()
                    r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
                    for r_c in r_cs:
                        #print ddict[r_c]
                        row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
                        gid_dict[(row, col)] = copy.deepcopy(mykeys_dict)
                        gid_dict[(row, col)]['text_lst'] =  [ ddict[r_c]['data'] ]
                        print ((row, col), ddict[r_c]['data'], ddict[r_c]['ldr'], ddict[r_c].get('colspan', ''), ddict[r_c].get('rowspan', ''), ddict[r_c]['xml_ids'])
                        try: 
                            gid_dict[(row, col)]['rowspan'] =  ddict[r_c]['rowspan'] 
                        except:
                            gid_dict[(row, col)]['rowspan'] =  '1'
                        try:
                             gid_dict[(row, col)]['colspan'] =  ddict[r_c]['colspan'] 
                        except:
                             gid_dict[(row, col)]['colspan'] =  '1'
                        bl = []
                        bbox_lst = ddict[r_c]['bbox'].split('$$') 
                        for bx in bbox_lst:#xmin_ymin_xmax_ymax
                            if bx:
                                b = map(lambda x:int(x), bx.split('_'))
                                x   = b[0]
                                y   = b[1]
                                w   = b[2] - b[0]
                                h   = b[3] - b[1]
                                bl.append([x, y, w, h])
                         
                        bbx = [bl, page_no]
                        if not bl: 
                            bbx = []
                        gid_dict[(row, col)]['bbox_lst']    = bbx
                        chref   = ddict[r_c].get('chref', '')
                        gid_dict[(row, col)]['text_ids'] =  map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), ddict[r_c]['xml_ids'].split('$$')[:]))
                        gid_dict[(row, col)]['ph']    = ''
                        gid_dict[(row, col)]['scale']    = ''
                        gid_dict[(row, col)]['currency']    = ''
                        gid_dict[(row, col)]['value_type']    = ''

                        phcsv_d = {}
                        #print [r_c, gid_dict[(row, col)]['text_ids'], gid_dict[(row, col)]['text_lst'], ddict[r_c]['ldr']]
                        for xid in [ddict[r_c]['xml_ids']]:
                            if xid in xml_d:
                                phinfo, csvinfo = xml_d[xid][0], xml_d[xid][1]
                                #print '\t', xid, phinfo, csvinfo
                                if phinfo.get('Year') and phinfo.get('Period_Type'):
                                    gid_dict[(row, col)]['ph']   = phinfo['Period_Type']+str(phinfo['Year'])
                                if csvinfo.get('Currency'):
                                    gid_dict[(row, col)]['currency']    = csvinfo['Currency']
                                if csvinfo.get('Scale'):
                                    gid_dict[(row, col)]['scale']    = csvinfo['Scale']
                                if csvinfo.get('ValueType'):
                                    gid_dict[(row, col)]['value_type']    = csvinfo['ValueType']
                        stype = ddict[r_c]['ldr']
                        #print stype
                        #if stype == []:continue
                        if stype == 'value': 
                           #if not gid_dict[(row, col)]['text_ids']:
                           #     print [r_c, ddict[r_c]['xml_ids'], ddict[r_c]['data'] , ddict[r_c]['ldr']]
                           #     print "XML ID NOT EXISTS"
                           #     sys.exit()
                           gid_dict[(row, col)]['section_type'] =  'GV'
                        elif stype == 'hch': 
                           gid_dict[(row, col)]['section_type'] =  'HGH'
                        elif stype == 'vch': 
                           gid_dict[(row, col)]['section_type'] =  'VGH'
                        elif stype in ['gh', 'g_header']: 
                           gid_dict[(row, col)]['section_type'] =  'GH'
                        elif stype in ['undefined']:
                           gid_dict[(row, col)]['section_type'] =  ''
                        else:
                           gid_dict[(row, col)]['section_type'] =  ''
                           if 0:#stype.strip():
                              print ' -- ', stype
                              sys.exit()  
                              #gid_dict[(row, col)]['section_type'] =  'GV'
                    if phcsv_data.get(int(page_no), {}).get(int(gid), {}):
                       pass
                       #print (batch, data_elm[0], page_no, gid)
                       #print ' PH Present'
                       #print phcsv_data.get(int(page_no), {}).get(int(gid), {})['x56_1']
                       #sys.exit()     
                    #print "HHHH"
                    #print '*'*100
                    #print gid_dict   
                    #sys.exit()
                    if 0:
                        r_cs = gid_dict.keys()
                        for r_c in r_cs[:0]:
                            r_c_dict = gid_dict[r_c]
                            ref_ars =  r_c_dict.get('text_ids', []) 
                            mj = ''.join(ref_ars).strip()
                            if not mj:
                               new_red = 'x'+str(rcount)+'_'+str(page_no)
                               gid_dict[r_c]['text_ids'] = [ new_red ]
                               rcount = rcount + 1
                    k_gid_dict = {gid:gid_dict}
                    res_error_info = eObj.error_classes(k_gid_dict, gid)
                    if res_error_info:
                        print res_error_info
                        print (data_elm[0], page_no, gid)
                        print 
                    doc_page_grid_dict[(data_elm[0], page_no, gid)] = ('', '', gid_dict, data_elm[4]) #, gid_dict, phcsv_data.get(int(page_no), {}).get(int(gid), {}) ]   
        #print doc_page_grid_dict
        sys.exit()
        if  1:
            for k, vs in doc_page_grid_dict.items():
                   print k
                   print vs[2] 
                   print 
        sh_data_dict    = {company_name:({}, doc_page_grid_dict, doc_phlst, [])}
        #print sh_data_dict     
        #print doc_page_grid_dict
        #print dbname 
        sys.exit() 
        import mp_pdf_add_batch_cp_70_new_project_60_similar_inc_20
        obj = mp_pdf_add_batch_cp_70_new_project_60_similar_inc_20.Company_populate_new()
        obj.get_company_info(sh_data_dict, '20', dbname)
        #call
        return 'Done'

if __name__ == '__main__':
    bObj = BDS_Data()
    company_name = sys.argv[1]
    doc_lst = map(str, sys.argv[2].split('#'))
    print bObj.data_preparation_bds(company_name, doc_lst)
    #print bObj.data_preparation_bds('NestleSA', ['3228', '3229', '3230', '3231', '3233', '3234', '3235', '3236', '3225']) 
    #print bObj.data_preparation_bds('HyundaiEngineeringandConstructionCoLtd', ['3','4', '5', '6', '7', '8', '9', '10', '11', '22'])
    #print bObj.data_preparation_bds('HyundaiEngineeringandConstructionCoLtd', ['6'])
    #print bObj.data_preparation_bds('TataMotorsLimited', ['12', '13', '14' , '15', '16', '17', '18', '19', '20', '21'])
    #print bObj.data_preparation_bds('NestleSA', ['3225'])


