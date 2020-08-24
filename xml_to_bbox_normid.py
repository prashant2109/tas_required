import MySQLdb, os, shelve, lmdb
import collections
class cell_id_bbox:
    def __init__(self):
        pass

    def read_celldict(self,pid, uid, norm_resid):
        path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/'%(pid, uid)
        sh_path = os.path.join(path, norm_resid)
        sh = shelve.open(sh_path+'.sh')
        cell_dict = sh['celldata']
        sh.close()
        return cell_dict[0]


    def get_document_pageno_celldict_beformnorm(self,project_id, url_id, docid, pageno):
        cellpath_beforenorm ='/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/celldata/%s'
        tmp_path=cellpath_beforenorm%(project_id, url_id, docid, pageno)    
        import common.webdatastore as webdatastore
        self.webObj = webdatastore.webdatastore()
        tmpd = self.webObj.read_all_from_lmdb(tmp_path, {})
        d0 = tmpd
        return d0[int(pageno)]['cell_dict']

    def get_org_mdata(self,doc_page_celldict, xml_id_docpage_dict,pageno):
        for cell, vv in doc_page_celldict.items():
            text_ids = vv.get('text_ids',[''])
            for textid in text_ids:
                #print (vv.get('mdata',[]),pageno)
                xml_id_docpage_dict[textid] = [vv, pageno]
        return xml_id_docpage_dict    
            

    def get_unique_page(self, celldict):
        xml_dict = []
        for k,v in celldict.items():
            if not v.get('text_ids',[]):continue
            for i in v['text_ids']:
                    if i:
                        xml_dict.append(i)
        uq_xml_dict = list(set(xml_dict))
        pg_nos = {}
        for i in uq_xml_dict:
            pg_nos[i.split("_")[-1]] = 1

        page_lst = pg_nos.keys()
        return page_lst

    def get_cell_bbox_dict(self, celldict, project_id, url_id,docid, norm_resid=None):
            dealid = project_id+'_'+url_id
            pageno_lst  = self.get_unique_page(celldict)
            xml_id_docpage_dict = {}
            for pageno in pageno_lst:
                doc_page_celldict = self.get_document_pageno_celldict_beformnorm(project_id, url_id, docid, pageno)
                #print doc_page_celldict
                xml_id_docpage_dict = self.get_org_mdata(doc_page_celldict, xml_id_docpage_dict,pageno)
            cell_bbox_dict = {}
            ks  = celldict.keys()
            ks.sort()
            for cell in ks:
                data    = celldict[cell]
                #print data
                #print 'cell', cell
                #if cell!=(4, 2):continue
                text_ids_lst =  data.get('text_ids',[])
                org_ids = text_ids_lst[:]
                org_ids_lst =  data.get('org_text_ids',[])
                txt = ' '.join(data.get('text_lst', [])) 
                #print  '==============================================================='
                #print [cell, txt,org_ids,org_ids_lst]
                #print '-------------------------------------------------------'
                #sys.exit()
                #print 'extra info', data.get('mdata', [])
                #print 'bbox', data.get('bbox_lst', [])
                #print '*'*100
                flg = 0
                #for textid in text_ids_lst:
                bbox_ar = []
                #print "org_ids_lst", org_ids_lst
                if type(org_ids_lst)==int:
                   continue 
                for textid in org_ids_lst:
                    if not textid:
                       continue
                    xml_dict = cell_bbox_dict.get(cell,{})
                    bbox_pageno = xml_id_docpage_dict.get(textid,[])
                    if bbox_pageno:
                        flg = 1
                        #break
                        word_info = bbox_pageno[0]
                        pageno = bbox_pageno[1]
                        word_list = []
                        if not word_info.get('attr',[]):continue    
                        for welm in word_info['attr']:
                            #print welm.keys()
                            #sys.exit()
                            #bbox = welm['txt_bbox']
                            bbox = [float(x) for x in welm['gcoords'].split('_')]
                            #print [bbox ]
                            #bbox = [int(x/2.7) for x in bbox]
                            #bbox = (bbox[0], bbox[1], bbox[2]-bbox[0],bbox[3]-bbox[1])      
                            #x, y, w, h = map(lambda x:x/2.25, bbox)
                            #bbox = [x, y, w, h]
                            word_list.append(bbox)
                            '''
                            wrd_li = welm.get('word_lst',[])
                            for wrd_elm in wrd_li[:]:
                                word_list.append(wrd_elm[2])
                            '''
                        #print textid, word_list
                        if word_list:
                            bbox_ar.append((word_list,int(pageno)))
                
                if not bbox_ar:
                    text_ids_lst = text_ids_lst[:]
                    #print "FOUND", txt, org_ids_lst, text_ids_lst
                    bbox_ar = []
                    if text_ids_lst:
                        for p, textid in enumerate(text_ids_lst):                 
                            if not textid:continue
                            #print textid
                            #xml_dict = cell_bbox_dict.get(cell,{})            
                            bbox_pageno = xml_id_docpage_dict.get(textid,[])
                            #print textid
                             
                            if bbox_pageno:
                                word_info = bbox_pageno[0]
                                pageno = bbox_pageno[1]
                                word_list = []
                                
                                if not word_info.get('attr',[]):continue    
                                for welm in word_info['attr']:
                                    #print welm.keys()
                                    #sys.exit()
                                    #bbox = welm['txt_bbox']
                                    #print welm['gcoords'].split('_')
                                    bbox = [float(x) for x in welm['gcoords'].split('_')]
                                    
                                    #bbox = [int(x/2.7) for x in bbox]
                                    #bbox = (bbox[0], bbox[1], bbox[2]-bbox[0],bbox[3]-bbox[1])      
                                    #x, y, w, h = map(lambda x:x/2.25, bbox)
                                    #bbox = [x, y, w, h]
                                    word_list.append(bbox)
                                    '''
                                    wrd_li = welm.get('word_lst',[])
                                    for wrd_elm in wrd_li[:]:
                                        word_list.append(wrd_elm[2])
                                    '''
                                if word_list:
                                    #cell_bbox_dict[textid] = [word_list,int(pageno)]
                                    bbox_ar.append((word_list,int(pageno)))
                            #cell_bbox_dict[cell] = xml_dict
                if bbox_ar:
                    word_list,pageno    = [], ''
                    pg_d    = {}
                    x  = ''
                    y  = ''
                    x_max  = ''
                    y_max  = ''
                    for tup in bbox_ar:
                        for bboxs in tup[0]:
                            for bbox in [bboxs]:
                                if x == '':
                                    x, y, x_max, y_max  = bbox
                                    x_max   = x+x_max
                                    y_max   = y+y_max
                                else:
                                    x   = min(x, bbox[0])
                                    y   = min(y, bbox[1])
                                    xmax1   = bbox[0]+bbox[2]
                                    ymax1   = bbox[1]+bbox[3]
                                    x_max   = max(x_max, xmax1)
                                    y_max   = max(y_max, ymax1)
                        word_list   += tup[0]
                        pageno  = tup[1]
                        pg_d[pageno]    = 1
                    if len(pg_d.keys()) > 1:
                        print 'Warning More than one page number ',[cell, docid, norm_resid, bbox_ar]
                        #sys.exit()
                    #print 'B', word_list 
                    word_list   = [[x, y, x_max-x, y_max-y]]
                    #print 'A', word_list 
                    for textid in org_ids:
                        #print "HIII",textid 
                        cell_bbox_dict[textid] = [word_list,int(pageno)]

                
            return cell_bbox_dict
  
        
            

    def get_cell_bbox_data(self, project_id, url_id, docid, norm_resid):
         if 1:        
            celldict = self.read_celldict(project_id, url_id, norm_resid)
            bbox_xmldict = self.get_cell_bbox_dict(celldict,project_id, url_id,docid, norm_resid)
            return bbox_xmldict 

if __name__=='__main__':
    fobj = cell_id_bbox()
    #print fobj.get_cell_bbox_data('1','206','12','60706')
    #print fobj.get_cell_bbox_data('1','206','12','57082')
    print fobj.get_cell_bbox_data('1','24','55','2673')
        
