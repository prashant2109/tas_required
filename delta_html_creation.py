import delta_utils, traceback, copy
import MySQLdb
import xlrd
import hashlib
util_obj    = delta_utils.Utils()
import os, sys, shelve, json, time, sets, re
import iframe_template
import gen_delta_html
import binascii
import lmdb
import ConfigParser
import time
import urllib
import hashlib
import datetime
config = ConfigParser.ConfigParser()
config.read('dbConfig.ini')
import multiprocessing
queue = multiprocessing.JoinableQueue()
db_queue = multiprocessing.JoinableQueue()
class Task(object):
    def __init__(t_self, pair, doc_id, i, l):
        t_self.deal_id   = doc_id
        t_self.pair   = pair
        t_self.i        = i
        t_self.l        = l


def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__
    pass


class Index():

    def __init__(self, cfgfile='config.ini'):
        self.__opath    = '/var/www/html/TR_Filings_output/data/output/'
        self.core_count = 10
        pass
 
    def __read_from_shelve(self, fname, default={}):
        if os.path.isfile(fname):
            sh      = shelve.open(fname, 'r')
            data    = sh.get('data', default)
            sh.close()
            return data
        return default

    def __write_to_shelve(self, fname, idata):
        dirname = os.path.dirname(fname)
        if dirname and (not os.path.exists(dirname)):
            os.makedirs(dirname)
        sh = shelve.open(fname, 'n')
        sh['data'] = idata
        sh.close()
        return

    def get_parent_id_tag(self, cid, tag_name, pdict, wdata):
        p_id    = pdict[cid]
        data    = wdata.get(p_id, ())
        while data and (data[1] not in tag_name):
            p_id    = pdict[p_id]
            data    = wdata.get(p_id, ())
        data    = wdata.get(p_id, ())
        if data and (data[1] in tag_name):
            return p_id
        return ''

    def gen_table_txt(self, project_id, user_id, url_id, agent_id, mgmt_id, cid_d, resd):
        docids  = cid_d.keys()
        docids.sort(key=lambda x:int(x))
        for doc_id in docids:
            cidxs       = cid_d[doc_id]
            body_html   = os.path.join(self.__opath, str(project_id), str(url_id), str(agent_id)+'_'+str(mgmt_id), str(user_id), str(doc_id), 'html', '%s_body.html'%(str(doc_id)))
            linegroups, wdata, pdict, cdict, nodeid_cid_map, line_group_dict    = self.get_linegroups(project_id, user_id, url_id, agent_id, mgmt_id, doc_id)
            cidx_txt    = '/tmp/'+str(doc_id)+'.txt' 
            cidx_txt_out= '/tmp/'+str(doc_id)+'_out.txt' 
            fin         = open(cidx_txt, 'w')
            f_table     = 0
            for cidlst in cidxs:
                if str(cidlst[0]) in resd:continue
                for cid in cidlst[1]:
                    cid     = int(cid)
                    c_idx   = nodeid_cid_map.get(cid, '')  
                    #print 'c_idx ',c_idx
                    cust_index  = c_idx[0]
                    d = wdata.get(cust_index, [])
                    if not d: continue
                    table_c_id    = self.get_parent_id_tag(cust_index, ['table'], pdict, wdata)
                    if table_c_id == '':
                        continue
                    table_c_id      = wdata[table_c_id][4]['customindex']
                    f_table = 1
                    fin.write(str(table_c_id)+'\t'+str(cidlst[0])+'\t'+'#'.join(cidlst[1])+'\n');
                    break
            fin.close()
            if f_table == 1:
                cmd = "node clip_html.js  "+' '.join(map(lambda x:'"'+x+'"', [body_html, str(doc_id), cidx_txt, cidx_txt_out]))
                os.system(cmd)
                fout    = open(cidx_txt_out, 'r')
                lines   = fout.readlines()
                fout.close()
                for line in lines:
                    line    = line.strip()
                    if not line:continue
                    line    = line.split(':$:TABLE:$:')
                    resd[line[0]] = line[1]
                os.system("rm -rf "+cidx_txt)
                os.system("rm -rf "+cidx_txt_out)
        return resd

    def get_part_item_scope_lst(self, project_id, user_id, url_id, agent_id, mgmt_id, hyp_prof_id, ref_prof_id):
        addition_hyp_grps, deletion_ref_grps, final_part_item = part_item_obj.get_section_header_scope_only_part_item(url_id, agent_id, mgmt_id)
        part_item_scope_dict_hyp    = {}
        part_item_scope_dict_ref    = {}
        hyp_line_num    = ''
        ref_line_num    = ''
        for part_item_tup in final_part_item:
            hyp_tup = part_item_tup[0]
            ref_tup = part_item_tup[1]
            if (hyp_line_num !='') and (ref_line_num != ''):break
            if (hyp_tup) and (hyp_line_num == ''):
                hyp_line_num    = hyp_tup[0]
            if (ref_tup) and (ref_line_num == ''):
                ref_line_num    = ref_tup[0]
            if (hyp_line_num !='') and (ref_line_num != ''):break
        final_part_item = [((0, hyp_line_num), (0, ref_line_num))]+final_part_item
        for part_item_tup in final_part_item:
            #print '------------------------------------------------------'
            #print part_item_tup
            hyp_tup = part_item_tup[0]
            ref_tup = part_item_tup[1]
            if hyp_tup:
                #print para_wise_dict_hyp[hyp_tup[0]]
                for line_num in range(hyp_tup[0], hyp_tup[1]):
                    part_item_scope_dict_hyp[line_num]  = part_item_tup
            if ref_tup:
                #print para_wise_dict_ref[ref_tup[0]]
                for line_num in range(ref_tup[0], ref_tup[1]):
                    part_item_scope_dict_ref[line_num]  = part_item_tup
        return final_part_item, part_item_scope_dict_hyp, part_item_scope_dict_ref

    def get_final_pairs(self, final_match_arr_tmp, final_part_item, part_item_scope_dict_hyp, part_item_scope_dict_ref):
        part_item_scope_dict        = {}
        for rr in final_match_arr_tmp:
            hyp_arr, ref_arr    = rr
            if hyp_arr and (hyp_arr[0] in part_item_scope_dict_hyp):
                part_item_tup = part_item_scope_dict_hyp[hyp_arr[0]]
                if part_item_tup not in part_item_scope_dict:
                    part_item_scope_dict[part_item_tup] = []
                part_item_scope_dict[part_item_tup].append(rr)
            elif ref_arr and (ref_arr[0] in part_item_scope_dict_ref):
                part_item_tup = part_item_scope_dict_ref[ref_arr[0]]
                if part_item_tup not in part_item_scope_dict:
                    part_item_scope_dict[part_item_tup] = []
                part_item_scope_dict[part_item_tup].append(rr)
        #final_pairs = []
        #for part_item_tup in final_part_item:
        #    final_match_arr_tmp   = part_item_scope_dict[part_item_tup]
        #    #final_match_arr_tmp   = rearrange_obj.rearrange(final_match_arr_tmp)
        #    final_pairs += final_match_arr_tmp
        return part_item_scope_dict

    def get_linegroups(self, project_id, user_id, url_id, agent_id, mgmt_id, doc_id):
        docid_ofname = os.path.join(self.__opath, str(project_id), str(url_id), str(agent_id)+'_'+str(mgmt_id), str(user_id), str(doc_id), 'sh', '%s_pcdata.slv'%(str(doc_id)))
        docid_data = self.__read_from_shelve(docid_ofname, {})
        linegroups = docid_data.get('linegroups', [])
        wdata, pdict, cdict = docid_data.get('webdata', ({}, {}, {}))
        nodeid_cid_map = docid_data.get('nodeid_cid_map', {})
        wdata   = wdata
        #wdata   = self.__remove_navigation_index_and_page_number(wdata,copy.deepcopy(linegroups), doc_id)
        line_group_dict = {}
        for lidx in reversed(range(0, len(linegroups))):
            tmp_ttup = linegroups[lidx]
            lst    = tmp_ttup[1]
            break_flg   = 0
            for cust_index in lst:
                line_group_dict[cust_index] = lidx
        return  linegroups, wdata, pdict, cdict, nodeid_cid_map, line_group_dict

    def combine_two_list_elms(self, list_1, list_2):
        final_arr   = []
        for i in range(max((len(list_1),len(list_2)))):
            while True:
                try:
                    card = (list_1[i],list_2[i])
                except IndexError:
                    if len(list_1)>len(list_2):
                        list_2.append('')
                        card = (list_1[i],list_2[i])
                    elif len(list_1)<len(list_2):
                        list_1.append('')
                        card = (list_1[i], list_2[i])
                    continue
                final_arr.append(card)
                break
        return final_arr

    def delta_display(self, deal_id, doc1, doc2, final_pairs,  css_file, user_delta={}, res_tr={}):
        color_rep_map_dict  = {'BLUE':'rgba(0, 0,255, 0.3)', 'GREEN':'rgba(0, 255,0, 0.3)'}
        meta_data_str       = {}
        #slice_dict  = {}
        #slice_dict['ALL']  = final_pairs

        #item_list   = ['ALL']
        color_dict      = {'@@HEADER@@':'#999', '@@TABLE@@':'#87D1F4', '@@DFD@@':'#DFB295'}#, '@@DFD@@':'#03B5A2', '@@THD@@':'#FFAEAE', '@@FOOTER@@':'#FF6347'}
        subscript_dict  = {'@@HEADER@@':'~~@@HEADER@@', '@@DFD@@':'~~@@DFD@@', '@@THD@@':'~~@@THD@@', '@@FOOTER@@':'~~@@FOOTER@@', '@@TABLE@@':'~~@@TABLE@@'}
        html_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'
        os.system('mkdir -p '+html_path)
        item            = str(doc1.split('_')[-1])+'-'+str(doc2.split('_')[-1])
        new_html        = {}
        if 1:
            final_arr   = []
            #item        = 'ALL_TEST'
            html_str    = ''
            pno, pid = 1, 1
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_'+'table_info'
            #print 'IN ', html_path_lmdb
            env = lmdb.open(html_path_lmdb)
            res_edit_tr = {}
            regen_flag  = ''
            with env.begin() as txn:   
                regen_flag  = txn.get('regen_flag')
                for (tmp_item_txt, final_line_list) in final_pairs[:]:
                    tmp_html_str =  ""
                    tmparr  = []
                    for line_tup in final_line_list:
                        hyp_lines   = '_'.join(map(lambda x:str(x), line_tup[0]))
                        ref_lines   = '_'.join(map(lambda x:str(x), line_tup[1]))
                        tmparr.append(hyp_lines+'-'+ref_lines)
                        user_class  = ''
                        if hyp_lines+'-'+ref_lines in user_delta:
                            user_class  = 'user_match'
                        key_str     = hyp_lines+'-'+ref_lines+'@@'+user_class
                        tmp_tr_html = txn.get(self.__get_quid(key_str))
                        if tmp_tr_html and (regen_flag == 'Y'):
                             tmp_tr_html    = binascii.a2b_hex(tmp_tr_html)
                             if not user_class and (user_class in tmp_tr_html):
                                 pass
                             else:
                                 tmp_html_str    += tmp_tr_html
                                 if hyp_lines+'-'+ref_lines in res_tr:
                                    res_edit_tr[res_tr[hyp_lines+'-'+ref_lines]]    = res_edit_tr.get(res_tr[hyp_lines+'-'+ref_lines], '')+tmp_tr_html
                                 continue

                        p1          = ''
                        p2          = ''
                        txt1        = ''
                        txt2        = ''
                        if hyp_lines:
                            #p1          = '<span class="line_num_span" line_num="%s:">%s</span>'%(hyp_lines, hyp_lines)
                            if hyp_lines+'-'+ref_lines in user_delta:
                                p1          = '<span class="line_num_span" line_num="%s:">%s</span><span class="glyphicon glyphicon-remove-circle pull-right" onclick="unmatch_hyp_ref_line(this, event)"></span>'%(hyp_lines, hyp_lines)
                            else:
                                p1          = '<span class="line_num_span" line_num="%s:">%s</span>'%(hyp_lines, hyp_lines)
                            txt1  = binascii.a2b_hex(txn.get('L-'+hyp_lines))
                        if ref_lines:
                            p2          = '<span class="line_num_span" line_num=":%s">%s</span>'%(ref_lines, ref_lines)
                            txt2  = binascii.a2b_hex(txn.get('R-'+ref_lines))
                        user_class  = ''
                        if hyp_lines+'-'+ref_lines in user_delta:
                            user_class  = 'user_match'
                        tmp_tr_html = '<tr class="%s" pair_str="%s"><td>%s</td><td onclick="select_left_line(this);">%s : </td><td onclick="select_right_line(this);" >%s</td><td>%s</td></tr>'%(user_class,  hyp_lines+'-'+ref_lines, txt1,p1, p2, txt2)
                        new_html[key_str]   = tmp_tr_html
                        if hyp_lines+'-'+ref_lines in res_tr:
                            res_edit_tr[res_tr[hyp_lines+'-'+ref_lines]]    = res_edit_tr.get(res_tr[hyp_lines+'-'+ref_lines], '')+tmp_tr_html
                        tmp_html_str    += tmp_tr_html
                    html_str += iframe_template.header_template %(tmp_item_txt, tmp_item_txt, tmp_item_txt, tmp_html_str)
                    #if not regen_flag:
                    #tmparr  = self.re_order_final_pairs(tmparr, user_delta)
                    #for rr in tmparr:
                    #    print rr
                    #sys.exit()
                    final_arr.append(tmp_item_txt+'$'+'#'.join(tmparr))
            final_pair_str  = '@@'.join(final_arr)
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_table_info'
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:
               txn.put('regen_flag', 'Y')
               for k, v in new_html.items():
                   txn.put(self.__get_quid(k), binascii.b2a_hex(v))
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
            #os.system("rm -rf "+html_path_lmdb)
            #os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
               txn.put('FINAL_PAIRS', final_pair_str)
            os.system("chmod -R 777 "+html_path_lmdb)
            meta_data   = {'deal_id':deal_id, 'doc1':doc1, 'doc2':doc2}
            html_str   = iframe_template.template % (css_file+'<script>var meta_deal_info=%s</script>'%(json.dumps(meta_data)), '', '', '', '', html_str)
            h_file  = open(html_path+item+'.html', 'w')
            h_file.write(html_str)
            h_file.close()
            #print html_path+item+'.html'
            #return html_path+item+'.html'
        os.system('chmod -R 777 '+html_path)
        return res_edit_tr

    def re_order_final_pairs(self, tmparr, user_delta):
        final_delta = {}
        for k, v in user_delta.items():
            k   = k.split('-')
            final_delta[tuple(map(lambda x:int(x), k))]   = v
        #print final_delta
        pages_wise_hype = {}
        pages_wise_ref  = {}
        for pair in tmparr:
            pair    = pair.split('-')
            if pair[0]:
                pages_wise_hype[int(pair[0])] = 1
            if pair[1]:
                pages_wise_ref[int(pair[1])] = 1
        hyp_match_dict = {}
        ref_match_dict = {}
        user_delta      = {}
        for (hline, rline), flag in final_delta.items():
            hyp_match_dict.setdefault(hline, {})[rline] = 1
            user_delta[str(hline)+'-'+str(rline)] = 1
            ref_match_dict.setdefault(rline, {})[hline] = 1
        hyp_lines	= pages_wise_hype.keys()
        ref_lines	= pages_wise_ref.keys()
        hyp_lines.sort()
        ref_lines.sort()
        tmp_final_ar	= []
        tmp_m_dict	= {}
        for hyp_line in hyp_lines:
            if hyp_line in hyp_match_dict:
               reflines = hyp_match_dict[hyp_line].keys()
               for ref_line in reflines:
                   if (hyp_line in ref_match_dict.get(ref_line, {})) and (ref_line in ref_lines):
                       tmp_final_ar.append(([hyp_line], [ref_line]))
                       tmp_m_dict[ref_line]	= hyp_line
                   else:
                       tmp_final_ar.append(([hyp_line], []))
            else:
               tmp_final_ar.append(([hyp_line], []))
        del_lines = list(sets.Set(ref_lines) - sets.Set(tmp_m_dict.keys()))
        del_lines.sort()
        reverse_map_dict_ref_part_item  = {}
        for tmp_ind, elm in enumerate(ref_lines):
              reverse_map_dict_ref_part_item[elm]  = tmp_ind
        tmp_final_ar	= self.rearrange_pairs(ref_lines, del_lines, reverse_map_dict_ref_part_item, tmp_final_ar)
        page_pairs	        = self.re_arrange_final_pair(tmp_final_ar, user_delta)
        tmp_final_ar    = []
        for tt in page_pairs:
            hyp_lines   = '_'.join(map(lambda x:str(x), tt[0]))
            ref_lines   = '_'.join(map(lambda x:str(x), tt[1]))
            tmp_final_ar.append(hyp_lines+'-'+ref_lines)
        return tmp_final_ar
            

    def rearrange_pairs(self, ref_lines, deleted_para_ids, reverse_map_dict_ref, part_item_pairs):
        #print '----------------------------------------'
        #print deleted_para_ids, part_item_pairs, reverse_map_dict_ref
        for ind, tmp_ref_line in enumerate(deleted_para_ids):
            new_ar_ind  = -1
            ref_line	= reverse_map_dict_ref[tmp_ref_line]
            if tmp_ref_line == ref_lines[0]:
                new_ar_ind  = 0
            for ar_ind, tmp_tup in enumerate(part_item_pairs):
                if tmp_tup[1] and ((ref_line - reverse_map_dict_ref[tmp_tup[1][-1]]) == 1):
                    new_ar_ind  = ar_ind + 1
                    continue
                if (new_ar_ind != -1):
                    if(tmp_tup[1]):
                        break
                    new_ar_ind  = ar_ind + 1
            if new_ar_ind   == -1:
                #print 'Ref item index not found ', ref_line, tmp_ref_line
                #sys.exit()
                new_ar_ind  = len(part_item_pairs)
            #if (tmp_ref_line != ref_lines[0]) and (new_ar_ind == 0):
            #    new_ar_ind = 1
            #print tmp_ref_line, ref_line, new_ar_ind, part_item_pairs
            part_item_pairs   = part_item_pairs[:new_ar_ind]+[([], [tmp_ref_line])]+part_item_pairs[new_ar_ind:]
        return part_item_pairs

    def re_arrange_final_pair(self, final_line_list, user_delta):
        final_arr   = []
        tmp_arr_hyp, tmp_arr_ref	= [], []
        ind = 0
        total   = len(final_line_list)
        while (ind < total):
            rr  = final_line_list[ind]
            pair_str    = '_'.join(map(lambda x:str(x), rr[0]))+'-'+'_'.join(map(lambda x:str(x), rr[1]))
            if (rr[0] and rr[1]) or (pair_str in user_delta) or (ind == 1):
                ind += 1
            if (rr[0] and rr[1]) or (pair_str in user_delta) or (ind == 0):
                if (rr[0] and rr[1]) or (pair_str in user_delta):
                    final_arr.append(rr+('EQUAL', ))
                tmp_arr_hyp = []
                tmp_arr_ref = []
                for tmp_rr in final_line_list[ind:]:
                    if tmp_rr[0] and tmp_rr[1]:
                        break
                    pair_str    = '_'.join(map(lambda x:str(x), tmp_rr[0]))+'-'+'_'.join(map(lambda x:str(x), tmp_rr[1]))
                    if pair_str in user_delta:break
                    if tmp_rr[0]:
                        tmp_arr_hyp.append(tmp_rr[0][0])
                    if tmp_rr[1]:
                        tmp_arr_ref.append(tmp_rr[1][0])
                    ind += 1
                tmp_final_pairs = self.combine_two_list_elms(tmp_arr_hyp, tmp_arr_ref)
                for elm in tmp_final_pairs:
                    hyp = []
                    ref = []
                    if elm[0]:
                        hyp = [elm[0]]
                    if elm[1]:
                        ref = [elm[1]]
                    final_arr.append((hyp, ref, 'DIFF'))
                tmp_arr_hyp, tmp_arr_ref	= [], []
        return final_arr

    def __get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def remove_user_delta(self, ijson):
        deal_id              = ijson['deal_id']
        doc1                 = ijson['doc1']
        doc2                 = ijson['doc2']
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        os.system("rm -rf "+html_path_lmdb)
        os.system("mkdir -p "+html_path_lmdb)
        os.system("chmod -R 777 "+html_path_lmdb)
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
        env = lmdb.open(html_path_lmdb)
        with env.begin() as txn:   
           final_pair = txn.get('FINAL_PAIRS')
           final = final_pair.split('@@')
           final_pairs  = []
           res_tr       = {}
           for final_p in final:
               finalp = final_p.split('$')
               final_par = finalp[1].split('#')
               tmparr1   = []
               tmparr2   = []
               for ind, pair in enumerate(final_par):
                   hline, rline = pair.split('-')
                   if hline:
                       tmparr1.append(hline)
                   if rline:
                       tmparr2.append(rline)
               tmparr1.sort(key=lambda x:int(x))
               tmparr2.sort(key=lambda x:int(x))
               page_pairs = self.combine_two_list_elms(tmparr1, tmparr2)
               final_pair = []
               for pair_page in page_pairs:
                    pair_1 = pair_page[0]
                    pair_2 = pair_page[1]
                    if not pair_1:
                       pair_1 = []
                    else:
                       pair_1 = [pair_1]
                    if not pair_2:
                       pair_2 = []
                    else:
                       pair_2 = [pair_2]
                    final_pair.append((pair_1, pair_2))
               final_pairs.append((finalp[0], final_pair))
        final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1.split('_')[-1])+'_'+str(doc2.split('_')[-1])+'.css'
        t_res_tr  = self.delta_display(deal_id, doc1, doc2, final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">', {},{})

    def unmatch_user_delta(self, ijson):
        deal_id              = ijson['deal_id']
        doc1                 = ijson['doc1']
        doc2                 = ijson['doc2']
        hyp_line             = ijson['hyp_line']
        ref_line             = ijson['ref_line']
        item                 = ijson.get('item', 'ALL')
        hyp_line             = str(hyp_line).strip(':')
        ref_line             = str(ref_line).strip(':')
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        
        env = lmdb.open(html_path_lmdb)
        user_delta_info = {}
        with env.begin() as txn:   
            for k, v in txn.cursor():
                if v == 'Y':
                    user_delta_info[k] = v
        if hyp_line+'-'+ref_line in user_delta_info:
            del user_delta_info[hyp_line+'-'+ref_line]
        os.system("chmod -R 777 "+ html_path_lmdb)
        final_res_tr    = {}    
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
        env = lmdb.open(html_path_lmdb)
        with env.begin() as txn:   
           final_pair = txn.get('FINAL_PAIRS')
           final = final_pair.split('@@')
           final_pairs  = []
           res_tr       = {}
           for final_p in final:
               finalp = final_p.split('$')
               if finalp[0] != item:continue
               final_par = finalp[1].split('#')
               tmparr   = []
               hindex   = 0
               rindex   = 0
               h_d      = {}
               r_d      = {}
               inds     = {}
               pair_ind = {}
               for ind, pair in enumerate(final_par):
                   hline, rline = pair.split('-')
                   h_d.setdefault(hline, {})[ind]   = pair
                   r_d.setdefault(rline, {})[ind]   = pair
                   inds[ind]    = [pair]
                   pair_ind[pair] = ind
 
               edit_inds    = {}
               if hyp_line+'-'+ref_line in pair_ind:
                   inds_copy    = copy.deepcopy(inds)
                   hinds    = h_d[hyp_line].keys()
                   rinds    = r_d[ref_line].keys()
                   hinds.sort()
                   rinds.sort()
                   l_hinds  = len(hinds)
                   l_rinds  = len(rinds)
                   if l_hinds == 1 and l_rinds == 1:
                       edit_inds[hinds[0]]    = []
                   elif l_hinds == 1 and l_rinds > 1:
                       hind = hinds[0]
                       inds[hind] = [hyp_line+'-']
                       edit_inds[hind]    = []
                   elif l_hinds > 1 and l_rinds == 1:
                       rind = rinds[0]
                       inds[rind] = ['-'+ref_line]
                       edit_inds[rind]    = []
                   elif l_hinds > 1 and l_rinds > 1:
                       hind = pair_ind[hyp_line+'-'+ref_line]
                       inds[hind] = ['-']
                       edit_inds[hind]    = []
               all_inds     = inds.keys()
               all_inds.sort()
               tmparr   = []
               for ind in all_inds:
                   pairs    = inds[ind]
                   for pair in pairs:
                       if ind in edit_inds:
                           res_tr[pair] = ind
                       pair = pair.split('-')
                       if pair[0] and pair[1]:
                           tmparr.append((pair[0].split('_'), pair[1].split('_')))
                       elif pair[0]:
                           tmparr.append((pair[0].split('_'), []))
                       elif pair[1]:
                           tmparr.append(([], pair[1].split('_')))
                       else:
                           final_res_tr[ind]  = ''
               final_pairs.append((finalp[0], tmparr))
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
        #user_delta_info[hyp_line+'-'+ref_line] = 1
        with env.begin(write=True) as txn:   
           txn.put(hyp_line+'-'+ref_line, 'N')
        final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1.split('_')[-1])+'_'+str(doc2.split('_')[-1])+'.css'
        t_res_tr  = self.delta_display(deal_id, doc1, doc2, final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">', user_delta_info, res_tr)
        final_res_tr.update(t_res_tr)
        all_inds    = final_res_tr.keys()
        all_inds.sort()
        pair_ar = []
        pair_d  = {}
        for ind in all_inds:
            pair_ar.append(inds_copy[ind][0])
            pair_d[inds_copy[ind][0]] = final_res_tr[ind]
        return [pair_d, pair_ar]

    def get_user_delta_info(self, deal_id, doc1, doc2):
        html_path_lmdb  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1+'_'+doc2+'_user_delta_info'
        env             = lmdb.open(html_path_lmdb)
        user_delta_info = {}
        with env.begin() as txn:   
            for k, v in txn.cursor():
                if v == 'Y':
                    user_delta_info[k] = v
        return user_delta_info 

    def read_table_info(self, deal_id, doc1, doc2):
        final_pairs         = self.get_user_delta_info(deal_id, doc1, doc2)
        html_path_lmdb      = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1+'_'+doc2+'_'+'table_info'
        #print html_path_lmdb     
        env                 = lmdb.open(html_path_lmdb)
        final_arr           = []
        i_file              = "/tmp/"+str(deal_id[0])+'_'+str(deal_id[1])+'_'+doc1+'_'+doc2+'_'+'table_info.txt'
        fin                 = open(i_file, 'wb')
        xml_id              = {}
        with env.begin() as txn:   
            for tt in final_pairs.keys():
                hyp_ref_sp      = tt.split('-')
                hyp_lines       = hyp_ref_sp[0]
                ref_lines       = hyp_ref_sp[1]
                #print hyp_lines 
                #print ref_lines  
                txt1            = binascii.a2b_hex(txn.get('L-'+hyp_lines))
                txt2            = binascii.a2b_hex(txn.get('R-'+ref_lines))
                if 'XML_REF=' in txt1:
                    a1     = txt1.split('XML_REF="')[1].split('"')[0].split('@@')[1].split('#')
                    a2     = txt2.split('XML_REF="')[1].split('"')[0].split('@@')[1].split('#')
                    xml_id['L-'+hyp_lines]  = a1
                    xml_id['R-'+ref_lines]  = a2
                else:
                    fin.write('L-'+hyp_lines+'\t'+txt1+'\n')
                    fin.write('R-'+ref_lines+'\t'+txt2+'\n')
            fin.close()
            cmd = "node --max-old-space-size=8192 read_info_span.js %s"%(i_file)
            res = os.popen(cmd).read().strip()
            tmpxml_id   = {}
            try:
                  tmpxml_id   = json.loads(res)
            except:pass
            os.system("rm -rf "+i_file)
            for tt in final_pairs.keys():
                hyp_ref_sp      = tt.split('-')
                hyp_lines       = hyp_ref_sp[0]
                ref_lines       = hyp_ref_sp[1]
                if 'L-'+hyp_lines in xml_id:
                    a1  = xml_id['L-'+hyp_lines]
                    a2  = xml_id['R-'+ref_lines]
                    final_arr.append((a1, a2))
                elif ('L-'+hyp_lines in tmpxml_id) and ('R-'+ref_lines in tmpxml_id ):
                    a1  = tmpxml_id['L-'+hyp_lines]
                    a2  = tmpxml_id['R-'+ref_lines]
                    final_arr.append((a1, a2))
        return final_arr

    def get_done_deal_info(self, cmp_deal):
        deal_id     = cmp_deal.split('_')
        done_deals  = self.get_done_user_delta(cmp_deal)
        final_dict  = {}
        for page_doc in done_deals.get(cmp_deal, {}).keys():
             doc_page_sp = page_doc.split('-')
             doc1        = doc_page_sp[0]
             doc2        = doc_page_sp[1]
             final_arr   = self.read_table_info(deal_id, doc1, doc2)
             if final_arr:
                 final_dict[(str(doc1), str(doc2))]  = final_arr

        return final_dict

    def re_order_delta_lines(self, ijson):
        deal_id              = ijson['deal_id']
        doc1                 = ijson['doc1']
        doc2                 = ijson['doc2']
        item                 = ijson.get('item', 'ALL')
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        env = lmdb.open(html_path_lmdb)
        user_delta_info = {}
        with env.begin() as txn:   
            for k, v in txn.cursor():
                if v == 'Y':
                    user_delta_info[k] = v
        os.system("chmod -R 777 "+ html_path_lmdb)
        final_res_tr    = {}    
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
        env = lmdb.open(html_path_lmdb)
        final_arr   = []
        final_pairs  = []
        with env.begin() as txn:   
           final_pair = txn.get('FINAL_PAIRS')
           final = final_pair.split('@@')
           res_tr       = {}
           for final_p in final:
               finalp = final_p.split('$')
               if finalp[0] != item:continue
               final_par = finalp[1].split('#')
               tmparr  = self.re_order_final_pairs(final_par, user_delta_info)
               final_arr.append(finalp[0]+'$'+'#'.join(tmparr))
               tmplst   = []
               for pair in tmparr:
                   pair = pair.split('-')
                   if pair[0] and pair[1]:
                       tmplst.append((pair[0].split('_'), pair[1].split('_')))
                   elif pair[0]:
                       tmplst.append((pair[0].split('_'), []))
                   elif pair[1]:
                       tmplst.append(([], pair[1].split('_')))
               final_pairs.append((finalp[0], tmplst))
        final_pair_str  = '@@'.join(final_arr)
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
        env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn:   
           txn.put('FINAL_PAIRS', final_pair_str)
        final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1.split('_')[-1])+'_'+str(doc2.split('_')[-1])+'.css'
        self.delta_display(deal_id, doc1, doc2, final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">', user_delta_info)
        return ['Done']


    def get_delta_user_info(self, ijson):
        deal_id              = ijson['deal_id']
        doc1                 = ijson['doc1']
        doc2                 = ijson['doc2']
        hyp_line             = ijson['hyp_line']
        ref_line             = ijson['ref_line']
        item                 = ijson.get('item', 'ALL')
        hyp_line             = str(hyp_line).strip(':')
        ref_line             = str(ref_line).strip(':')
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        
        env = lmdb.open(html_path_lmdb)
        user_delta_info = {}
        with env.begin() as txn:   
            for k, v in txn.cursor():
                if v == 'Y':
                    user_delta_info[k] = v
        os.system("chmod -R 777 "+ html_path_lmdb)
        final_res_tr    = {}    
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_final_pairs'
        env = lmdb.open(html_path_lmdb)
        with env.begin() as txn:   
           final_pair = txn.get('FINAL_PAIRS')
           final = final_pair.split('@@')
           final_pairs  = []
           res_tr       = {}
           for final_p in final:
               finalp = final_p.split('$')
               if finalp[0] != item:continue
               final_par = finalp[1].split('#')
               tmparr   = []
               hindex   = 0
               rindex   = 0
               h_d      = {}
               r_d      = {}
               inds     = {}
               for ind, pair in enumerate(final_par):
                   hline, rline = pair.split('-')
                   h_d.setdefault(hline, {})[ind]   = pair
                   r_d.setdefault(rline, {})[ind]   = pair
                   inds[ind]    = [pair]
               inds_copy    = copy.deepcopy(inds)
               hinds    = h_d[hyp_line].keys()
               rinds    = r_d[ref_line].keys()
               hinds.sort()
               rinds.sort()
               hind = filter(lambda x:h_d[hyp_line][x] not in user_delta_info, hinds)
               if not hind:
                  hind  = hinds[:1]
               rind = filter(lambda x:r_d[ref_line][x] not in user_delta_info, rinds)
               if not rind:
                  rind  = rinds[:1]
               hind = hind[0]
               rind = rind[0]
               user_delta_info[hyp_line+'-'+ref_line]   = 1
               edit_inds    = {}
               if hind == rind:
                   edit_inds[hind]    = []
               elif hind != rind:
                   if(h_d[hyp_line][hind] not in user_delta_info) and (r_d[ref_line][rind] not in user_delta_info):
                         inds[hind] = [hyp_line+'-'+ref_line, '-'+h_d[hyp_line][hind].split('-')[1]]
                         inds[rind] = [r_d[ref_line][rind].split('-')[0]+'-']
                         edit_inds[hind]    = []
                         edit_inds[rind]    = []
                   elif(r_d[ref_line][rind] not in user_delta_info):
                         inds[hind] = [inds[hind][0], hyp_line+'-'+ref_line]
                         inds[rind] = [r_d[ref_line][rind].split('-')[0]+'-']
                         edit_inds[hind]    = []
                         edit_inds[rind]    = []
                   elif(h_d[hyp_line][hind] not in user_delta_info):
                         inds[hind] = [hyp_line+'-'+ref_line, '-'+h_d[hyp_line][hind].split('-')[1]]
                         edit_inds[hind]    = []
                   else:
                       edit_inds[hind]    = []
                       edit_inds[rind]    = []
               all_inds     = inds.keys()
               all_inds.sort()
               tmparr   = []
               for ind in all_inds:
                   pairs    = inds[ind]
                   for pair in pairs:
                       if ind in edit_inds:
                           res_tr[pair] = ind
                       pair = pair.split('-')
                       if pair[0] and pair[1]:
                           tmparr.append((pair[0].split('_'), pair[1].split('_')))
                       elif pair[0]:
                           tmparr.append((pair[0].split('_'), []))
                       elif pair[1]:
                           tmparr.append(([], pair[1].split('_')))
                       else:
                           final_res_tr[ind]  = ''
               final_pairs.append((finalp[0], tmparr))
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+doc1.split('_')[-1]+'_'+doc2.split('_')[-1]+'_user_delta_info'
        env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
        user_delta_info[hyp_line+'-'+ref_line] = 1
        with env.begin(write=True) as txn:   
           txn.put(hyp_line+'-'+ref_line, 'Y')
        final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1.split('_')[-1])+'_'+str(doc2.split('_')[-1])+'.css'
        #print res_tr
        t_res_tr  = self.delta_display(deal_id, doc1, doc2, final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">', user_delta_info, res_tr)
        final_res_tr.update(t_res_tr)
        all_inds    = final_res_tr.keys()
        all_inds.sort()
        pair_ar = []
        pair_d  = {}
        for ind in all_inds:
            #print ind, inds_copy[ind]
            pair_ar.append(inds_copy[ind][0])
            pair_d[inds_copy[ind][0]] = final_res_tr[ind]
        return [pair_d, pair_ar]

    def create_connection(self):
        database    = 'fundamental_delta'
        dbc =("172.16.20.122","root","tas123",database)
        db = MySQLdb.connect(dbc[0],dbc[1],dbc[2],dbc[3])
        cur = db.cursor()
        return db, cur 

    
    def get_connection(self):
        data = config.get('login_database','value')
        khost, kpasswd, kuser, kdb = data.split('##')
        conn = MySQLdb.connect(khost, kuser, kpasswd, kdb)
        cur = conn.cursor()
        return conn, cur
    def process_delta_multi(self, deal_ids):
        pass
    def print_exception(self):
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line

    def process_delta_user(self, ijson):
        pair_arr       = ijson['pairs']
        pairs    = []
        input_docs  = []
        for pair in pair_arr:
            doc_id = pair['doc_id']    
            input_docs.append(str(doc_id))
            doc_name = pair['company_name']
            pairs.append((doc_name, str(doc_id)))
        self.core_count = 2
        pairs  = [pairs]
        disableprint()
        deal_id     = ijson['deal_id']
        inp_deal_id = deal_id
        user_log    = {} #self.get_done_user_delta(deal_id)
        project_id  = 4
        deal_id     = tuple(deal_id.split('_'))
        outfile     = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'
        os.system("mkdir -p "+outfile)
        outfile     = '/var/www/html/output_file/css'
        os.system("mkdir -p "+outfile)
        import read_slt_info
        self.upObj  = read_slt_info.update(deal_id)
        self.doc_d  = {}
        import read_norm_cell_data as Slt_normdata
        sObj = Slt_normdata.Slt_normdata()
        norm_res_list = sObj.slt_normresids(deal_id[0], deal_id[1])
        #for i in self.upObj.query1():
        #    if not i[1]:continue
        #    self.doc_d.setdefault(int(i[0]), {})[int(i[1])] = 1
        for rr in norm_res_list:
              doc_id, page_number, norm_table_id    = rr
              doc_id        = int(doc_id)
              if str(doc_id) not in input_docs:continue
              page_number   = int(page_number)
              self.doc_d.setdefault(doc_id, {}).setdefault(page_number, {})[norm_table_id] = 1
        def ProcessHandler(thisObj,cpucore):
            import os
            import gc

            os.system("taskset -c -p %d %d" % (cpucore,os.getpid()))
            while 1:
                if not  queue.empty():
                    item = queue.get()
                    if item == 'STOP':
                        break
                    try:
                           print 'Running DELTA ', item.i, ' / ',item.l
                           thisObj.index_table_data(item.pair, item.deal_id)
                           #thisObj.create_doc_data(item.pair, item.deal_id)
                           #thisObj.create_doc_html(item.pair, item.deal_id)
                    except:
                        self.print_exception()
                else:
                    time.sleep(2)
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
            self.procs[-1].daemon = True
            self.procs[-1].start()
        pairs   = pairs[::-1][:]
        l   = len(self.doc_d.keys())
        i = 0
        done_d  = {}
        for pp, doc1 in enumerate(self.doc_d.keys()):
            #if pair != (('TheChildrensPlaceInc_8K_Q12013', '1'), ('TheChildrensPlaceInc_8K_Q22013', '6')):continue
            #doc1    = int(pair[0][1])
            #doc2    = int(pair[1][1])
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
            inp_files   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_match_ids.txt'
            if (os.path.exists(table_path)) and os.path.exists(inp_files):
                env = lmdb.open(table_path)
                with env.begin() as txn:   
                    if txn.get('ALL_KEYS'):continue
            if doc1 not in done_d:
                i +=1
                task = Task(doc1, deal_id, i, l)
                queue.put(task)
                done_d[doc1]    = 1
        for i in range(self.core_count):
           queue.put('STOP')
        while 1:
         alive_count = 0
         for i in range(self.core_count):
           if self.procs[i].is_alive():
                alive_count += 1
         if alive_count == 0 :
            break;
        if 1:
            pairs   = pairs[::-1][:]
            for pp, pair in enumerate(pairs):
                print 'Running PAIR ', pp, ' / ', len(pairs), pair
                doc1    = int(pair[0][1])
                doc2    = int(pair[1][1])
                #if (str(doc1)+'-'+str(doc2) in user_log.get(inp_deal_id, {})):
                #    print 'DONE DOC ', pair
                #    continue
                #if pair != (('TheChildrensPlaceInc_8K_Q12013', '1'), ('TheChildrensPlaceInc_8K_Q22013', '6')):continue
                #print '<hr>FIND DELTA</hr>'
                final_pp = self.find_delta_normalized_table(pair, deal_id)
                #final_pp = self.find_delta_new(pair, deal_id)
                if final_pp:
                   return final_pp
            os.system("chmod -R 777 "+ '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1]))
            for doc1, pns in self.doc_d.items():
                output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
                os.system("rm -rf "+output_file_name_css)
                for page_no in pns:    
                    filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(page_no)+'.txt'
                    os.system("rm -rf "+filename)
            
            #return self.get_company_deal_info() 
        enableprint()
        return self.get_company_deal_info() 
        os.system("chmod -R 777 "+ '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1]))

    def process_delta(self, deal_id, input_docs):
        import read_norm_cell_data as Slt_normdata
        sObj = Slt_normdata.Slt_normdata()
        inp_deal_id = deal_id
        user_log    = self.get_done_user_delta(deal_id)
        project_id  = 4
        deal_id = tuple(deal_id.split('_'))
        outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'
        os.system("mkdir -p "+outfile)
        outfile = '/var/www/html/output_file/css'
        os.system("mkdir -p "+outfile)
        norm_res_list = sObj.slt_normresids(deal_id[0], deal_id[1])
        import read_slt_info
        self.upObj = read_slt_info.update(deal_id)
        docname_docid = self.upObj.get_documentName_id()
        print docname_docid
        docinfo_d   = {}
        for docinfo in docname_docid:
            filing  = docinfo[0].split('_')[-1]
            docid   = docinfo[1]
            year    = ''
            ftype   = ''
            if len(filing) == 4:
                try:
                    tmpxx   = int(filing)
                    filing  = docinfo[0].split('_')[-2]+filing
                except:pass
            if 'AR' in filing:
                ftype   = 'A'
                year    = int(filing[-4:].strip())
                docinfo_d.setdefault(ftype, {}).setdefault(year, []).append(docinfo)
            elif 'Q' in filing:
                ftype   = 'Q'
                q       = filing[:2]
                year    = int(filing[-4:].strip())
                docinfo_d.setdefault(ftype, {}).setdefault((q, year), []).append(docinfo)
            elif 'H' in filing:
                ftype   = 'H'
                q       = filing[:2]
                year    = int(filing[-4:].strip())
                docinfo_d.setdefault(ftype, {}).setdefault((q, year), []).append(docinfo)
            if year and ftype:
                year    = int(filing[-4:].strip())
            elif len(docinfo[0].split('_')) > 2 :
                filing  = docinfo[0].split('_')[-2]
                year    = ''
                ftype   = ''
                if 'AR' in filing:
                    ftype   = 'A'
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault(ftype, {}).setdefault(year, []).append(docinfo)
                elif 'Q' in filing:
                    ftype   = 'Q'
                    q       = filing[:2]
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault(ftype, {}).setdefault((q, year), []).append(docinfo)
                elif 'H' in filing:
                    ftype   = 'H'
                    q       = filing[:2]
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault(ftype, {}).setdefault((q, year), []).append(docinfo)
        pairs    = []
        af  = docinfo_d.get('A', {})
        ks  = af.keys()
        ks.sort()
        don_year    = {}
        for k in ks:
            nyear   = k+1
            if nyear in af:
                page_pairs = self.all_pos_pair_elms(af[nyear], af[k])
                for rr in page_pairs:
                    if rr[0] and rr[1]:
                         pairs.append(rr)
        
        af  = docinfo_d.get('Q', {})
        ks  = af.keys()
        ks.sort()
        don_year    = {}
        for k in ks:
            if k[0] == 'Q1':
                nyear   = ('Q1', k[1]+1)
            elif k[0] == 'Q2':
                nyear   = ('Q2', k[1]+1)
            elif k[0] == 'Q3':
                nyear   = ('Q3', k[1]+1)
            elif k[0] == 'Q4':
                nyear   = ('Q4', k[1]+1)
            if nyear in af:
                #pairs.append((af[k], af[nyear]))
                page_pairs = self.all_pos_pair_elms(af[nyear], af[k])
                for rr in page_pairs:
                    if rr[0] and rr[1]:
                         pairs.append(rr)
        af  = docinfo_d.get('H', {})
        ks  = af.keys()
        ks.sort()
        don_year    = {}
        for k in ks:
            if k[0] == 'H1':
                nyear   = ('H1', k[1]+1)
            elif k[0] == 'H2':
                nyear   = ('H2', k[1]+1)
            if nyear in af:
                #pairs.append((af[k], af[nyear]))
                page_pairs = self.all_pos_pair_elms(af[nyear], af[k])
                for rr in page_pairs:
                    if rr[0] and rr[1]:
                         pairs.append(rr)
        self.doc_d   = {}
        #for i in self.upObj.query1():
        #    if not i[1]:continue
        #    self.doc_d.setdefault(int(i[0]), {})[int(i[1])] = 1
        for rr in norm_res_list:
              doc_id, page_number, norm_table_id    = rr
              doc_id        = int(doc_id)
              page_number   = int(page_number)
              self.doc_d.setdefault(doc_id, {}).setdefault(page_number, {})[norm_table_id] = 1
              #if doc_id != 10:continue
              #if page_number != 7:continue
              #ids, html_str   = self.read_normalized_table_ids(deal_id[0], deal_id[1], ('', '', norm_table_id))
              #print norm_table_id, 'ID ', ids
              
              #print '<hr>'            
              #print html_str
              #print '<hr>'            

        #for k, v in self.doc_d.items():
        #    print k, len(v.keys()), v.keys()
        #sys.exit()
        def ProcessHandler(thisObj,cpucore):
            import os
            import gc

            os.system("taskset -c -p %d %d" % (cpucore,os.getpid()))
            while 1:
                if not  queue.empty():
                    item = queue.get()
                    if item == 'STOP':
                        break
                    try:
                           print 'Running DELTA ', item.i, ' / ',item.l
                           thisObj.index_table_data(item.pair, item.deal_id)
                           #thisObj.create_doc_data(item.pair, item.deal_id)
                           #thisObj.create_doc_html(item.pair, item.deal_id)
                    except:
                        self.print_exception()
                else:
                    time.sleep(2)
        #for pp, pair in enumerate(pairs):
        #    print 'Running ', pp, ' / ', len(pairs), pair
        #    doc1    = int(pair[0][1])
        #    doc2    = int(pair[1][1])
        #    if (str(doc1)+'-'+str(doc2) in user_log.get(inp_deal_id, {})):
        #        print 'DONE DOC ', pair
        #sys.exit()
        #for pp, pair in enumerate(pairs):
        #    print pair
        #sys.exit()
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
            self.procs[-1].daemon = True
            self.procs[-1].start()
        pairs   = pairs[::-1][:]
        l   = len(self.doc_d.keys())
        i = 0
        done_d  = {}
        for pp, doc1 in enumerate(self.doc_d.keys()):
            #if pair != (('TheChildrensPlaceInc_8K_Q12013', '1'), ('TheChildrensPlaceInc_8K_Q22013', '6')):continue
            #doc1    = int(pair[0][1])
            #doc2    = int(pair[1][1])
            #if str(doc1) not in ['2','1']:continue
            if input_docs and (str(doc1) not in input_docs):continue
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
            inp_files   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_match_ids.txt'
            if (os.path.exists(table_path)) and os.path.exists(inp_files) and (not input_docs):
                env = lmdb.open(table_path)
                with env.begin() as txn:   
                    if txn.get('ALL_KEYS'):continue
            if doc1 not in done_d: 
                i +=1
                task = Task(doc1, deal_id, i, l)
                queue.put(task)
                done_d[doc1]    = 1
            #table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc2)+'_table_info'
            #if doc2 not in done_d and (not os.path.exists(table_path)):
            #    i +=1
            #    task = Task(doc2, deal_id, i, l)
            #    queue.put(task)
            #    done_d[doc2]    = 1
        for i in range(self.core_count):
           queue.put('STOP')
        while 1:
         alive_count = 0
         for i in range(self.core_count):
           if self.procs[i].is_alive():
                alive_count += 1
         if alive_count == 0 :
            break;
        def ProcessHandler(thisObj,cpucore):
            import os
            import gc

            os.system("taskset -c -p %d %d" % (cpucore,os.getpid()))
            while 1:
                if not  queue.empty():
                    item = queue.get()
                    if item == 'STOP':
                        break
                    try:
                           print 'Running DELTA ', item.i, ' / ',item.l
                           thisObj.find_delta_normalized_table(item.pair, item.deal_id)
                           #thisObj.create_doc_data(item.pair, item.deal_id)
                           #thisObj.create_doc_html(item.pair, item.deal_id)
                    except:
                        self.print_exception()
                else:
                    time.sleep(2)
        #for pp, pair in enumerate(pairs):
        #    print 'Running ', pp, ' / ', len(pairs), pair
        #    doc1    = int(pair[0][1])
        #    doc2    = int(pair[1][1])
        #    if (str(doc1)+'-'+str(doc2) in user_log.get(inp_deal_id, {})):
        #        print 'DONE DOC ', pair
        #sys.exit()
        #for pp, pair in enumerate(pairs):
        #    print pair
        #sys.exit()
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
            self.procs[-1].daemon = True
            self.procs[-1].start()
        if 1:
            pairs   = pairs[::-1][:]
            l   = len(pairs)
            i = 0
            print 'DELTA START ....',len(pairs)
            
            for pp, pair in enumerate(pairs):
                print 'Running PAIR ', pp, ' / ', len(pairs), pair
                doc1    = int(pair[0][1])
                doc2    = int(pair[1][1])
                #if str(doc1)+'-'+str(doc2) not in ['2-1']:continue
                if input_docs and ((str(doc1) not in input_docs) or (str(doc2) not in input_docs)) :continue
                if (str(doc1)+'-'+str(doc2) in user_log.get(inp_deal_id, {})):
                    print 'DONE DOC ', pair
                    if input_docs:
                        if (str(doc1) not in input_docs) or (str(doc2) not in input_docs) :continue
                    else:
                         continue
                #if pair != (('TheChildrensPlaceInc_8K_Q12013', '1'), ('TheChildrensPlaceInc_8K_Q22013', '6')):continue
                #self.find_delta_new(pair, deal_id)
                i +=1
                task = Task(pair, deal_id, i, l)
                queue.put(task)
        for i in range(self.core_count):
           queue.put('STOP')
        while 1:
         alive_count = 0
         for i in range(self.core_count):
           if self.procs[i].is_alive():
                alive_count += 1
         if alive_count == 0 :
            break;
            #return
        for doc1, pns in self.doc_d.items():
            #output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
            #os.system("rm -rf "+output_file_name_css)
            for page_no in pns:    
                filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(page_no)+'.txt'
                os.system("rm -rf "+filename)
            
        os.system("chmod -R 777 "+ '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1]))
        return ['Done']

    def all_pos_pair_elms(self, l1, l2):
        final_pairs = []
        for i in l1:
            for j in l2:
                final_pairs.append((i, j))
        return final_pairs

    def create_doc_html(self, doc1, deal_id):
        i1       = self.doc_d.get(doc1, {})
        i1      = i1.keys()
        i1.sort()
        final_page_doc  = []
        fin_lst = []
        for page_no in i1:
                path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc1, page_no)
                fin_lst.append(str(page_no)+'\t'+path_celldict)
                final_page_doc.append((doc1, page_no, deal_id))
        inp_files   = '/tmp/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'.txt'
        fin = open(inp_files, 'wb')
        fin.write('\n'.join(fin_lst))
        fin.close()
        outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'+str(doc1)+'.html'
        print "node join_html.js %s %s"%(inp_files, outfile)
        os.system("node join_html.js %s %s"%(inp_files, outfile))
        #os.system("rm -rf "+inp_files)

    def read_norm_table_info(self, doc_id, page_no, deal_id, norm_table_d):
        env = lmdb.open('/var/www/html/TASFundamentalsV2/data/output/%s/%s/1_1/21/rdata/APP_RESULT_GC/%s_%s/'%(deal_id[0],deal_id[1],doc_id, page_no))
        path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc_id, page_no)
        db_file_name    = '/tmp/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc_id)+'_'+str(page_no)+'.txt'
        fin = open(db_file_name, 'w')
        found_table = 0
        m_ids   = {}
        with env.begin() as txn:
            for k, v in txn.cursor():
                v = eval(v)
                opcombo_list = v['data'][7]
                ids = []
                for cnt, data in enumerate(opcombo_list):  
                    ids += data[1][4]
                tmp_norm_id = 'NEW TABLE'
                html_str    = ''
                if ids:
                    found_table = 1
                    for tableid, norm_id in norm_table_d.items():
                         if sets.Set(tableid).intersection(sets.Set(ids)):
                             tmp_norm_id, html_str = norm_id
                             m_ids[tmp_norm_id]   = 1
                             break
                ides   = '#'.join(ids)
                fin.write(str(tmp_norm_id)+'\t'+ides+'\t'+html_str+'\n')
        fin.close()
        if found_table == 1:
            output_file_name = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc_id)+'_'+str(page_no)+'.txt'
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc_id)+'.css'
            if not os.path.exists(output_file_name_css):
                os.system("touch "+output_file_name_css)
            cmd = "node clip_html_id.js '%s' '%s' '%s' '%s' %s %s"%(path_celldict, db_file_name, output_file_name, output_file_name_css, doc_id, page_no)
            os.system(cmd)
        os.system("rm -rf "+db_file_name)
        return m_ids

    def index_table_data(self, doc1, deal_id):
        output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
        os.system("rm -rf "+output_file_name_css)
        i1       = self.doc_d.get(doc1, {})
        i1      = i1.keys()
        i1.sort()
        final_page_doc  = []
        fin_lst = []
        for page_no in i1:
                path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc1, page_no)
                fin_lst.append(str(page_no)+'\t'+path_celldict)
                final_page_doc.append((doc1, page_no, deal_id))
        inp_files   = '/tmp/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'.txt'
        fin = open(inp_files, 'wb')
        fin.write('\n'.join(fin_lst))
        fin.close()
        outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'+str(doc1)+'.html'
        os.system("node join_html.js %s %s"%(inp_files, outfile))
        os.system("rm -rf "+inp_files)
        match_ids   = {}
        txt1_d      = {}
        p1      = 1
        for p, page_no in enumerate(final_page_doc):
            #print '\tRunning ',p, ' / ', len(final_page_doc), ' ( ', pp, ' / ', len(pairs), pair, ')', page_no[:2]
            print '\tRunning ',p, ' / ', len(final_page_doc),  page_no[:2]
            doc1, page_no, deal_id  = page_no
            norm_ids    = self.doc_d.get(doc1, {}).get(page_no, {}).keys()
            norm_ids.sort()
            norm_table_d    = {}
            print '\t\tnorm_ids', norm_ids
            for norm_id in norm_ids:
                ids, html_str   = self.read_normalized_table_ids(deal_id[0], deal_id[1], ('', '', norm_id))
                if ids:
                     #norm_table_d[tuple(ids)]    = (norm_id, html_str)
                     match_ids[norm_id]             = 1
                     txt1_d[str(p1)]  = str(norm_id)+'::TASDELIM::'+html_str+'::TASDELIM::'
                     p1 += 1
            #if norm_table_d:
            #    print '\t\tMAP ',len(norm_table_d.keys())
            #    norm_ids    = self.read_norm_table_info(doc1, page_no, deal_id, norm_table_d)
            #    match_ids.update(norm_ids)
            #    #break
        inp_files   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_match_ids.txt'
        fin = open(inp_files, 'wb')
        fin.write('\n'.join(match_ids.keys()))
        fin.close()
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
        os.system("rm -rf "+html_path_lmdb)
        os.system("mkdir -p "+html_path_lmdb)
        env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn:   
            txn.put('ALL_KEYS', '#'.join(txt1_d.keys()))
            for key, val in txt1_d.items():
                 line   = val.split('::TASDELIM::')
                 val   = line[1]
                 txn.put('TABLE-'+str(key), binascii.b2a_hex(val))
                 txn.put('TABLE-MAP-'+str(key), line[0])

    def read_normalized_table_ids(self, project_id, url_id, norm_id):
        norm_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh'
        sh = shelve.open(norm_path%(project_id, url_id, norm_id[2]))
        if 'celldata' not in sh:
            return [], ''
        celldata = sh['celldata']
        row_col_dict = {}
        taxo_d  = {}
        ids     = []
        for key, value_dict in celldata.items():
            #print key
            cell_ids = value_dict.keys()
            cell_ids.sort()
            #print '\t', cell_ids
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                #print '\t\t',cell_id, cell_info, '<hr>'
                txt = ' '.join(cell_info.get('text_lst', []))
                row, col = cell_id
                if row not in row_col_dict:
                    row_col_dict[row] = []
                #taxo_d.setdefault(cell_info['section_type'], {}).setdefault(txt, {})[(row, cell_id)]    = 1
                ids     += cell_info.get('text_ids', [])
                row_col_dict[row].append((col, txt, cell_info.get('section_type', ''), cell_info.get('text_ids', [])))
        #sys.exit()
        html_str = '<table class="taxonomy_data_table_cls" cellspacing="0" cellpadding="0">'
        rows = row_col_dict.keys() 
        rows.sort()
        page_num    = {}
        for row in rows:
            html_str+='<tr>'
            cols = row_col_dict[row]
            cols.sort()
            #print '\n================================================'
            #print 'ROW ',row
            for col_tup in cols:
                col, txt, section_type, ids = col_tup
                #print '\t\t', col_tup
                #for tt in ids:
                #    if tt:
                #        page_num[tt.split('_')[1]]    = 1
                html_str+='<td class="%s"><span id="%s">%s</span></td>' %(section_type, '#'.join(ids), txt)
            html_str+='</tr>'
        html_str+='</table>'
        return ids, html_str

    def read_norm_cell_dict(self, project_id, url_id, norm_id, taxo_d):
        norm_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/data/norm_celldict/%s.sh'
        sh = shelve.open(norm_path%(project_id, url_id, norm_id[2]))
        if 'celldata' not in sh:
            return {}
        celldata = sh['celldata']
        row_col_dict = {}
        tmpd        = {}
        for key, value_dict in celldata.items():
            #print key
            cell_ids = value_dict.keys()
            cell_ids.sort()
            #print '\t', cell_ids
            for cell_id in cell_ids:
                cell_info = value_dict[cell_id]
                #print '\t\t',cell_id, cell_info, '<hr>'
                txt = ' '.join(cell_info.get('text_lst', [])).lower()
                if cell_info.get('section_type', ''):
                    txt_sp  = txt.split('(')
                    if len(txt_sp)> 1:
                       i    = 0
                       for nt in txt_sp[::-1]:
                           try:    
                              tmpxx = int(nt.strip(')').strip())
                           except:break
                           i += 1
                       if i and (i < len(txt_sp)) and txt_sp[0].strip():
                           #print 'FOOTER ', i, len(txt_sp)
                           #print 'prev ', [txt]
                           #print 'prev ', ['('.join(txt_sp[:(len(txt_sp) - i)])]
                           txt  = '('.join(txt_sp[:(len(txt_sp) - i)])
                       txt  = ' '.join(txt.split())
                           
                    taxo_d.setdefault(cell_info['section_type'], {}).setdefault(txt, {}).setdefault(norm_id[2], {})[cell_id]    = 1
                    tmpd.setdefault(cell_info['section_type'], {}).setdefault(txt, {})[cell_id]    = 1
             
        return tmpd 

    def find_delta_normalized_table(self, pair, deal_id):
        for ttt in [1]:
            #print 'Running ', pp, ' / ', len(pairs), pair
            doc1    = int(pair[0][1])
            doc2    = int(pair[1][1])

            i1       = self.doc_d.get(doc1, {})
            i2       = self.doc_d.get(doc2, {})
            #enableprint()
            if len(i1.keys()) == 0:
                enableprint()
                return "There is no Pages are available For Doc: "+ str(doc1)
            if len(i2.keys()) == 0:
                enableprint()
                return "There is no Pages are available For Doc: "+ str(doc2)
            #print '<hr>i1 ', i1
            #print '<hr>i2 ', i2
            #print '<hr>'
            if not i1 or not i2:continue
            tids1       = []
            tids2       = []
            taxo_d1     = {}
            taxo_d2     = {}
            inp_files   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_match_ids.txt'
            fin         = open(inp_files, 'r')
            lines       =  fin.readlines()  
            fin.close()
            print 'GET NORMALIZE TABLE ', [doc1, len(lines)]
            for line in lines:
                line    = line.strip()
                if not line:continue
                td  = self.read_norm_cell_dict(deal_id[0], deal_id[1], ('', '', line), {})
                tids1.append((line, td))
            #os.system("rm -rf "+inp_files)
            inp_files   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'_match_ids.txt'
            fin         = open(inp_files, 'r')
            lines       =  fin.readlines()  
            fin.close()
            print 'GET NORMALIZE TABLE ', [doc2, len(lines)]
            for line in lines:
                line    = line.strip()
                if not line:continue
                td  = self.read_norm_cell_dict(deal_id[0], deal_id[1], ('', '', line), taxo_d2)
                tids2.append((line, td))
            #os.system("rm -rf "+inp_files)
            final_delta, delta_pos_d = self.match_delta(tids1, tids2, taxo_d1, taxo_d2)
            #self.find_delta(pair, deal_id, final_delta, delta_pos_d)
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
            txt1_d  = {}
            txt2_d  = {}
            pages_wise_hype = []
            pages_wise_ref = []
            pid_normid_h  = {}
            pid_normid_r  = {}
            env = lmdb.open(table_path)
            with env.begin() as txn:   
                pnos    = txn.get('ALL_KEYS').split('#')
                for pid in pnos:
                    pid = int(pid)
                    txt1_d[pid] = binascii.a2b_hex(txn.get('TABLE-'+str(pid)))
                    pages_wise_hype.append(pid)
                    pid_normid_h[txn.get('TABLE-MAP-'+str(pid))]    = pid
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc2)+'_table_info'
            env = lmdb.open(table_path)
            with env.begin() as txn:   
                pnos    = txn.get('ALL_KEYS').split('#')
                for pid in pnos:
                    pid = int(pid)
                    txt2_d[pid] = binascii.a2b_hex(txn.get('TABLE-'+str(pid)))
                    pages_wise_ref.append(pid)
                    pid_normid_r[txn.get('TABLE-MAP-'+str(pid))]    = pid
            hyp_match_dict = {}
            ref_match_dict = {}
            user_delta      = {}
            for (hline, rline), flag in final_delta.items():
                hline   = pid_normid_h[hline]
                rline   = pid_normid_r[rline]
                hyp_match_dict[hline]    = rline
                user_delta[str(hline)+'-'+str(rline)] = 1
                ref_match_dict[rline] = hline
            pos_dd  = {}
            for hline, v in delta_pos_d.items():
                hline   = pid_normid_h[hline]
                pos_lines   = []
                for rline in v:
                    rline   = pid_normid_r[rline]
                    pos_lines.append(str(rline))
                pos_dd[str(hline)]  = pos_lines
               
            hyp_lines	= pages_wise_hype
            ref_lines	= pages_wise_ref
            hyp_lines.sort()
            ref_lines.sort()
            tmp_final_ar	= []
            tmp_m_dict	= {}
            for hyp_line in hyp_lines:
                if hyp_line in hyp_match_dict:
                   ref_line = hyp_match_dict[hyp_line]
                   if (ref_match_dict.get(ref_line, '') == hyp_line) and (ref_line in ref_lines):
                       tmp_final_ar.append(([hyp_line], [ref_line]))
                       tmp_m_dict[ref_line]	= hyp_line
                   else:
                       tmp_final_ar.append(([hyp_line], []))
                else:
                   tmp_final_ar.append(([hyp_line], []))
            del_lines = list(sets.Set(ref_lines) - sets.Set(tmp_m_dict.keys()))
            del_lines.sort()
            reverse_map_dict_ref_part_item  = {}
            for tmp_ind, elm in enumerate(ref_lines):
                  reverse_map_dict_ref_part_item[elm]  = tmp_ind
            #print 'Before '
            #for rr in tmp_final_ar:
            #    print rr
            #print 'After '
            tmp_final_ar	= self.rearrange_pairs(ref_lines, del_lines, reverse_map_dict_ref_part_item, tmp_final_ar)
            page_pairs	        = self.re_arrange_final_pair(tmp_final_ar, user_delta)
            #for rr in page_pairs:
            #    print rr
            #print user_delta
            #sys.exit()
            print 'Final Pairs ', (deal_id, pair, len(page_pairs))
            page_pairs  = map(lambda x:x[:2], page_pairs)
            final_pairs = [('ALL', page_pairs)]
            final_css   = ['.taxonomy_data_table_cls {border: 1px solid #6DCCFF;}', '.taxonomy_data_table_cls td {background-color: #FFFFFF; border:1px solid #6DCCFF;font-size:12px;}']
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc2)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'_'+str(doc2)+'.css'
            fout    = open(final_css_file, 'wb')
            fout.write('\n'.join(final_css))
            fout.close()

            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+pair[0][1]+'_'+pair[1][1]+'_delta_possibility'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for k, v in pos_dd.items():
                    txn.put('POSS-'+str(k), '#'.join(v))
            os.system("chmod -R 777 "+html_path_lmdb)

            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(pair[0][1])+'_'+str(pair[1][1])+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt1_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))
                for key, val in txt2_d.items():
                    txn.put('R-'+str(key), binascii.b2a_hex(val))
            os.system("chmod -R 777 "+html_path_lmdb)
            #html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_'+str(doc2)+'_user_delta_info'
            #os.system("rm -rf "+html_path_lmdb)
            os.system("chmod -R 777 "+html_path_lmdb)
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_'+str(doc2)+'_user_delta_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
               txn.put('SYSTEM_MATCH', '#'.join(user_delta.keys()))
               for k, v in user_delta.items():
                  txn.put(k, 'Y')
            self.delta_display(deal_id, '_'.join(pair[0]), '_'.join(pair[1]), final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">', user_delta)

    def match_delta(self, tids1, tids2, taxod1, taxod2):
        tids1.sort(key=lambda x:int(x[0]))
        final_pairs = []
        ref_match   = {}
        for tinfo in tids1:
            tableid, td = tinfo
            GH      = td.get('GH', {})
            if not GH:continue
            HGH     = td.get('HGH', {})
            if not HGH:continue
            GH_r    = taxod2.get('GH', {})
            #print '\n*********************************************************'
            #print '\n', tableid
            m_tables    = {}
            for txt in GH.keys():
                m_t = GH_r.get(txt, {})
                if m_t:
                    m_tables.update(m_t)
                    #print '\t================================'
                    #print '\t', txt
                    #print '\t\t', m_t.keys() 
            if m_tables:
                toal_igs   = len(HGH.keys())
                #print 'MATCH HGH ', toal_igs
                HGH_r    = taxod2.get('HGH', {})
                table_match = {}
                for txt in HGH.keys():
                    m_t = {}
                    for tid, tv in HGH_r.get(txt, {}).items():
                        if tid  in m_tables:
                            m_t[tid]    = tv
                            table_match.setdefault(tid, {}).setdefault(txt, {}).update(tv)
                ntables = table_match.keys()
                ntables.sort(key=lambda x:(len(table_match[x].keys()), 0 - int(x)), reverse=True)
                for ntable in ntables:
                    taxos = table_match[ntable]
                    #print '\t-----------------------------------------' 
                    #print '\t',ntable
                    matched_igs = len(taxos.keys())
                    percent     = int((matched_igs/float(toal_igs))*100)
                    final_pairs.append((tableid, percent, matched_igs, ntable))
                    ref_match.setdefault(ntable, []).append((percent, matched_igs, tableid))
                    #print '\t\t', matched_igs
                    #print '\t\tMatch(%)', int((matched_igs/float(toal_igs))*100),'%'
                    #for tt in taxos.keys():
                    #   print '\t\t\t', tt
                    #print '\tUnmatched' , list(sets.Set(HGH.keys()) - sets.Set(taxos.keys()))
                
                    
             
            #break
        final_pair_d    = {}
        hdone           = {}
        rdone           = {}
        #SINGLE POS
        delta_pos_d     = {}
        for rr in final_pairs:
            #print '==============================' 
            #print rr
            tableid, percent, matched_igs, ntable   = rr 
            if percent > 70:
                delta_pos_d.setdefault(tableid, []).append((ntable, percent))
            if (tableid, ntable) in final_pair_d:continue
            if tableid in hdone:continue
            if ntable in rdone:continue
            rmatchs = ref_match.get(ntable, [])
            rmatchs.sort(key=lambda x:(x[1], 0-int(x[2])), reverse=True)
            if rmatchs[0][2] == tableid and percent > 70:
                #print 'FINAL SINGLE POS ', rr
                final_pair_d[(tableid, ntable)] = 1
                hdone[tableid]  = 1
                rdone[ntable]   = 1
        #MULTI  POS
        for rr in final_pairs:
            tableid, percent, matched_igs, ntable   = rr 
            if (tableid, ntable) in final_pair_d:continue
            if tableid in hdone:continue
            if ntable in rdone:continue
            #print '=================================='
            #print rr
            rmatchs = ref_match.get(ntable, [])
            rmatchs.sort(key=lambda x:(x[1], 0-int(x[2])), reverse=True)
            f   = ''
            for rmatch in rmatchs:
                #print '\t\t',rr
                if rmatch[1] > 70 and (rmatch[0] == tableid):
                   #print '\t\t', rmatch
                   f   = 1
            if f == 1:
                #print '\t\tFINAL MULTI POS ', rr
                final_pair_d[(tableid, ntable)] = 1
                hdone[tableid]  = 1
                rdone[ntable]   = 1
        ref_empty_grid_header   = {}
        for tinfo in tids2:
            tableid, td = tinfo
            GH      = td.get('GH', {})
            if  GH:continue
            ref_empty_grid_header[tableid] = 1
        final_pairs = []
        for tinfo in tids1:
            tableid, td = tinfo
            if tableid in delta_pos_d:continue
            GH      = td.get('GH', {})
            if  GH:continue
            HGH     = td.get('HGH', {})
            if not HGH:continue
            #print '\n*********************************************************'
            #print '\n', tableid
            m_tables    = ref_empty_grid_header
            if 1: #m_tables:
                toal_igs   = len(HGH.keys())
                #print 'MATCH HGH ', toal_igs
                HGH_r    = taxod2.get('HGH', {})
                table_match = {}
                for txt in HGH.keys():
                    m_t = {}
                    for tid, tv in HGH_r.get(txt, {}).items():
                        if tid  in m_tables:
                            m_t[tid]    = tv
                            table_match.setdefault(tid, {}).setdefault(txt, {}).update(tv)
                ntables = table_match.keys()
                ntables.sort(key=lambda x:(len(table_match[x].keys()), 0 - int(x)), reverse=True)
                for ntable in ntables:
                    taxos = table_match[ntable]
                    #print '\t-----------------------------------------' 
                    #print '\t',ntable
                    matched_igs = len(taxos.keys())
                    percent     = int((matched_igs/float(toal_igs))*100)
                    if percent > 70:
                        final_pairs.append((tableid, percent, matched_igs, ntable))
                    #print '\t\t', matched_igs
                    #print '\t\tMatch(%)', int((matched_igs/float(toal_igs))*100),'%'
                    #for tt in taxos.keys():
                    #   print '\t\t\t', tt
                    #print '\tUnmatched' , list(sets.Set(HGH.keys()) - sets.Set(taxos.keys()))
        for rr in final_pairs:
            #print '==============================' 
            #print rr
            tableid, percent, matched_igs, ntable   = rr 
            if percent > 70:
                delta_pos_d.setdefault(tableid, []).append((ntable, percent))
        tmp_delta_pos_d = {}
        for k, v in delta_pos_d.items():
            v.sort(key=lambda x:(x[1], 0-int(x[0])), reverse=True)
            tmp_delta_pos_d[k]=map(lambda x:x[0], v)
        return final_pair_d, tmp_delta_pos_d

    def create_doc_data(self, doc1, deal_id):
        i1       = self.doc_d.get(doc1, {})
        i1      = i1.keys()
        i1.sort()
        final_page_doc  = []
        fin_lst = []
        for page_no in i1:
                path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc1, page_no)
                fin_lst.append(str(page_no)+'\t'+path_celldict)
                final_page_doc.append((doc1, page_no, deal_id))
        inp_files   = '/tmp/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'.txt'
        fin = open(inp_files, 'wb')
        fin.write('\n'.join(fin_lst))
        fin.close()
        outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'+str(doc1)+'.html'
        os.system("node join_html.js %s %s"%(inp_files, outfile))
        os.system("rm -rf "+inp_files)
        for p, page_no in enumerate(final_page_doc):
            #print '\tRunning ',p, ' / ', len(final_page_doc), ' ( ', pp, ' / ', len(pairs), pair, ')', page_no[:2]
            print '\tRunning ',p, ' / ', len(final_page_doc),  page_no[:2]
            doc, page_no, deal_id  = page_no
            self.upObj.update_lmdb_cell(doc, page_no, deal_id)
        txt1_d  = {}
        p1      = 1
        for page_no in i1:
             filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(page_no)+'.txt'
             if not os.path.exists(filename):continue
             fin    = open(filename, 'r')
             lines  = fin.readlines()
             fin.close()
             #os.system("rm -rf "+filename)
             for line in lines:
                 line   = line.strip()
                 if not line:continue
                 txt1_d[str(p1)]  = line
                 p1 += 1
        html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
        os.system("rm -rf "+html_path_lmdb)
        os.system("mkdir -p "+html_path_lmdb)
        env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn:   
            txn.put('ALL_KEYS', '#'.join(txt1_d.keys()))
            for key, val in txt1_d.items():
                txn.put('TABLE-'+str(key), binascii.b2a_hex(val))

    def find_delta_new(self, pair, deal_id):
        for ttt in [1]:
            #print 'Running ', pp, ' / ', len(pairs), pair
            doc1    = int(pair[0][1])
            doc2    = int(pair[1][1])
            i1       = self.doc_d.get(doc1, {})
            i2       = self.doc_d.get(doc2, {})
            #enableprint()
            if len(i1.keys()) == 0:
                enableprint()
                return "There is no Pages are available For Doc: "+ str(doc1)
            if len(i2.keys()) == 0:
                enableprint()
                return "There is no Pages are available For Doc: "+ str(doc2)
            #print '<hr>i1 ', i1
            #print '<hr>i2 ', i2
            #print '<hr>'
            if not i1 or not i2:continue
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
            txt1_d  = {}
            txt2_d  = {}
            pages_wise_hype = []
            pages_wise_ref = []
            env = lmdb.open(table_path)
            with env.begin() as txn:   
                pnos    = txn.get('ALL_KEYS').split('#')
                for pid in pnos:
                    pid = int(pid)
                    txt1_d[pid] = binascii.a2b_hex(txn.get('TABLE-'+str(pid)))
                    pages_wise_hype.append(pid)
            table_path       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc2)+'_table_info'
            env = lmdb.open(table_path)
            with env.begin() as txn:   
                pnos    = txn.get('ALL_KEYS').split('#')
                for pid in pnos:
                    pid = int(pid)
                    txt2_d[pid] = binascii.a2b_hex(txn.get('TABLE-'+str(pid)))
                    pages_wise_ref.append(pid)
            
            final_pairs = []
            pages_wise_hype.sort()
            pages_wise_ref.sort()
            page_pairs = self.combine_two_list_elms(pages_wise_hype, pages_wise_ref)
            for pair_page in page_pairs:
                pair_1 = pair_page[0]
                pair_2 = pair_page[1]
                if not pair_1:
                   pair_1 = []
                else:
                   pair_1 = [pair_1]
                if not pair_2:
                   pair_2 = []
                else:
                   pair_2 = [pair_2]
                final_pairs.append((pair_1, pair_2))
            final_pairs = [('ALL', final_pairs)]
            final_css   = ['.taxonomy_data_table_cls {border: 1px solid #6DCCFF;}', '.taxonomy_data_table_cls td {background-color: #FFFFFF; border:1px solid #6DCCFF;font-size:12px;}']
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc2)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'_'+str(doc2)+'.css'
            fout    = open(final_css_file, 'wb')
            fout.write('\n'.join(final_css))
            fout.close()

            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(pair[0][1])+'_'+str(pair[1][1])+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt1_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))
                for key, val in txt2_d.items():
                    txn.put('R-'+str(key), binascii.b2a_hex(val))
            os.system("chmod -R 777 "+html_path_lmdb)
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_'+str(doc2)+'_user_delta_info'
            os.system("rm -rf "+html_path_lmdb)
            self.delta_display(deal_id, '_'.join(pair[0]), '_'.join(pair[1]), final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">')

    def find_delta(self, pair, deal_id):
        for ttt in [1]:
            #print 'Running ', pp, ' / ', len(pairs), pair
            doc1    = int(pair[0][1])
            doc2    = int(pair[1][1])
            i1       = self.doc_d.get(doc1, {})
            i2       = self.doc_d.get(doc2, {})
            #print '<hr>i1 ', i1
            #print '<hr>i2 ', i2
            #print '<hr>'
            if not i1:
                return ["For Doc "+ str(doc1) +" No Pages are available"]
            if not i2:
                return ["For Doc "+ str(doc2) +" No Pages are available"]
            if not i1 or not i2:continue
            i1      = i1.keys()
            i2      = i2.keys()
            i1.sort()
            i2.sort()
            i1  = filter(lambda x:os.path.exists('/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(x)+'.txt'), i1)
            i2  = filter(lambda x:os.path.exists('/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'_'+str(x)+'.txt'), i2)
            final_list  = self.combine_two_list_elms(i1, i2)
            final_pairs = []
            p1  = 1
            p2  = 1
            txt1_d  = {}
            txt2_d  = {}
            print i1
            print i2
            for page_pair in final_list:
                print page_pair
                pages_wise_hype = []
                pages_wise_ref = []
                page_pairs = []
                page_str    = ''
                if page_pair[0]:
                     page_str    = str(page_pair[0])
                     filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(page_pair[0])+'.txt'
                     fin    = open(filename, 'r')
                     lines  = fin.readlines()
                     fin.close()
                     #os.system("rm -rf "+filename)
                     for line in lines:
                         line   = line.strip()
                         if not line:continue
                         txt1_d[p1]  = line
                         #page_pairs.append(([p1], []))
                         pages_wise_hype.append(p1)
                         p1 += 1
                if page_pair[1]:
                     if page_str == '':
                         page_str    = 'R-'+str(page_pair[1])
                     filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'_'+str(page_pair[1])+'.txt'
                     fin    = open(filename, 'r')
                     lines  = fin.readlines()
                     fin.close()
                     #os.system("rm -rf "+filename)
                     for line in lines:
                         line   = line.strip()
                         if not line:continue
                         txt2_d[p2]  = line
                         #page_pairs.append(([], [p2]))
                         pages_wise_ref.append(p2)
                         p2 += 1
                #page_pairs  = pages_wise_hype + pages_wise_ref
                page_pairs = self.combine_two_list_elms(pages_wise_hype, pages_wise_ref)
                final_pair = []
                for pair_page in page_pairs:
                    pair_1 = pair_page[0]
                    pair_2 = pair_page[1]
                    if not pair_1:
                       pair_1 = []
                    else:
                       pair_1 = [pair_1]
                    if not pair_2:
                       pair_2 = []
                    else:
                       pair_2 = [pair_2]
                    final_pairs.append((pair_1, pair_2))
                #final_pairs.append((page_str, final_pair))
            final_pairs = [('ALL', final_pairs)]
            final_css   = ['.taxonomy_data_table_cls {border: 1px solid #6DCCFF;}', '.taxonomy_data_table_cls td {background-color: #FFFFFF; border:1px solid #6DCCFF;font-size:12px;}']
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc2)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                #os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'_'+str(doc2)+'.css'
            fout    = open(final_css_file, 'wb')
            fout.write('\n'.join(final_css))
            fout.close()
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(pair[0][1])+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt1_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))

            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(pair[1][1])+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt2_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))

            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(pair[0][1])+'_'+str(pair[1][1])+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt1_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))
                for key, val in txt2_d.items():
                    txn.put('R-'+str(key), binascii.b2a_hex(val))
            os.system("chmod -R 777 "+html_path_lmdb)
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_'+str(doc2)+'_user_delta_info'
            os.system("rm -rf "+html_path_lmdb)
            self.delta_display(deal_id, '_'.join(pair[0]), '_'.join(pair[1]), final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">')

    def find_delta_cmd(self, pair, deal_id):
        for ttt in [1]:
            #print 'Running ', pp, ' / ', len(pairs), pair
            doc1    = int(pair[0][1])
            doc2    = int(pair[1][1])
            i1       = self.doc_d.get(doc1, {})
            i2       = self.doc_d.get(doc2, {})
            if not i1 or not i2:continue
            i1      = i1.keys()
            i2      = i2.keys()
            i1.sort()
            i2.sort()
            final_page_doc  = []
            fin_lst = []
            for page_no in i1:
                path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc1, page_no)
                fin_lst.append(str(page_no)+'\t'+path_celldict)
                final_page_doc.append((doc1, page_no, deal_id))
            inp_files   = '/tmp/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'.txt'
            fin = open(inp_files, 'wb')
            fin.write('\n'.join(fin_lst))
            fin.close()
            outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'+str(doc1)+'.html'
            os.system("node join_html.js %s %s"%(inp_files, outfile))
            os.system("rm -rf "+inp_files)
            fin_lst = []
            for page_no in i2:
                path_celldict = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html'%(str(deal_id[0]),deal_id[1],doc2, page_no)
                fin_lst.append(str(page_no)+'\t'+path_celldict)
                final_page_doc.append((doc2, page_no, deal_id))
            inp_files   = '/tmp/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'.txt'
            fin = open(inp_files, 'wb')
            fin.write('\n'.join(fin_lst))
            fin.close()
            outfile = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/HTML/'+str(doc2)+'.html'
            os.system("node join_html.js %s %s"%(inp_files, outfile))
            os.system("rm -rf "+inp_files)
            for p, page_no in enumerate(final_page_doc):
                #print '\tRunning ',p, ' / ', len(final_page_doc), ' ( ', pp, ' / ', len(pairs), pair, ')', page_no[:2]
                print '\tRunning ',p, ' / ', len(final_page_doc),  page_no[:2]
                doc, page_no, deal_id  = page_no
                self.upObj.update_lmdb_cell(doc, page_no, deal_id)
            i1  = filter(lambda x:os.path.exists('/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(x)+'.txt'), i1)
            i2  = filter(lambda x:os.path.exists('/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'_'+str(x)+'.txt'), i2)
            final_list  = self.combine_two_list_elms(i1, i2)
            final_pairs = []
            p1  = 1
            p2  = 1
            txt1_d  = {}
            txt2_d  = {}
            print i1
            print i2
            for page_pair in final_list:
                print page_pair
                pages_wise_hype = []
                pages_wise_ref = []
                page_pairs = []
                page_str    = ''
                if page_pair[0]:
                     page_str    = str(page_pair[0])
                     filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc1)+'_'+str(page_pair[0])+'.txt'
                     fin    = open(filename, 'r')
                     lines  = fin.readlines()
                     fin.close()
                     os.system("rm -rf "+filename)
                     for line in lines:
                         line   = line.strip()
                         if not line:continue
                         txt1_d[p1]  = line
                         #page_pairs.append(([p1], []))
                         pages_wise_hype.append(p1)
                         p1 += 1
                if page_pair[1]:
                     if page_str == '':
                         page_str    = 'R-'+str(page_pair[1])
                     filename   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'_'+str(doc2)+'_'+str(page_pair[1])+'.txt'
                     fin    = open(filename, 'r')
                     lines  = fin.readlines()
                     fin.close()
                     os.system("rm -rf "+filename)
                     for line in lines:
                         line   = line.strip()
                         if not line:continue
                         txt2_d[p2]  = line
                         #page_pairs.append(([], [p2]))
                         pages_wise_ref.append(p2)
                         p2 += 1
                #page_pairs  = pages_wise_hype + pages_wise_ref
                page_pairs = self.combine_two_list_elms(pages_wise_hype, pages_wise_ref)
                final_pair = []
                for pair_page in page_pairs:
                    pair_1 = pair_page[0]
                    pair_2 = pair_page[1]
                    if not pair_1:
                       pair_1 = []
                    else:
                       pair_1 = [pair_1]
                    if not pair_2:
                       pair_2 = []
                    else:
                       pair_2 = [pair_2]
                    final_pairs.append((pair_1, pair_2))
                #final_pairs.append((page_str, final_pair))
            final_pairs = [('ALL', final_pairs)]
            final_css   = ['.taxonomy_data_table_cls {border: 1px solid #6DCCFF;}', '.taxonomy_data_table_cls td {background-color: #FFFFFF; border:1px solid #6DCCFF;font-size:12px;}']
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            output_file_name_css = '/var/www/html/output_file/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc2)+'.css'
            if os.path.exists(output_file_name_css):
                fin    = open(output_file_name_css, 'r')
                lines  = fin.readlines()
                fin.close()
                os.system("rm -rf "+output_file_name_css)
                for line in lines:
                    line   = line.strip()
                    if not line:continue
                    line   = line.split('\t')
                    final_css.append('.'+line[0]+'{ '+line[1]+';}')
            final_css_file   =  '/var/www/html/output_file/css/'+str(deal_id[0])+str(deal_id[1])+'_'+str(doc1)+'_'+str(doc2)+'.css'
            fout    = open(final_css_file, 'wb')
            fout.write('\n'.join(final_css))
            fout.close()
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+pair[0][1]+'_'+pair[1][1]+'_table_info'
            os.system("rm -rf "+html_path_lmdb)
            os.system("mkdir -p "+html_path_lmdb)
            print 'KEYS ',txt1_d.keys()
            env = lmdb.open(html_path_lmdb, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:   
                for key, val in txt1_d.items():
                    txn.put('L-'+str(key), binascii.b2a_hex(val))
                for key, val in txt2_d.items():
                    txn.put('R-'+str(key), binascii.b2a_hex(val))
            os.system("chmod -R 777 "+html_path_lmdb)
            html_path_lmdb       = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_'+str(doc2)+'_user_delta_info'
            os.system("rm -rf "+html_path_lmdb)
            self.delta_display(deal_id, '_'.join(pair[0]), '_'.join(pair[1]), final_pairs,  '<link rel="stylesheet" href="'+final_css_file.replace('/var/www/html', '')+'">')

    def save_final_pair_info(self, ijson):
        user_name   = ijson['user_name']
        dealid      = ijson['deal_id']
        doc1        = ijson['doc1'].split('_')[-1]
        doc2        = ijson['doc2'].split('_')[-1]
        doc_id      = doc1+'_'+doc2
        deal_id     = '_'.join(map(lambda x:str(x), dealid))
        date_time   = str(datetime.datetime.now())
        database    = 'fundamental_delta'
        dbc =("172.16.20.122","root","tas123",database)
        db = MySQLdb.connect(dbc[0],dbc[1],dbc[2],dbc[3])
        cur = db.cursor()
        sql = "insert into user_log (user_name, doc_id, date_time, deal_id) values('%s', '%s', '%s', '%s')"%(user_name, doc_id, date_time, deal_id)
        cur.execute(sql)
        db.commit()
        cur.close()
        db.close() 
        return ['Done'] 

    def get_done_user_delta(self, input_dealid=''):
        database    = 'fundamental_delta'
        dbc =("172.16.20.122","root","tas123",database)
        db = MySQLdb.connect(dbc[0],dbc[1],dbc[2],dbc[3])
        cur = db.cursor()
        if input_dealid:
            sql = "select user_name, doc_id, date_time, deal_id from user_log where deal_id='%s' and delta_type is NULL"%(input_dealid)
        else:
            sql = "select user_name, doc_id, date_time, deal_id from user_log where delta_type is NULL"
        
        cur.execute(sql)
        res = cur.fetchall()
        db.commit()
        cur.close()
        db.close() 
        final_dict = {}
        for rr in res:
           user_name, doc_id, date_time, deal_id = rr
           #final_dict[deal_id] = 'Y'
           doc_id  = doc_id.replace('_', '-')
           final_dict.setdefault(deal_id, {})[doc_id] = (user_name, str(date_time))
        return final_dict

    def get_doc_ides_info(self, ijson):
        deal_info = ijson['deal_id']
        deal_1    = deal_info.split('_')[0]
        deal_2    = deal_info.split('_')[1]
        database="tfms_urlid_%s_%s" % (deal_1, deal_2)
        dbc =("172.16.20.122","root","tas123",database)
        db = MySQLdb.connect(dbc[0],dbc[1],dbc[2],dbc[3])
        cur = db.cursor()
        final_doc_d = {}
        sql = "select document_id, document_name from tfms_urlid_%s.ir_document_master"%(deal_info)
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            document_id, document_name = rr
        return final_doc_d



    def get_company_deal_info(self):
        done_deals  = self.get_done_user_delta()
        txt_file = "/var/www/cgi-bin/table_delta/deal_ids.txt"
        fout    = open(txt_file, 'r')
        lines   = fout.readlines()
        fout.close()
        all_deal_dict = {}
        all_cmp_deal_arr = {}
        database="tfms_urlid_%s_%s" % (1, 44)
        dbc =("172.16.20.122","root","tas123",database)
        db = MySQLdb.connect(dbc[0],dbc[1],dbc[2],dbc[3])
        cur = db.cursor()
        deal_arr = []
        deal_dict = {}
        docwise_cmp_name = {}
        for line in lines:
            line    = line.strip()
            if not line:continue
            all_deal_dict[line] = 1
            #sql = "select document_name from tfms_urlid_%s.ir_document_master limit 1"%(line)
            sql = "select document_id, document_name from tfms_urlid_%s.ir_document_master"%(line)
            cur.execute(sql)
            res = cur.fetchall()
            for rr in res:
                document_id, documentname = rr 
                cmp_name  = documentname.split('_')[0]
                docwise_cmp_name[line+'_'+str(document_id)] = documentname
                deal_dict.setdefault(line, []).append({'doc_id':int(document_id), 'company_name':documentname})
            #res = cur.fetchone()
            #cmp_name  = res[0].split('_')[0]
            deal_arr.append({'deal_id':line, 'company_name':cmp_name})
        doc_dict = {}
        for deal in all_deal_dict.keys():
             if os.path.exists('/var/www/html/output_file/'+str(deal)+'/HTML/'):
                 tres = os.popen('ls '+ '/var/www/html/output_file/'+str(deal)+'/HTML/'+'*-*.html').read().strip()
                 if tres:
                    compay_name_lst = tres.split('\n')
                    for compay_name in compay_name_lst:
                       cmpname_sp = compay_name.split('/')[-1]
                       cmp_name   = cmpname_sp.split('_')[0]
                       dc_name    = cmpname_sp.split('.')[0]
                       done_lst   = done_deals.get(deal, {}).get(dc_name, [])
                       status     = 'N'
                       doc_sp = cmpname_sp.split('.')[0]
                       cmpname1   = docwise_cmp_name.get(deal+'_'+str(doc_sp.split('-')[0]))
                       cmpname2   = docwise_cmp_name.get(deal+'_'+str(doc_sp.split('-')[1]))
                       cmpname    = cmpname1 + '-' +cmpname2
                       date       = ''

                       user_name  = ''
                       date       = ''
                       if done_lst:
                          status  = 'Y'
                          user_name = done_lst[0]
                          date      = done_lst[1]
                       #doc_dict.setdefault(deal, []).append({'document_name':cmpname_sp.split('.')[0], 'status':status, 'user_name':user_name, 'date':date})  
                       doc_dict.setdefault(deal, []).append({'document_name':cmpname_sp.split('.')[0], 'status':status, 'user_name':user_name, 'date':date, 'cmpname':cmpname})  
                       #doc_dict.setdefault(deal, []).append({'document_name':cmpname_sp.split('.')[0], 'status':status})  
                    #deal_arr.append({'deal_id':deal, 'company_name':cmp_name})
        for rr in deal_arr:
            val         = doc_dict.get(rr['deal_id'], [])
            rr['count'] = len(val)
            val.sort(key=lambda x:tuple(map(lambda x1:int(x1), x['document_name'].split('-'))))
            doc_dict[rr['deal_id']] = val
        deal_arr.sort(key=lambda x:int(x['deal_id'].split('_')[1]), reverse=False)
        cur.close()
        db.close() 
        return deal_arr, doc_dict, deal_dict

    def login_info(self, ijson):
        user_name   = ijson['user_name']
        password    = ijson['password']
        ip = '172.16.20.133'
        cmd_id   = 3
        res =  urllib.urlopen('http://'+ip+'/cgi-bin/fundamentals_intf/home_wrapper.py?input_str={"cmd_id":'+str(cmd_id)+',"user_id":'+'"'+user_name+'"'+',"password":'+'"'+password+'"'+'}')
        try:
                return  json.loads(res.read())
        except:
                return res

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def get_deal_info_lmdb(self, ijson):
        deal      = ijson['deal_id']
        doc_id    = ijson['doc_id']
        text      = ijson['text']
        lmdb_path = '/var/www/html/output_file/all_deal_info/'+str(deal)+'/'+str(doc_id)
        page_xlmid_dict  = {}
        if os.path.exists(lmdb_path):
            env       = lmdb.open(lmdb_path)   
            with env.begin() as txn:
                txt_arr = text.lower().split()
                final_res   = {}
                for ind, tt in enumerate(txt_arr):
                    unqid     = self.get_quid(tt)
                    page_info     = txn.get(unqid)
                    if (not page_info):
                         final_res  = {}
                         break
                    tmpd    = {}
                    for ttinfo in page_info.split('@@'):
                         pno, sid, ind  = ttinfo.split('#')
                         ind    = int(ind)
                         tmpd.setdefault((pno, sid), {})[ind]   = 1
                    if not final_res:
                        final_res   = tmpd
                    else:
                        tmp_res = {}
                        for ps_tup, inds in final_res.items(): 
                            newres  = tmpd.get(ps_tup, {})
                            if not newres:continue
                            inds    = inds.keys()
                            inds.sort()
                            new_inds    = {}
                            for i in inds:
                                if i+1 in newres:
                                    new_inds[i+1] = 1
                            if new_inds:
                                 tmp_res[ps_tup]    = new_inds
                        if not tmp_res:
                            final_res   = {}
                            break
                        final_res   = tmp_res
                for k, v in final_res.items():
                    page_xlmid_dict.setdefault(k[0], {}).setdefault(k[1].split('_')[1], []).append(k[1])
                    #page_xlmid_dict[k[1]] = 1
        return page_xlmid_dict

    def get_filing_info(self, filing):
        filing  = docinfo[0].split('_')[-1]
        year    = ''
        ftype   = ''
        if len(filing) == 4:
            try:
                tmpxx   = int(filing)
                filing  = docinfo[0].split('_')[-2]+filing
            except:pass
        if 'AR' in filing:
            ftype   = 'FY'
            year    = int(filing[-4:].strip())
        elif 'Q' in filing:
            ftype   = filing[:2]
            year    = int(filing[-4:].strip())
        elif 'H' in filing:
            ftype   = filing[:2]
            year    = int(filing[-4:].strip())
        if year and ftype:
            year    = int(filing[-4:].strip())
        elif len(docinfo[0].split('_')) > 2 :
            filing  = docinfo[0].split('_')[-2]
            year    = ''
            ftype   = ''
            if 'AR' in filing:
                ftype   = 'FY'
                year    = int(filing[-4:].strip())
            elif 'Q' in filing:
                ftype   = filing[:2]
                year    = int(filing[-4:].strip())
            elif 'H' in filing:
                ftype   = filing[:2]
                year    = int(filing[-4:].strip())
        return ftype, year

    def indexing_page(self, deal_id):
        #txt_file = "/var/www/cgi-bin/table_delta/deal_ids.txt"
        #fout     = open(txt_file, 'r')
        #ines    = fout.readlines()
        #fout.close()
        #all_deal_dict = {}
        #for line in lines:
        #    line    = line.strip()
        #    if not line:continue
        #    all_deal_dict[line] = 1
        #deal_info  = all_deal_dict.keys()
        deal_info  = [deal_id]
        print deal_info
        import read_slt_info
        l   = len(deal_info)
        alpha_dict = {'a':1, 'b':1, 'c':1, 'd':1, 'e':1, 'f':1, 'g':1, 'h':1, 'i':1, 'j':1, 'k':1, 'l':1, 'm':1, 'n':1, 'o':1, 'p':1, 'q':1, 'r':1, 's':1, 't':1 ,'u':1, 'v':1, 'w':1, 'x':1, 'y':1, 'z':1}
        for dind, deal in  enumerate(deal_info):
            print 'Running ', dind, ' / ', l, [deal]
            deal_id     = deal.split('_')
            self.upObj  = read_slt_info.update(deal_id)
            docname_docid = self.upObj.get_documentName_id()
            print docname_docid
            docinfo_d   = {}
            for docinfo in docname_docid:
                filing  = docinfo[0].split('_')[-1]
                docid   = docinfo[1]
                year    = ''
                ftype   = ''
                if len(filing) == 4:
                    try:
                        tmpxx   = int(filing)
                        filing  = docinfo[0].split('_')[-2]+filing
                    except:pass
                if 'AR' in filing:
                    ftype   = 'A'
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault('AR'+str(year), []).append(docinfo)
                elif 'Q' in filing:
                    ftype   = 'Q'
                    q       = filing[:2]
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault(q+str(year), []).append(docinfo)
                elif 'H' in filing:
                    ftype   = 'H'
                    q       = filing[:2]
                    year    = int(filing[-4:].strip())
                    docinfo_d.setdefault(q+str(year), []).append(docinfo)
                if year and ftype:
                    year    = int(filing[-4:].strip())
                elif len(docinfo[0].split('_')) > 2 :
                    filing  = docinfo[0].split('_')[-2]
                    year    = ''
                    ftype   = ''
                    if 'AR' in filing:
                        ftype   = 'A'
                        year    = int(filing[-4:].strip())
                        docinfo_d.setdefault('AR'+str(year), []).append(docinfo)
                    elif 'Q' in filing:
                        ftype   = 'Q'
                        q       = filing[:2]
                        year    = int(filing[-4:].strip())
                        docinfo_d.setdefault(q+str(year), []).append(docinfo)
                    elif 'H' in filing:
                        ftype   = 'H'
                        q       = filing[:2]
                        year    = int(filing[-4:].strip())
                        docinfo_d.setdefault(q+str(year), []).append(docinfo)
            self.doc_d  = {}
            for i in self.upObj.query1():
                if not i[1]:continue
                self.doc_d.setdefault(int(i[0]), {})[int(i[1])] = 1
            doc_ides    = docinfo_d.keys()
            for docind, doc_id in enumerate(doc_ides):
                doc_dict    = {}
                page_nos    = []
                print '\n', doc_id, docinfo_d[doc_id]
                for rr in docinfo_d[doc_id]:
                     for pno in self.doc_d.get(int(rr[1]), {}).keys():
                         page_nos.append((rr[1], pno))                       
                lmdb_path   = '/var/www/html/output_file/all_deal_info/'+str(deal)+'/'+str(doc_id)
                os.system("rm -rf "+lmdb_path)
                os.system("mkdir -p "+lmdb_path)
                for pind, page_no in enumerate(page_nos):
                    print '\tRunning ',pind, ' / ',len(page_nos), ' ( ', docind, ' / ', len(doc_ides), ')', [dind, ' / ', l]
              
                    html_path = "/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/%s/html/%s_celldict.html"%(deal_id[0], deal_id[1], page_no[0], page_no[1])
                    cmd = "node read_info.js "+ html_path
                    res = os.popen(cmd).read()
                    res_final = json.loads(res)
                    page_no = page_no[0]
                    for key, val in res_final.items():
                        val  = val.lower()
                        idd  = key
                        value_sp = val.split()
                        for ind, sp_tx in enumerate(value_sp):
                           doc_dict.setdefault(sp_tx, {})[str(page_no)+'#'+str(idd)+'#'+str(ind)] = 1
                           found_alpha = 0
                           nlist       = []
                           for char in sp_tx:
                              if char in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.']:
                                  nlist.append(char)
                              if char.lower() in alpha_dict:
                                 found_alpha = 1                 
                                 break
                           if found_alpha == 0 and nlist:
                               new_tx     = ''.join(nlist)
                               doc_dict.setdefault(new_tx, {})[str(page_no)+'#'+str(idd)+'#'+str(ind)] = 1
                               doc_dict.setdefault(new_tx.strip('.'), {})[str(page_no)+'#'+str(idd)+'#'+str(ind)] = 1
                          
                env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024)
                with env.begin(write=True) as txn:   
                   for txt, valuee in doc_dict.items():
                       unqid       = self.get_quid(txt)
                       id_page_lst = valuee.keys()
                       txn.put(unqid, '@@'.join(id_page_lst))
                       txn.put(unqid+'_TEXT', binascii.b2a_hex(txt))
                print 'DONE DOCID ', lmdb_path
                os.system("chmod -R 777 "+lmdb_path)
        return ['DONE']

    def upload_status(self, ijson):
        fileitem, user_name = ijson['fileitem'], ijson['user_name']
        file_name           = fileitem.filename
        file_c              = fileitem.file
        random_no           = str(time.time())
        random_no           = random_no.replace(".", "")
        user_name           = '_'.join(user_name.split())
        output_filename     = user_name+'_'+random_no+'.xls'
        taxomgmtfilename    = "/var/www/html/rajeev/"+output_filename
        open(taxomgmtfilename, 'wb').write(fileitem.file.read())
        ress = self.read_excel_status(taxomgmtfilename, user_name)
        return ress

    def read_excel_status(self, excel_path="/var/www/html/rajeev/70_Company_Status.xls", user_name='SYSTEM'):
        xl_workbook = xlrd.open_workbook(excel_path)
        sheet_names = xl_workbook.sheet_names()
        db, cur     = self.create_connection()
        date_time   = str(datetime.datetime.now())
        sql = "select deal_id, indentification_process from processing_status"
        cur.execute(sql)
        res = cur.fetchall()
        exit_deal_dict = {}
        for rr in res:
            deal_id, indentification_process = rr
            exit_deal_dict[deal_id] = 1
        process_dict  = {'process_done':'Y', 'completed':'Y', 'done':'Y', 'wip':'P', '':'N'} 
        for i in range(0,len(sheet_names)):
            xl_sheet = xl_workbook.sheet_by_name(sheet_names[i])
            num_rows = xl_sheet.nrows   # Number of rows
            num_cols = xl_sheet.ncols   # Number of columns
            for row_idx in range(0, xl_sheet.nrows):    # Iterate through rows
                if row_idx >0:  # for Header
                    deal_id                 = xl_sheet.cell(row_idx,1).value
                    sql_c = "select document_name from tfms_urlid_%s.ir_document_master"%(deal_id)
                    cur.execute(sql_c)
                    res = cur.fetchone()
                    company_name = res[0].split('_')[0]
                    deal_id                 = deal_id.strip()
                    indentification_process = xl_sheet.cell(row_idx,2).value
                    indentifica             = xl_sheet.cell(row_idx,3).value
                    normalization_process   = xl_sheet.cell(row_idx,4).value
                    normaliza               = xl_sheet.cell(row_idx,5).value
                    table_delta_process     = xl_sheet.cell(row_idx,6).value
                    tabledelta              = xl_sheet.cell(row_idx,7).value
                    paragraph_delta_process = xl_sheet.cell(row_idx,8).value
                    paragraphdelta          = xl_sheet.cell(row_idx,9).value
                    #print [deal_id, indentification_status, indentification, normalization_status, normalization, table_delta_status, table_delta, paragraph_delta_status, paragraph_delta]
                    #continue
                    indentification_status  = 'N'
                    indentification         = 'N'
                    normalization_status    = 'N'
                    normalization           = 'N'
                    table_delta_status      = 'N'
                    table_delta             = 'N'
                    paragraph_delta_status  = 'N'
                    paragraph_delta         = 'N'
                    for k, v in process_dict.items():
                        if indentification_process.lower().startswith(k):
                            indentification_status  = v
                        if indentifica.lower().startswith(k):
                            indentification         = v
                        if normalization_process.lower().startswith(k):
                            normalization_status    = v
                        if normaliza.lower().startswith(k):
                            normalization           = v
                        if table_delta_process.lower().startswith(k):
                            table_delta_status      = v
                        if tabledelta.lower().startswith(k):
                            table_delta             = v
                        if paragraph_delta_process.lower().startswith(k):
                            paragraph_delta_status  = v
                        if paragraphdelta.lower().startswith(k):
                            paragraph_delta         = v

                    #print [indentification_status, indentification, normalization_status, normalization, table_delta_status, table_delta, paragraph_delta_status, paragraph_delta]
                    if deal_id in exit_deal_dict:
                        sql = "update processing_status set indentification_process = '%s', indentification = '%s', normalization_process = '%s', normalization = '%s', table_delta_process = '%s', table_delta='%s', paragraph_delta_process='%s', paragraph_delta = '%s', date_time = '%s', company_name = '%s', updated_user='%s' where deal_id = '%s'"%(indentification_status, indentification, normalization_status, normalization, table_delta_status, table_delta, paragraph_delta_status, paragraph_delta, date_time, company_name, user_name, deal_id)
                    else:
                        sql = "insert into processing_status (deal_id, indentification_process, indentification, normalization_process, normalization, table_delta_process, table_delta, paragraph_delta_process, paragraph_delta, date_time, company_name, updated_user) values('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s', '%s')"%(deal_id, indentification_status, indentification, normalization_status, normalization, table_delta_status, table_delta, paragraph_delta_status, paragraph_delta, date_time, company_name, user_name)
                    print sql
                    cur.execute(sql)
                    db.commit() 
        db.close()
        cur.close()
        return ['Done'] 

    def read_info_table(self, ijson):
        dealid     = ijson['deal_id']
        deal_id    = dealid.split('_')
        doc1        = ijson['doc_id']
        default_d   =  {
                            "SCALE":["MN", "BN", "TH"],
                            "STATEMENT TYPE":["CONSOLIDATED STATEMENT", "UNCONSOLIDATED STATEMENT"],
                            "STATEMENT STATUS TYPE":["AUDITED", "UNAUDITED"],
                            "PERIOD":["FY", "Q1", "Q2", "Q3","Q4","H1", "H2"],
                            "CURRENCY":["EUR", "USD", "GBP"],
                            "TABLE TYPE":["INCOME STATEMENT", "BALANCE SHEET", "CASH FLOW STATEMENT", "SALES BY PRODUCT", "NOTES TO INCOME STATEMENT"],
                            "TERM ENDING":['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
                    }
        taxo_arr = ['SCALE', 'STATEMENT TYPE', 'STATEMENT STATUS TYPE', 'PERIOD', 'CURRENCY', 'TABLE TYPE', 'TERM ENDING']
        saved_taxo_d  = self.read_save_meta_data(deal_id, doc1)
        table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_table_info'
        table_dict  = {}
        doc_table_d = {}
        env         = lmdb.open(table_path)
        with env.begin() as txn:   
            if txn.get('ALL_KEYS'):
                pnos    = txn.get('ALL_KEYS').split('#')
                for key in pnos:
                    val = txn.get('TABLE-'+key)
                    table  =  binascii.a2b_hex(val)
                    table_dict[key] = {'table':table}
        doc_table_d['all_taxonomy'] = default_d
        return table_dict, default_d, saved_taxo_d

    def read_save_meta_data(self, deal_id, doc1):
        meta_info   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env         = lmdb.open(meta_info)
        saved_taxo_d = {}
        with env.begin() as txn:   
            for key, val in txn.cursor():
                if not val:continue
                table_id  = key.split('_')[0]
                taxo_val  = val.split(':#:')
                for taxo in taxo_val:
                    taxo_lst =  taxo.split(':@@:') 
                    saved_taxo_d.setdefault(table_id, {})[taxo_lst[0]] = taxo_lst[1]
        return saved_taxo_d 

    def read_save_meta_data_bytableid(self, deal_id, doc1, tableid):
        meta_info   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env         = lmdb.open(meta_info)
        saved_taxo_d = {}
        with env.begin() as txn:   
            val  = txn.get(str(tableid)+'_TAXOS')
            if val:
                taxo_val  = val.split(':#:')
                for taxo in taxo_val:
                    taxo_lst =  taxo.split(':@@:') 
                    saved_taxo_d.setdefault(tableid, {})[taxo_lst[0]] = taxo_lst[1]
        return saved_taxo_d 


    def save_taxo_info(self, ijson):
        dealid      = ijson['deal_id']
        deal_id     = dealid.split('_')
        doc1        = ijson['doc_id']
        taxo_dict   = ijson['taxo_dict']
        exists_d    = {}
        table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env = lmdb.open(table_path)
        with env.begin() as txn: 
            for tableid, taxo in taxo_dict.items():
               val  = txn.get(tableid+'_TAXOS')
               if not val:continue
               for tt in val.split(':#:'):
                    if not tt:continue
                    tt  = tt.split(':@@:')
                    exists_d.setdefault(tableid, {})[tt[0]]= tt[1]
               #print key, val
        final_d = {}
        table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env = lmdb.open(table_path, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn: 
            for tableid, taxo in taxo_dict.items():
               tmpd = exists_d.get(tableid, {})
               tmpd.update(taxo)
               final_d[tableid] = tmpd
               tmpar    = []
               for k, v in tmpd.items():
                   tmpar.append(k+':@@:'+v)
               taxo_val = ':#:'.join(tmpar)
               txn.put(tableid+'_TAXOS', taxo_val)            
        return final_d

    def read_all_meta_data(self, ijson):
        deal_id       = ijson['deal_id']
        import read_slt_info
        self.upObj    = read_slt_info.update(deal_id)
        docname_docid = self.upObj.get_documentName_id()
        docinfo_d     = {}
        table_dict    = {}
        doc_xml_d     = {}
        for docinfo in docname_docid:
            docid       = docinfo[1]
            table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(docid)+'_table_info'
            env         = lmdb.open(table_path)
            
            with env.begin() as txn:   
                pnos    = txn.get('ALL_KEYS').split('#')
                for key in pnos:
                  val = txn.get('TABLE-'+key)
                  table  =  binascii.a2b_hex(val)
                  xml_lst = table.split('XML_REF="')[1].split('"')[0].split('@@')[1].split('#')
                  doc_xml_d[docid] = xml_lst
                  #table_dict[key] = {'table':table}
                  #for key, val in txn.cursor():
                  #    if key == 'ALL_KEYS':continue
                  #    table   =  binascii.a2b_hex(val)
                  #    xml_lst = table.split('XML_REF="')[1].split('"')[0].split('@@')[1].split('#')
                  #    doc_xml_d[docid] = xml_lst
            meta_info   = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(docid)+'_meta_data'
            env         = lmdb.open(meta_info)
            taxoname_d  = {}
            with env.begin(write=True) as txn: 
               for key, value in txn.cursor():
                  table_id  = key.split('_')[0]
                  val_sp    = value.split(':#:')
                  for taxo_val in val_sp:
                      vv_split  = taxo_val.split(':@@:')
                      taxoname  = vv_split[0]
                      val       = vv_split[1] 
                      taxoname_d.setdefault(docid, {})[taxoname] = val
            if taxoname_d:
                table_dict[docid] = [doc_xml_d.get(docid, []), taxoname_d.get(docid, {})]
        return table_dict

    def delete_meta_deta(self, ijson):
        dealid      = ijson['deal_id']
        deal_id     = dealid.split('_')
        doc1        = ijson['doc_id']
        taxo_dict   = ijson['taxo_dict']
        exists_d    = {}
        table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env = lmdb.open(table_path)
        with env.begin() as txn: 
            for tableid, taxo in taxo_dict.items():
               val  = txn.get(tableid+'_TAXOS')
               if not val:continue
               for tt in val.split(':#:'):
                    if not tt:continue
                    tt  = tt.split(':@@:')
                    exists_d.setdefault(tableid, {})[tt[0]]= tt[1]
               #print key, val
        final_d = {}
        table_path  = '/var/www/html/output_file/'+str(deal_id[0])+'_'+str(deal_id[1])+'/'+str(doc1)+'_meta_data'
        env = lmdb.open(table_path, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn: 
            for tableid, taxo in taxo_dict.items():
               tmpd = exists_d.get(tableid, {})
               for key, val in taxo.items():
                  if key in tmpd:
                     del tmpd[key]
               final_d[tableid] = tmpd
               tmpar    = []
               for k, v in tmpd.items():
                   tmpar.append(k+':@@:'+v)
               taxo_val = ':#:'.join(tmpar)
               txn.put(tableid+'_TAXOS', taxo_val)            
        return final_d
    
    def process_delta_from_ui(self, ijson):
        dealid   = ijson['deal_id']
        deal_text= "/var/www/cgi-bin/table_delta/deal_ids.txt"
        deal_id  = dealid.split('_')
        finn     = open(deal_text, 'r')
        lines    = finn.readlines()
        finn.close()
        exit_deal= []
        for line in lines:
            if not line.strip():continue
            exit_deal.append(line.strip())
        if dealid in exit_deal:
             return ['Deal alredy exit'] 
        disableprint()
        fin = open(deal_text, 'a')
        fin.write(dealid)
        fin.close()
        res = 'not done'
        res = self.process_delta(dealid)
        enableprint()
        return res


    def process(self, cmd_id, ijson):
        resd    = []

        if 1 == cmd_id:
            resd    = self.get_delta_user_info(ijson)

        elif 2 == cmd_id:
            resd    = self.save_final_pair_info(ijson)

        elif 3 == cmd_id:
            resd    = self.remove_user_delta(ijson)

        elif 4 == cmd_id:
            resd    = self.get_company_deal_info()

        elif 5 == cmd_id:
            resd    = self.login_info(ijson)

        elif 6 == cmd_id:
            resd    = self.indexing_page()

        elif 7 == cmd_id:
            resd    = self.get_deal_info_lmdb(ijson)

        elif 8 == cmd_id:
            resd    = self.unmatch_user_delta(ijson)

        elif 9 == cmd_id:
            resd    = self.process_delta_user(ijson)

        elif 10 == cmd_id:
            resd    = self.read_excel_status()

        elif 11 == cmd_id:
            resd    = self.read_info_table(ijson)

        elif 12 == cmd_id:
            resd    = self.save_taxo_info(ijson)

        elif 13 == cmd_id:
            resd    = self.get_doc_ides_info(ijson)

        elif 14 == cmd_id:
            resd    = self.read_all_meta_data(ijson)

        elif 15 == cmd_id:
            resd    = self.delete_meta_deta(ijson)
        elif 16 == cmd_id:
            resd    = self.re_order_delta_lines(ijson)

        elif 17 == cmd_id:
            resd    = self.process_delta_from_ui(ijson)
        elif 120 == cmd_id:
            resd    = self.read_info_table(ijson)
            tmparr  = []
            for k, v in resd[0].items():
                tmparr.append({'id': k, 'name': v['table'], 'value': '2'})
            resd    = tmparr
        return resd

if __name__ == '__main__':
    obj = Index()
    doclist = []
    if len(sys.argv) > 2:
        doclist = sys.argv[2].split('#')
    obj.process_delta(sys.argv[1], doclist)
