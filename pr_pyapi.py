#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii
import lmdb, copy, json, ast 
import datetime, sqlite3, MySQLdb
import redis
from urllib import unquote
#import numpy as np
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config
import json
from urllib import unquote
from urlparse import urlparse
from url_execution import Request
import msgpack

def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class PYAPI():
    def __init__(self):
        pypath    = os.path.dirname(os.path.abspath(__file__))
        self.dateformat = {
                            'www.businesswire.com'      : ['%Y-%m-%d %H:%M:%S', '%B %d, %Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'], #[April 24, 2019 12:44:29]
                            'press.aboutamazon.com'     : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'www.sec.gov'               : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'pressroom.aboutschwab.com' : ['%m/%d/%y %H:%M %p %Z'],
                            }
        self.month_map  = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        pass

    def calll_ftp_data(self, project_id, ws_id):
        url = "http://172.16.20.10/cgi-bin/ftp_scanner/cgi_ftp_scanner.py?input_str={'project_id':%s, 'url_id':%s}" %(39, 1)
        content = urllib.urlopen(url).read()
        datadict = json.loads(content)
        return datadict

    def setup_new_url(self, ijson):
        project_id  = ijson.get("project_id")
        user_id     = ijson.get("user")
        urlname     = ijson.get("link")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        sql         = "select max(url_id)+1 from project_url_mgmt where project_id=%s"
        cur.execute(sql, project_id)
        res         = cur.fetchone()
        url_id = res[0]

        if url_id is None:
            url_id = 1

        sql2 = "insert into project_url_mgmt (project_id, url_id, urlname, status) values (%s,%s,%s,%s)"
        value = (project_id, url_id, urlname,"Y")
        cur.execute(sql2, value) 
        conn.commit()
        cur.close()
        conn.close()
        res = [{"message":"done","url_id":url_id}]
        return res
        

    def get_url_stats(self, ijson):
        project_id  = ijson.get("project_id","Amazon")
        user_id     = ijson.get("user_id","tas")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        sql         = "select project_id, url_id, urlname, status from project_url_mgmt where project_id = '%s'"
        cur.execute(sql % (project_id))
        res         = cur.fetchall()
        stats       = {}
        #{"src_date_time":"7/16/19 5:45 am PDT","Headline":"Schwab Reports Net Income of $937 Million, Up 8%, Posting the Strongest Second Quarter in Company History","File Description":"The Charles Schwab Corporation announced today that its net income for the second quarter of 2019 was $937 million, down 3% from $964 million for the prior quarter, and up 8% from $866 million for the second quarter of 2018. Net income for the six months ended June 30, 2019 was a record $1.9 billion, up 15% from the year-earlier period.\n\n\n\n\n\n \n\n\n\n\n \n\n\n\n\nThree Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n \n\n\n\n\nSix Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n\n\nFinancial Highlights\n\n\n\n\n \n\n\n\n\n2019\n\n\n\n\n  more...","linkid":"219","link":"https://pressroom.aboutschwab.com/press-release/corporate-and-financial-news/schwab-reports-net-income-937-million-8-posting-strongest"} 
        done_d  = {}
        year_d  = {}
        url_d   = {}
        overall = {}
        sn  = 1
        recent  = []
        done_url    = {}
        recent_data = {}
        #res+[()]
        doc_d   = 1
        for r in res:
            project_id, url_id, rurlname, status = r
            sql = "select record_id, urlname, meta_data, process_time from scheduler_download_common_new_%s_%s.link_mgmt_meta_data where active_status='Y'"%(project_id, url_id)
            cur.execute(sql)
            tmpres  = cur.fetchall()
            #stats.setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
            tmplst  = []
            urlname = rurlname
            udata = urlparse(urlname)
            domain  = udata.netloc.split('/')[-1]
            if (domain, udata.path) in done_url:
                url_id  = done_url[(domain, udata.path)]
            else:
                done_url[(domain, udata.path)]   = url_id
            for tmpr in tmpres:
                document_id, urlname, meta_data, process_time   = tmpr
                #print (url_id, domain, tmpr[0])
                document_id = str(doc_d)
                doc_d   += 1
                meta_data       = json.loads(unquote(meta_data).decode('utf8'))
                #print (url_id, urlname, meta_data)
                #print (meta_data["src_date_time"], self.dateformat[domain])
                src_date_time   = ''
                for frmt in self.dateformat[domain]:
                    try:
                        src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"]), frmt)
                    except:
                        try:
                            src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"].split()[0]), frmt.split()[0])    
                        except:pass
                    if src_date_time:break
                if not src_date_time:
                    print 'ERROR ', (url_id, meta_data["src_date_time"], domain, self.dateformat[domain])
                    continue
                tmplst.append((document_id, urlname, process_time, meta_data, src_date_time))
            urlname = rurlname
            urlname = urlname.decode("iso-8859-1")
            if isinstance(urlname, unicode):
                urlname = urlname.encode('utf-8')
            url_d.setdefault(url_id, {'n':domain, 'link':urlname, 's':status, 'sn':sn, 'uinfo':{}, 'info':udata.path})
            sn  += 1
            tmplst.sort(key=lambda x:x[4], reverse=True)
            stats   = {'total':{}, 'processed':{}, 'pending':{}, 'rejected':{}}
            for ii, tmpr in enumerate(tmplst):
                document_id, urlname, process_time, meta_data, src_date_time    = tmpr
                year            = int(src_date_time.strftime('%Y'))
                #print (url_id, domain, document_id, src_date_time)
                #print year
                if year < 2000:continue
                urlname = urlname.decode("iso-8859-1")
                if isinstance(urlname, unicode):
                    urlname = urlname.encode('utf-8')
                if ii == 0:
                    recent_data[url_id] = src_date_time
                    recent.append({'n':domain, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 'count':1, 'd':str(document_id), 'link':urlname})
                month           = src_date_time.strftime('%b')
                Headline        = meta_data['Headline']
                #desc            = meta_data['File Description']
                stats['total'][document_id]   = 1
                year_d.setdefault(year, {}).setdefault(month, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d[year][month][url_id]['total']    += 1
                year_d.setdefault('Overall', {}).setdefault(year, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d['Overall'][year][url_id]['total']    += 1
                done_sts    = 'N'
                if done_d.get(str(document_id)) == 'Y':
                    done_sts    = 'Y'
                    stats['processed'][document_id]   = 1
                    year_d[year][month][url_id]['processed']        += 1
                    year_d['Overall'][year][url_id]['processed']    += 1
                elif done_d.get(str(document_id)) == 'R':
                    stats['rejected'][document_id]   = 1
                    year_d[year][month][url_id]['rejected']        += 1
                    year_d['Overall'][year][url_id]['rejected']    += 1
                    done_sts    = 'R'
                else:
                    stats['pending'][document_id]   = 1
                    year_d[year][month][url_id]['pending']        += 1
                    year_d['Overall'][year][url_id]['pending']    += 1
                    done_sts    = 'P'
                #Headline    = Headline.decode("iso-8859-1")
                if isinstance(Headline, unicode):
                    Headline = Headline.encode('utf-8')
                dtype   = 'Link'
                if urlname.split('?')[0].split('.')[-1].lower() == 'pdf':
                    dtype   = 'PDF'
                #print 'YES'
                url_d[url_id]['uinfo'][str(document_id)]    = {'n':Headline, 'link':urlname, 'sn':ii+1, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 's':done_sts, 'dtype':dtype}
                url_d[url_id]['uinfo'][str(document_id)]['sn'] = len(url_d[url_id]['uinfo'].keys())
            if not url_d[url_id]['uinfo']:
                url_d[url_id]['s']  = 'N'
            if url_d[url_id]['s']  == 'Y':
                url_d[url_id]['s'] = 'success'
            elif url_d[url_id]['s']  == 'N':
                url_d[url_id]['s'] = 'danger'
            total   = len(stats['total'].keys())
            pending = len(stats['pending'].keys())
            processed = len(stats['processed'].keys())
            rejected = len(stats['rejected'].keys())
            pending_p       = 0
            processed_p     = 0
            rejected_p      = 0
            if total:
                pending_p   = int((float(pending)/total)*100)
                processed_p   = int((float(processed)/total)*100)
                rejected_p   = int((float(rejected)/total)*100)
            stats['pending_c']  = pending
            stats['pending_p']  = pending_p
            stats['processed_c']  = processed
            stats['processed_p']  = processed_p
            stats['rejected_c']  = rejected
            stats['rejected_p']  = rejected_p
            url_d[url_id]['stats']  = stats
        gdata   = []
        years   = filter(lambda x:x != 'Overall', year_d.keys())
        years.sort(reverse=True)
        years   = ['Overall']+years
        for y in years[:]:
            months  = self.month_map
            if y == 'Overall':
                months  = year_d[y].keys()
                months.sort(reverse=True)
            dd  = {'categories':months, 'series':[], 'n':y}
            for urlid in url_d.keys():
                tdd  = {'name':url_d[urlid]['n'], 'data':[]}
                for m in months:
                    tdd['data'].append(year_d[y].get(m, {}).get(urlid, {}).get('total', 0))
                dd['series'].append(tdd)
            gdata.append(dd)
        u_ar    = []
        urls    = url_d.keys()
        urls.sort(key=lambda x:recent_data.get(x, datetime.datetime.strptime('1900', '%Y')), reverse=True)
        for urlid in urls:
            u_ar.append(url_d[urlid])
            u_ar[-1]['uid'] = urlid
                
        return [{'message':'done','data':u_ar, 'ysts':gdata, 'recent':recent}]

    def dinsert_demo_doc(self, ijson):
        dbname =  ''
        projectid = ''
        dbinfo  = copy.deepcopy(config.Config.dbinfo)
        dbinfo['host']  = '172.16.20.10'
        dbinfo['db']    = ijson['dbname']
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        pass

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def insert_demo_doc(self, ijson):
        db_name            = ijson["db_name"]
        project_id   = ijson["project_id"]
        batch_dct    = ijson["batch_dct"]
        
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        batch_str = ', '.join(["'"+str(e)+"'" for e in batch_dct.keys()])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE batch in (%s)  """%(batch_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            try:
                get_cid = batch_dct[str(batch)]
            except:get_cid = "2"
            data_rows.append((str(project_id), str(get_cid), str(meta_data), str(doc_id), 'Y'))  

        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    def insert_demo_doc_no_batch(self):
        sys.exit()
        db_name  = 'FactSheet_Tree'        #ijson["db_name"]
        project_id =  '40'            #ijson["project_id"]
        doc_ids  =  ['104', '109', '110'] #   ['88', '87', '102', '103', '104', '107', '108', '109', '110']             #ijson["doc_lst"]
        company_row_id = '16' #"9" 

        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y'))  

        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    
    def add_companies(self):
        company_display_name = 'BristolMyersSquibbCompany'
        company_name= ''.join(company_display_name.split())
        meta_data = '{"company_name":"BristolMyersSquibbCompany", "deal_id":"88", "industry_type":"Pharmaceutical", "model_number":"1", "project_id":"1", "project_name":["Credit Model", "Schroders"]}'
        user_name = 'demo'
        insert_time = ''
        project_id = 'FE'        

        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        ins_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, insert_time) VALUES('%s', '%s', '%s', '%s', '%s'); """%(company_name, company_display_name, meta_data, user_name, insert_time)
        m_cur.execute(ins_stmt)
        m_conn.commit()
 
        read_qry = """ SELECT max(row_id) FROM company_mgmt;  """
        m_cur.execute(read_qry)
        crid   =  int(m_cur.fetchone()[0])
        
        insp_stmt = """ INSERT INTO project_company_mgmt(project_id, company_row_id, user_name, insert_time) VALUES('%s', '%s', 'demo', '');  """%(project_id, str(crid)) 
        m_cur.execute(insp_stmt)
        m_conn.commit()
        m_conn.close() 

    def insert_demo_doc_1(self):
        #sys.exit()
        project_id =  'FE'            #ijson["project_id"]
        doc_ids  =  ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44']
        company_row_id = '6' #"9" 
        
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%('Westjetairlinesltd', '1')
        m_conn = sqlite3.connect(db_path)
        m_cur  = m_conn.cursor()
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids]) 
        read_qry = """  SELECT doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to FROM company_meta_info WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to = row
            meta_data = {'doc_type':document_type, 'period':period, 'reporting_year':reporting_year, 'doc_name':doc_name, 'doc_release_date':doc_release_date, 'doc_from':doc_from, 'doc_to':doc_to}
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y'))  
        
        #for k in data_rows: 
        #    print k
        #    print 
        #sys.exit()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    
    def execute_url(self, ijson):
        j_ijson = json.dumps(ijson)
        import url_execution as ue
        u_Obj = ue.Request()
        url_info = 'http://172.16.20.10:5008/tree_data?input=[%s]'%(j_ijson)
        print url_info
        txt, txt1   = u_Obj.load_url(url_info, 120)
        print 'SSSSSS', (txt, txt1)
        data = json.loads(txt1)
        return data#[{'message':'done', 'data':data}]
    
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

    def doc_wise_meta_info(self, doc_list, db_name):
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_list])
        read_qry = """ SELECT doc_id, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        doc_meta_map_lst = []
        for row in t_data:
            doc_id, doc_name, doc_type = row[:-1]
            try:
                meta_data = eval(row[-1])
            except:meta_data = {}
            meta_data.update({'doc_name':doc_name, 'doc_type':doc_type})
            dt_dct = {'doc_id':str(doc_id), 'meta_data':meta_data, 'doc_type':doc_type}
            doc_meta_map_lst.append(dt_dct)
        return doc_meta_map_lst        
 
    def upload_document_info(self, ijson):
        doc_lst        =  ijson['doclist']
        project_id       =  str(ijson['project_id'])
        company_row_id   =  str(ijson['i_company_id'])
        db_name          =  ijson['db_name'] 
       
        doc_meta_map_lst = self.doc_wise_meta_info(doc_lst, db_name)
        #print doc_lst, project_id, company_row_id, db_name, doc_meta_map_lst;sys.exit()
         
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
            
        chk_read = """ SELECT project_id, company_row_id, doc_id FROM document_mgmt WHERE project_id='%s' AND company_row_id='%s' """%(project_id, company_row_id)
        m_cur.execute(chk_read)
        t_info = m_cur.fetchall()       
            
        info_chk_dct = {}
        for rw in t_info:
            pid, crid, dcid = map(str, rw)
            info_chk_dct[(pid, crid, dcid)] = 1

        insert_rows = []
        update_rows = []
        
        def reaf_page_info(path):
            no_pages=0;
            if os.path.exists(path):  
                f = open(path)
                no_pages = f.read()
                f.close()
            return no_pages
        
        for dc_info in doc_meta_map_lst:
            dc, meta, doc_type = str(dc_info['doc_id']),  str(dc_info['meta_data']), dc_info['doc_type']                  
            if meta == 'None':
                meta = '{}'

            if (project_id, company_row_id, dc) in info_chk_dct:
                update_rows.append((meta, project_id, company_row_id, dc))                

            elif (project_id, company_row_id, dc) not in info_chk_dct:
                if doc_type == 'PDF':
                    dc_page_path = '/var/www/html/WorkSpaceBuilder_DB_demo/%s/1/pdata/docs/%s/pdf_total_pages'%(project_id, dc)
                    if not os.path.exists(dc_page_path):
                        nop = '0'
                    elif os.path.exists(dc_page_path): 
                        nop = reaf_page_info(dc_page_path)
                elif doc_type == 'HTML':
                    nop = '1' 
                insert_rows.append((project_id, company_row_id, meta, dc, 'Y', nop))
        
        #print 'ss', insert_rows
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

    def read_all_company_info(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT row_id, company_name, company_display_name FROM company_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            company_row_id, company_name, company_display_name = row
            if company_row_id == 7:continue
            dt_dct = {'company_id':company_name, 'company_name':company_display_name, 'crid':company_row_id, 'flg':1}
            res_lst.append(dt_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res

    def get_projectid_info(self, project_name):
        ru_Obj = Request()
        s_json = {"user_id":"sunil","ProjectName":"%s"%(project_name), "oper_flag":3}
        as_json = json.dumps(s_json)
        url_info = """ http://172.16.20.10:5007/tree_data?input=[%s] """%(as_json)
        print url_info
        #sys.exit('MT')
        txt, txt1   = ru_Obj.load_url(url_info, 120)
        #print (txt, txt1, type(txt1))
        data = json.loads(txt1)
        #print data, type(data)
        info_dct = data[0]['data']
        project_id = int(info_dct.get('ProjectID', 0))
        if not project_id:
            return '', ''
        user_id    = str(info_dct['UserID'])
        p_name     = str(info_dct['ProjectName'])
        db_name = '_'.join(p_name.split())

        ###################  DB creation ##################
        # {"user_id":"sunil","ProjectID":44,"WSName":"1","db_name":"muthu_test_proj","oper_flag":90014} 
        d_json = {"user_id":"%s"%(user_id),"ProjectID":"%d"%(project_id),"WSName":"1","db_name":"%s"%(db_name),"oper_flag":90014}
        ad_json = json.dumps(d_json) 
        d_url_info = """ http://172.16.20.10:5007/tree_data?input=[%s] """%(ad_json)
        #print d_url_info
        dtxt, dtxt1   = ru_Obj.load_url(d_url_info, 120)
        #print (dtxt, dtxt1)
        d_data = json.loads(dtxt1)
        d_info_dct = d_data[0]['data'] 
        d_db_name = d_info_dct['DBName']
        return project_id, db_name

    def call_module_mgmt_user_save(self, ijson, project_id):
        dc_ijson =  {}
        dc_ijson['data']          =   ijson['data']
        dc_ijson['project_id']    =   project_id
                 
        import module_storage_info_project_company_mgmt as mpyf
        m_Obj = mpyf.PC_mgmg()
        m_Obj.user_save(dc_ijson)
        return 

    def project_configuration(self, ijson):
        if ijson.get('PRINT', 'N') != 'Y':
            disableprint() 
        pc_data = ijson['pc_data']
        project_name = pc_data['project_name'] 
        description  = pc_data['desc'] 
        user_name    = str(ijson['user'])
        project_id, db_name =  self.get_projectid_info(project_name)
        project_id, db_name = map(str, [project_id, db_name])
        print project_id, db_name
        if not project_id:
            return [{'message':'Project Already Exists'}]            
        dt_time = str(datetime.datetime.now())
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        insert_stmt = """ INSERT INTO project_mgmt(project_id, project_name, description, user_name, insert_time, db_name) VALUES('%s', '%s', '%s', '%s', '%s', '%s')  """%(project_id, project_name, description, user_name, dt_time, db_name) 
        print insert_stmt
        m_cur.execute(insert_stmt)

        '''
        insert company info for respective company
        '''
        
        project_company_info = pc_data['info']
        #print project_company_info;sys.exit()
        insert_rows = []
        for row_dct in project_company_info:
            crid = str(row_dct['crid'])
            dt_tup = (project_id, crid, user_name, dt_time)
            insert_rows.append(dt_tup)
        if insert_rows:
            ins_stmt = """ INSERT INTO project_company_mgmt(project_id, company_row_id, user_name, insert_time) VALUES(%s, %s, %s, %s)   """
            m_cur.executemany(ins_stmt, insert_rows)
            m_conn.commit()
        m_conn.close()
        self.call_module_mgmt_user_save(ijson, project_id) 
        enableprint()
        res = [{"message":"done", 'project_id':project_id}]
        return res        
            
    def read_xl_using_openpyxl(self):
        file_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Demo/pysrc/xl_files'  
        file_name = 'Factsheet120Docs'
        import xlsxReader as xl_rd
        xl_Obj = xl_rd.xlsxReader()
        xl_data = xl_Obj.process(file_path, file_name) 
        return xl_data 

    def get_redis_conn(self, redis_ip, redis_port):
        r = redis.Redis( host= redis_ip, port= redis_port, password='')
        return r
    
    def insert_urlname_info(self):  
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
            
        read_qry = """ SELECT project_id, company_row_id, url_id FROM project_company_mgmt WHERE url_id IS NOT NULL  """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        ijson = {} 
        company_wise_urls_lst = []
        for row in t_data:
            p_id, company_row_id, url_id = map(str, row)
            #if 'p_id' == 'FE':continue
            project_id  = copy.deepcopy(url_id)
            user_id     = ijson.get("user_id","tas")
            r = self.get_redis_conn(config.Config.dbinfo['host'], config.Config.dbinfo['redis_port'])
            pkey = 'webextract_%s_urlids' %(project_id)
            ddict  = r.hgetall(pkey)
            res = []
            maxid   = 0
            for d in sorted(ddict.keys()):
                val         = ddict.get(d)
                cdict       = msgpack.unpackb(val.decode("hex"), raw=False)
                urlname     = str(cdict.get('urlname', ''))
                if (company_row_id, urlname, project_id, 'Y', user_id) not in company_wise_urls_lst:
                    company_wise_urls_lst.append((company_row_id, urlname, project_id, 'Y', user_id)) 

        #print company_wise_urls_lst;sys.exit('M T')
        #ins_stmt = """  INSERT INTO url_name_info(company_row_id, url_name, client_url_id, status, user_id) VALUES(%s, %s, %s, %s, %s)  """
        #m_cur.executemany(ins_stmt, company_wise_urls_lst)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def scheduler_info(self):  
        pass

if __name__ == '__main__':
    obj = PYAPI()
    #print obj.read_xl_using_openpyxl()        
    #print obj.insert_urlname_info()
    print obj.scheduler_info()
