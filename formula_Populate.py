#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys, time 
import datetime
import traceback
import multiprocessing
queue = multiprocessing.JoinableQueue()
db_queue = multiprocessing.JoinableQueue()
import traceback
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class Task(object):
    def __init__(t_self, row_id, project_id, company_name, doc_id, page_no, grid_id):
        t_self.row_id  = row_id
        t_self.project_id  = project_id
        t_self.company_name  = company_name
        t_self.doc_id  = doc_id
        t_self.page_no  = page_no
        t_self.grid_id  = grid_id       

def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line

import change_wrapper as cw
cw_Obj    = cw.Wrapper()

def ProcessHandler(thisObj,cpucore):
    import os
    import gc

    os.system("taskset -c -p %d %d" % (cpucore,os.getpid()))
    while 1:
        if not  queue.empty():
            item = queue.get()
            if item == 'STOP':
                break
            thisObj.update_status(item.row_id, 'P', 'N')
            try:
                doc_info = {item.doc_id:['%s-%s'%(item.page_no, item.grid_id)]}
                ijson = {'row_id':item.row_id, 'project_id':item.project_id, 'company_name':item.company_name, 'doc_info':doc_info}
                cw_Obj.formula_populate(ijson)
            except:
                print_exception()
                thisObj.update_status(item.row_id, 'E', 'N')
            else:
                thisObj.update_status(item.row_id, 'Y', 'N')
        else:
            time.sleep(2)

class Render():
    def __init__(self, core_count=2):
        self.core_count = core_count
        self.dbinfo  = {'user':'root', 'password':'tas123', 'host':'172.16.20.229', 'db':'populate_info'}
        return

    def get_connection(self, dbinfo):
        return conn_obj.MySQLdb_connection(dbinfo)

    def __get_company_ids(self, company_ids, first=''):
        conn, cur = self.get_connection(self.dbinfo)
        sql = """ SELECT row_id, project_id, company_name, doc_id, page_no, grid_id FROM formula_populate_info WHERE (populate_status in ('N') AND queue_status in ('N') or populate_status in ('Y', 'E') AND queue_status in ('Q')); """  
        cur.execute(sql)
        t_data = cur.fetchall()
        cur.close()
        conn.close()
        row_dct   = {}
        for row in t_data:      
            row_id, project_id, company_name, doc_id, page_no, grid_id = map(str, row)
            row_dct[row_id] = {'r':row_id, 'project_id':project_id, 'company_name':company_name, 'doc_id':doc_id, 'page_no':page_no, 'grid_id':grid_id}
        return row_dct

    def update_status(self, row_id, populate_status, queue_status):
        conn, cur = self.get_connection(self.dbinfo)
            
        if populate_status in ('Y', 'E'):
            read_qry = """ SELECT queue_status FROM formula_populate_info WHERE row_id='%s' """%(row_id)
            print read_qry
            cur.execute(read_qry)
            t_d = cur.fetchone()
            if t_d[0] == 'Q':
                queue_status = t_d[0]
        update_stmt =  """ UPDATE formula_populate_info SET populate_status='%s', queue_status='%s' WHERE row_id='%s' """%(populate_status, queue_status, row_id)            
        print update_stmt
        #print update_stmt;sys.exit()
        cur.execute(update_stmt)
        conn.commit()
        cur.close()
        conn.close()
        return 
        
    def run(self):
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
            self.procs[-1].daemon = True
            self.procs[-1].start()
        first   = 'first'
        while 1:
            alive_count = 0
            for i in range(self.core_count):
                if self.procs[i].is_alive():
                    alive_count += 1
                else:
                    print 'RESTART PROCESS ', i
                    self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
                    self.procs[-1].daemon = True
                    self.procs[-1].start()
            if alive_count == 0 and docids:break
            print 'Formula Alive Count ', alive_count, ' / ',self.core_count
            doc_info_dct   = self.__get_company_ids([], first)
            first   = ''
            if doc_info_dct:
                #self.update_status(map(doc_info_dct, 'Q')
                for row_id, info_dct in doc_info_dct.iteritems():
                    #{'r':row_id, 'project_id':project_id, 'company_name':company_name, 'doc_id':doc_id, 'page_no':page_no, 'grid_id':grid_id}
                    project_id, company_name = info_dct['project_id'], info_dct['company_name']
                    doc_id, page_no, grid_id = info_dct['doc_id'], info_dct['page_no'], info_dct['grid_id']
                    task    = Task(row_id, project_id, company_name, doc_id, page_no, grid_id)
                    queue.put(task)
            time.sleep(10)

if __name__ == '__main__':
    obj = Render(int(sys.argv[1]))
    obj.run()






