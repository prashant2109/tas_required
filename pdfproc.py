import sys, os, lmdb, sets, hashlib
from cStringIO import StringIO
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter

class process_pdfs_pgnumber:

    def isnumber(self, x):

        #ar = range(1900, 3000) 
        #new_ar = map(lambda x:str(x), ar[:])
        #if x in new_ar:
        #   return 0 


        if '%' in x: return 0
        if '/' in x: return 0

        try:
            int_x = int(x)
        except:
            int_x = -100000000
        if (1900 <= int_x) and (int_x <= 3000):
           return 0

        flg = 0
        num_flg = 0
        for e in x:
            if e in '0123456789':
               num_flg = 1
            if e in '.,()-%':
               continue
            if e.lower() in 'abcdefghijklmnopqrstuvwxyz':
               num_flg = 0
               flg = 0
               return 0

        if len(x.split()) > 2:
           return 0
        if len(x.split('-')) > 2:
           return 0

        if len(x.split()) == 2:
           n1 = x.split()[0]   
           n2 = x.split()[1]
           flg1 = 0
           flg2 = 0 
           for e in '0123456789':
               if (flg1==0) and (e in n1):
                  flg1 = 1   
               if (flg2==0) and (e in n2):
                  flg2 = 1   
               if flg1 and flg2:  break
           if (flg1 and flg2):
               return 0
           
        if num_flg:
           return 1         
        else:
           return 0 

    def replace_str(self, x):
        r_ar = [ ('\xe2\x80\x93', '-'), ('\xc2\xa0', ' '), ('\xef\xac\x81', 'fi')] 
        for e in r_ar:
            x = x.replace(e[0], e[1]) 
        return x.strip()   




    def pdf_to_text(self, pdfname):
        # PDFMiner boilerplate
        rsrcmgr = PDFResourceManager()
        sio = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        # Dictionary for page numbers as keys and it's page contents as values
        page = {}
        print "Reading through " + pdfname
        print os.getcwd()
        # Extract text
        with open(pdfname, 'rb') as fp:
            for page_num, contents in enumerate(PDFPage.get_pages(fp)):
                sio = StringIO()
                device = TextConverter(rsrcmgr, sio, codec=codec, laparams=laparams)
                interpreter = PDFPageInterpreter(rsrcmgr, device)
                contents = interpreter.process_page(contents)
                # Get text from StringIO
                text = sio.getvalue()
                page[page_num+1] = text
            fp.close()
            # Cleanup
            device.close()
            sio.close()
        return page



    def pdfparser_store(self, all_pdfpath, lmdb_path):
        write_flg = 1
        all_pdfs = os.listdir(all_pdf_paths)
        os.system('rm -rf '+ lmdb_path) 
        os.system('mkdir -p '+ lmdb_path) 
        if write_flg:
            env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
            with env.begin(write=True) as txn: 
                 txn.put('ALLDOCS', ':#:'.join(all_pdfs)) 
        else: 
            print 'Writing'   , ('ALLDOCS', ':#:'.join(all_pdfs)) 


        k_all_pdfs  = all_pdfs[:]


        ngram_dict = {}
        all_docs = []
        data_map_dict = {}
        pg_docs = {}
        for pdfname in  k_all_pdfs: 
            all_docs.append(pdfname)
            print pdfname
            pdfpath = all_pdfpath+pdfname
            page_dict = self.pdf_to_text(pdfpath)
            pg_docs[pdfname] = page_dict.keys() 

            num_map_dict = {} 
            data_map_dict[pdfname] = {}

            for pg_number in page_dict.keys():
                data = page_dict[pg_number]
                mkey = pdfname+':'+str(pg_number)+':DATA'
                if  write_flg:
                    env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
                    with env.begin(write=True) as txn: 
                         txn.put(mkey, data)
                    data_ar = filter(lambda x:x.strip(), data.split('\n')[:])
                    data_ar = map(lambda x:' '.join(x.split()).strip(), data_ar[:])

                    if  1:
                        for data_elm in data_ar:
                            data_elm = data_elm.lower() 
                            data_elm_sp = data_elm.split()
                            for i in range(0, len(data_elm_sp)):
                                for d in range(1, 10):
                                    if (i+d <= len(data_elm_sp)):
                                       ngram_t = ' '.join(data_elm_sp[i:i+d])
                                       #print ngram
                                       ngram = hashlib.md5(ngram_t).hexdigest()   
                                       if ngram not in ngram_dict:
                                          ngram_dict[ngram] = {}
                                       ngram_dict[ngram][(pdfname,pg_number )] = 1
                                    
                    num_ar = filter(lambda x:self.isnumber(x), data_ar[:])
                    rem_data_ar = list(sets.Set(data_ar) - sets.Set(num_ar))
                    rem_data_ar = map(lambda x:x.lower(), rem_data_ar[:])
                    num_ar = map(lambda x:''.join(x.split()).strip(), num_ar[:])
                    mkey1 = pdfname+':'+str(pg_number)+':LABELS'
                    mkey2 = pdfname+':'+str(pg_number)+':NUM'
                    env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
                    with env.begin(write=True) as txn: 
                         txn.put(mkey1, str(rem_data_ar))
                         txn.put(mkey2, str(num_ar))
                    data_map_dict[pdfname][pg_number] = { 'label_ar':rem_data_ar, 'num_ar':num_ar }
                    #print '========================================' 
                    #print pdfname, pg_number 
                    #print rem_data_ar
                    #print num_ar 
                else: 
                    print 'Writing', (mkey, data) 
 
        if 1:   
            for fname, pages in pg_docs.items():
                mkey = fname+':ALLPAGES'
                if write_flg:
                    env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
                    with env.begin(write=True) as txn: 
                         pages = map(lambda x:str(x), pages[:])
                         txn.put(mkey, '#'.join(pages))
                else:
                    print 'Writing', (mkey, '#'.join(pages))


        print 'Writing NGrams 1 - 5'
        env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
        with env.begin(write=True) as txn: 
             for ngram, vs_dict  in  ngram_dict.items():
                 txn.put(ngram+':HASH', str(vs_dict.keys()))

        inter_data = {}  
        for pindex, pdfname1 in  enumerate(k_all_pdfs):
            pages1 = data_map_dict[pdfname1].keys() 
            for pdfname2 in  k_all_pdfs[pindex+1:]:
                print pdfname1, ' vs ', pdfname2  
                pages2 = data_map_dict[pdfname2].keys()
                for p1 in pages1:
                    ls1 = data_map_dict[pdfname1][p1]['label_ar']
                    ns1 = data_map_dict[pdfname1][p1]['num_ar']
                    for p2 in pages2:
                        ls2 = data_map_dict[pdfname2][p2]['label_ar']
                        ns2 = data_map_dict[pdfname2][p2]['num_ar']
                        ls_inter = list(sets.Set(ls1).intersection(sets.Set(ls2)))
                        ns_inter = list(sets.Set(ns1).intersection(sets.Set(ns2)))
                        if  ls_inter or ns_inter:
                            inter_data[(pdfname1, str(p1), pdfname2, str(p2))] = [ ls_inter[:], ns_inter[:] ]
                            print '==============================================================================================='
                            print 'adding: ', (pdfname1, str(p1), pdfname2, str(p2)), len(ls_inter), len(ns_inter)
                            print ls_inter
                            print ns_inter  
                             
        
        print ' Storing LMDB' 
        env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
        with env.begin(write=True) as txn: 
             for k, vs in inter_data.items():         
                 mykey1 = k[0]+'-'+k[1]+'-'+k[2]+'-'+k[3]+':LABELS'
                 txn.put( mykey1, str(vs[0]))
                 mykey2 = k[0]+'-'+k[1]+'-'+k[2]+'-'+k[3]+':NUM'
                 txn.put(mykey2, str(vs[1]))
             txn.put('ALLPAIRSCORE', str(inter_data.keys())) 
        return 
 


    def decrypt_pdfs(self, pdfpath, decryptpdfpath):
       
        os.system('rm -rf '+decryptpdfpath)  
        os.system('mkdir -p '+decryptpdfpath)  
        k_all_pdfs = os.listdir(pdfpath)


               
        for pdfname in  k_all_pdfs: 
            pdfpath1 = pdfpath+pdfname
            dpdfpath = decryptpdfpath+pdfname
            os.system('qpdf --password=%s --decrypt %s %s' %('', pdfpath1, dpdfpath) ) 
            #print 'qpdf --password=%s --decrypt %s %s' %('', pdfpath1, dpdfpath)  
             

    def form_pair_scoring(self, lmdb_path):
        env = lmdb.open(lmdb_path)
        with env.begin() as txn: 
             all_docs = txn.get('ALLPAIRSCORE') 
             if not all_docs:
                all_docs = []
             else:
                all_docs = eval(all_docs)  

        score_pos_dict = {}
        score_tup = [] 
        all_docs = map(lambda x:list(x), all_docs[:])
        all_docs = map(lambda x:x[0]+'-'+x[1]+'-'+x[2]+'-'+x[3], all_docs[:])
        env = lmdb.open(lmdb_path)
        with env.begin() as txn: 
             for k in all_docs:
                 mykey1 = k+':LABELS' 
                 mykey2 = k+':NUM' 
                 ls_inter = txn.get(mykey1, '[]')
                 ls_inter = eval(ls_inter)
                 ns_inter = txn.get(mykey2, '[]')
                 ns_inter = eval(ns_inter)
                 #print '=============================================='
                 #print mykey1
                 #print ls_inter 
                 #print mykey2
                 #print ns_inter
                 score = (len(ns_inter), len(ls_inter))
                 k_elm1 = '-'.join(k.split('-')[:2])   
                 k_elm2 = '-'.join(k.split('-')[2:])
                 #print k_elm1, k_elm2, score
                 #score_tup.append((score, k_elm1, k_elm2))

                 if k_elm1 not in score_pos_dict:
                    score_pos_dict[k_elm1] = []
                 score_pos_dict[k_elm1].append((score, k_elm2))
                 
                 if k_elm2 not in score_pos_dict:
                    score_pos_dict[k_elm2] = []
                 score_pos_dict[k_elm2].append((score, k_elm1))

        print 'Scoring Poss'
        env = lmdb.open(lmdb_path, map_size=20*1024*1024*1024*1024)
        with env.begin(write=True) as txn: 
             for k, vs in score_pos_dict.items():         
                 txn.put(k+':SCORE', str(vs))
                       
                 



         

if __name__ == "__main__":
      
    obj =  process_pdfs_pgnumber()
    if 1:
       #comp_id = 'Norwegianairshuttle'
       #comp_id = 'NOKAIRLINES'
       #comp_id = 'JETAIRWAYSINDIALTD'
       #comp_id = 'LatamAirlines'
       #comp_id = 'CIEFINANCIERERICHEMONTSA'
       #comp_id = 'AnheuserBuschInBev'
       #comp_id = 'ABBLTD'
       comp_id = 'HennesandMauritzAB'
       comp_id = 'Amazon'
       #all_pdf_paths = '/var/www/html/TASFundamentals_kpi/tasfms/data/output/company_info/'+comp_id+'_DECRYPT/'
       all_pdf_paths = '/var/www/html/TASFundamentals_kpi/tasfms/data/output/company_info/'+comp_id+'/'
       store_path = '/mnt/eMB_db_kpi/'+comp_id+'/1/rawpdfdata/' 
       obj.pdfparser_store(all_pdf_paths, store_path)
       obj.form_pair_scoring(store_path)
    if 0:
       comp_id = 'DEUTSCHEPOST'
       all_pdf_paths = '/var/www/html/TASFundamentals_kpi/tasfms/data/output/company_info/'+comp_id+'/'
       de_paths = '/var/www/html/TASFundamentals_kpi/tasfms/data/output/company_info/'+comp_id+'_DECRYPT/'
       obj.decrypt_pdfs(all_pdf_paths, de_paths)
       
       
