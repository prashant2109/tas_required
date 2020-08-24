import os, sys, lmdb
# '/var/www/html/fundamentals_intf/output/'

class ImageCoordinates(object):
    def __init__(self, company_id):
        self.company_id = company_id 
        self.project_id, self.deal_id   = company_id.split('_')
        self.deal_wise_doc_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/%s/output/'%(self.deal_id)
        self.lmdb_path = '/var/www/html/fundamentals_intf/output/' + self.company_id  + '/doc_page_adj_cords_all' 

    def get_image_height_width(self, filename):
        from PIL import Image 
        width, height = '', '' 
        with Image.open(filename) as image: 
            width, height = image.size     
        return ['0', '0', width, height]
    
    def whole_deal_coordinates_gen(self, doc_ids=''):
        if not doc_ids:
            if os.path.exists(self.lmdb_path):
                os.system("rm -rf %s"%(self.lmdb_path)) 
            get_all_docs = os.listdir(self.deal_wise_doc_path)
        elif doc_ids:
            get_all_docs = doc_ids.split('#')
        env = lmdb.open(self.lmdb_path)
        txn = env.begin(write=True) 
        res_dct = {}
        for docu in get_all_docs[:8]:
            try:
                s = int(docu)
            except:continue
            pdf_page_path = self.deal_wise_doc_path + '%s/pages/'%(docu)
            save_image_path = self.deal_wise_doc_path + '%s/test_pdf_image_save/'%(docu)
            if not os.path.exists(save_image_path):
                os.system('mkdir -p %s'%(save_image_path))
            get_list_all_pages = os.listdir(pdf_page_path)
            page_dct = {}
            for page_pdf in get_list_all_pages[:5]:
                pg = page_pdf.split('.')[0]
                pdfPage_input_path = os.path.join(pdf_page_path, '%s'%(page_pdf))
                png_save_path = os.path.join(save_image_path, '%s.png'%(pg))
                cmd = "convert -flatten -density 300 %s  -resize 1836x2376 %s"%(pdfPage_input_path, png_save_path)
                print cmd
                os.system(cmd)
                get_cord = self.get_image_height_width(png_save_path)
                page_dct[pg] = get_cord
                print 
            txn.put(docu, str(page_dct))
        return 'done' 

if __name__ == "__main__":
    company_id = sys.argv[1]
    obj = ImageCoordinates(company_id)
    len_sys = len(sys.argv)
    if len_sys == 2: 
        obj.whole_deal_coordinates_gen() 
    if len_sys == 3:
        doc_ids = sys.argv[2]
        obj.whole_deal_coordinates_gen(doc_ids)





 
