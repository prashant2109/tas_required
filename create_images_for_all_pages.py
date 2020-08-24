import os, sys, lmdb
# '/var/www/html/fundamentals_intf/output/'
deal_id = sys.argv[1]
deal_wise_doc_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/%s/output/'%(deal_id)
get_all_docs = os.listdir(deal_wise_doc_path)

def get_image_height_width(filename):
    from PIL import Image 
    width, height = '', '' 
    with Image.open(filename) as image: 
        width, height = image.size     
    return ['0', '0', width, height]

if 1:
    lmdb_path =  '/var/www/html/fundamentals_intf/output/' + '1_%s'%(deal_id) + 'doc_page_adj_cords_all'
    env = lmdb.open(lmdb_path)
    txn = env.begin(write=True) 
    res_dct = {}
    for docu in get_all_docs[:3]:
        try:
            s = int(docu)
        except:continue
        pdf_page_path = deal_wise_doc_path + '%s/pages/'%(docu)
        save_image_path = deal_wise_doc_path + '%s/test_pdf_image_save/'%(docu)
        if not os.path.exists(save_image_path):
            os.system('mkdir -p %s'%(save_image_path))
        get_list_all_pages = os.listdir(pdf_page_path)
        page_dct = {}
        for page_pdf in get_list_all_pages:
            pg = page_pdf.split('.')[0]
            pdfPage_input_path = os.path.join(pdf_page_path, '%s'%(page_pdf))
            png_save_path = os.path.join(save_image_path, '%s.png'%(pg))
            cmd = "convert -flatten -density 300 %s  -resize 1836x2376 %s"%(pdfPage_input_path, png_save_path)
            print cmd
            os.system(cmd)
            get_cord = get_image_height_width(png_save_path)
            page_dct[pg] = get_cord
            print 
         txn.put(docu, str(page_dct))
 
