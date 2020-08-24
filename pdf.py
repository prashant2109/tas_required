import os, sys
from wand.image import Image
from wand.color import Color

page_page_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/96/output/1/pages'
get_all_page_pdf = os.listdir(page_page_path)
pdf_file_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/96/output/1/pages/'
pdf_file_path_save = '/var/www/html/TASFundamentalsV2/tasfms/data/output/1_common/data/96/output/1/test_pdf_image_save/'
if 0:    
    if not os.path.exists(pdf_file_path_save):
        os.system('mkdir -p %s'%(pdf_file_path_save))
    for fle in get_all_page_pdf[:10]:
        pg = fle.split('.')[0]
        file_name = os.path.join(pdf_file_path, fle)
        #print file_name;continue
        with Image(filename=file_name, resolution=216) as img:
            img.background_color = Color("white")
            img.alpha_channel = 'remove'
            save_file_path = os.path.join(pdf_file_path_save, '10_%s_page.png'%(pg))
            print save_file_path
            img.save(filename=save_file_path)

def get_image_height_width(filename):
    from PIL import Image 
    width, height = '', '' 
    with Image.open(filename) as image: 
        width, height = image.size     
    return ['0', '0', width, height]

if 0:
    res_dct = {}
    get_image_files = os.listdir(pdf_file_path_save)
    for im in get_image_files:
        image_file = os.path.join(pdf_file_path_save, im)
        get_cord = get_image_height_width(image_file)
        res_dct[im] = get_cord
    print res_dct
    
if 1:
    cmd = "convert -flatten -density 300 %s/1.pdf  -resize 1836x2376  %s%d.png"%(page_page_path, pdf_file_path_save, 1)
    os.system(cmd)
#print get_image_height_width('/tmp/4-200.png')


#company_name, model_number, company_id, machine_id = 'ArmstrongWorldIndustriesInc', '1', '1_95', '229'
#company_name, model_number, company_id, machine_id = 'TelephoneandDataSystems', '1', '1_94', '229'



 
