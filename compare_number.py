import sys
class Comp():
    def __init__(self):
        self.scale_map_d = {
            '1':'One', 'TH':'Thousand', 'TENTHOUSAND':'TenThousand', 'Mn':'Million', 'Bn':'Billion', 'KILO':'Thousand','Ton':'Million','Tn':'Million', 'Mn/Ton':"Million",
            'TH/KILO':"Thousand",
        }

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

    def compare_number(self, num1, num2, ph_csv1, ph_csv2):
        num_obj = {'One': 1, 'Dozen': 12, 'Hundred': 100, 'Thousand': 1000, 'Million': 1000000, 'Billion': 1000000000, 'Trillion': 1000000000000}
        if not ph_csv1 or not ph_csv2: 
            return num1 == num2
        if ph_csv1 == ph_csv2:
            return num1 == num2
        if num_obj[self.scale_map_d[ph_csv2]] > num_obj[self.scale_map_d[ph_csv1]]:
            num1, xx    = self.convert_frm_to(num1, self.scale_map_d[ph_csv1]+' - '+self.scale_map_d[ph_csv2], ph_csv1)
            num1    = self.convert_floating_point(num1).replace(',','')
            num2    = self.convert_floating_point(num2).replace(',','')
            return num1 == num2 
        if num_obj[self.scale_map_d[ph_csv1]] > num_obj[self.scale_map_d[ph_csv2]]:
            num2, xx    = self.convert_frm_to(num2, self.scale_map_d[ph_csv2]+' - '+self.scale_map_d[ph_csv1], ph_csv2)
            num2    = self.convert_floating_point(num2).replace(',','')
            num1    = self.convert_floating_point(num1).replace(',','')
            return num1 == num2 
        return num1 == num2

if __name__ == '__main__':
    obj = Comp()
    num1, num2   = float(sys.argv[1]), float(sys.argv[3])
    s1, s2      = sys.argv[2], sys.argv[4]
    print obj.compare_number(num1, num2, s1, s2)
                
        
