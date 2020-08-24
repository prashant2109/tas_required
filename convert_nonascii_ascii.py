#!/usr/bin/python
# -*- coding:utf-8 -*-
import cgi

class convert:

    def unicodeToHTMLEntities(self, text):
        """Converts unicode to HTML entities.  For example '&' becomes '&amp;'."""
        text = cgi.escape(text).encode('ascii', 'xmlcharrefreplace')
        return text

    def convertNonAsciiToHTMLEntities(self, text):
        text = text.replace('&amp;', '&')
        text = text.replace('&AMP;', '&')
        text = text.replace('&QUOT;', '"')
        new_txt = ""
        error_flg = 0  
        find_entities = {}
        for x in text:
            if ord(x)==ord('&'):
               new_txt += '&amp;'       
            elif ord(x) < 128:
                new_txt += x
            else:
                flg = 0   
                try:
                    elm =  self.unicodeToHTMLEntities(x)
                except:
                    flg = 1
                    elm = '&#'+str(ord(x))+';'
                if flg: 
                   find_entities[elm] = 0
                new_txt += elm #self.unicodeToHTMLEntities(x)
        new_txt = new_txt.replace('&amp;#', '&#') 
        new_txt = new_txt.replace('&amp;quot;', '&quot;') 
        new_txt = new_txt.replace('&#160;', ' ')
        new_txt = ' '.join(new_txt.strip().split())  
        cp1252_to_unicode_dict = {'&#92;':'&#2019;', '&#145;':'&#8216;', '&#146;':'&#8217;', '&#147;':'&#8220;', '&#148;':'&#8221;', '&#149;':'&#8226;', '&#150;':'&#8211;', '&#151;':'&#8212;', '&#152;':'&#732;', '&#153;':'&#8482;'} 
        for k, v in cp1252_to_unicode_dict.items():
            new_txt = new_txt.replace(k, v) 
            #try: 
            # find_entities[k] = 1   
        error_keys = [] 
        find_keys = find_entities.keys()
        for find_key in find_keys:
            flg = cp1252_to_unicode_dict.get(find_key, 0)
            if flg == 0:
               error_flg = 1
               error_keys.append(find_key) 
               break
        if error_flg:
           pass
           #print 'Error in Non Ascii...', new_txt, error_keys         
        return new_txt 


if __name__=="__main__":
  obj = convert()
  print obj.convertNonAsciiToHTMLEntities(u'Total stockholders\u2019 equity')  
