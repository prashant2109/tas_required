import urllib, sys
import urllib2
from urlparse import urlparse, urlunparse, parse_qsl

class Request():
    def load_url(self, url, timeout=60):
        txt1    = ''
        url = urllib.unquote(url);
        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        #print query;sys.exit('MT')
        url_parts[4] = urllib.urlencode(query)
        print '>>>', url_parts;sys.exit('MT')
        new_url = urlunparse(url_parts)
        res = urllib2.urlopen(new_url, timeout=timeout)
        status_code = res.getcode()
        txt1 = res.read()
        txt = "OK"
        '''
        except Exception, e:
            if type(e).__name__ == 'HTTPError':
                status_code = e.getcode()
                txt = "httperr"
            elif type(e).__name__ == 'timeout':
                status_code = -1
                txt = "timeout"
            else:
                txt = "othererr"
                print dir(e)
        '''
        return txt, txt1

if __name__ == '__main__':
    r_Obj = Request()
    url = 'https://docs.python.org/2/library/urlparse.html'
    print r_Obj.load_url(url)
        

