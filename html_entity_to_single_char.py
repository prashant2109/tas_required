"""ents.pty
Convert SGML character entities into Unicode

Function taken from:
http://stackoverflow.com/questions/1197981/convert-html-entities-to-ascii-in-python/1582036#1582036

Thanks agazso!
"""
import re
import htmlentitydefs
try:
    from HTMLParser import HTMLParser
except ImportError:
    from html.parser import HTMLParser
h_parser = HTMLParser()
empty_space = {
    '&#12288;'  :1,
    '&#8197;'   :1
}
class html_entity_to_single_char:
	def convert(self, s):
            for e_s in empty_space.keys():
                s   = s.replace(e_s, ' ')
            s   = ' '.join(s.split())
            s   = h_parser.unescape(s)  
            return s
	    """Take an input string s, find all things that look like SGML character
	    entities, and replace them with the Unicode equivalent.
	    Function is from:
	    http://stackoverflow.com/questions/1197981/convert-html-entities-to-ascii-in-python/1582036#1582036
	    """
	    matches = re.findall("&#\d+;", s)
	    if len(matches) > 0:
		hits = set(matches)
		for hit in hits:
		    name = hit[2:-1]
		    try:
			entnum = int(name)
			s = s.replace(hit, unichr(entnum))
		    except ValueError:
			pass
	    matches = re.findall("&\w+;", s)
	    hits = set(matches)
	    amp = "&"
	    if amp in hits:
		hits.remove(amp)
	    for hit in hits:
		name = hit[1:-1]
		if name in htmlentitydefs.name2codepoint:
		    s = s.replace(hit,
				  unichr(htmlentitydefs.name2codepoint[name]))
	    s = s.replace(amp, "&")
	    return s

if __name__=="__main__":
    obj = html_entity_to_single_char()

    #mystr = "&#163; &#65510;, &#8211;Circuit Issues Split &#917;  Decisions on PTO Continuation Rules,&#8221; Banner &amp; Witcoff IP UPDATE" # utf-8

    #print mystr 
    #print ' + ', len(mystr)
    mystr = '(&#24179;&#25104;24&#24180;&#65299;&#12105;31&#12103;)'
    convert_mystr = obj.convert(mystr)
    print [convert_mystr ]

