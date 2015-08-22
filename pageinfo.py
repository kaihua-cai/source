#!/usr/bin/python2.7

import urllib2
from bs4 import BeautifulSoup
import re

import unicodedata
def cleanUnicode(tmp):
    if type(tmp) != str :
	tmp = unicodedata.normalize('NFKD', tmp).encode('ascii','ignore')
    tmp = tmp.replace("'", ' ').replace('"', ' ').replace("\n", ' ').replace('%', ' ').strip()
    return re.sub('\s+', ' ', tmp)
	
class PageInfo(object):
    """Downloads content from URL, extracts title, meta description
    and anchors titles

    """
    def __init__(self, domain):
        self.title = ''
        self.meta_description = ''
        self.meta_keywords = []
        self.ref_titles = []
        self.img_alt_titles = []
        self.domain = domain
	self.url = "http://" + domain
        self.content = []
        self.load()

    def load(self):
        page = urllib2.urlopen(self.url, timeout=20)
        self.parse_page(page)

    def parse_page(self, html_stream):
        soup = BeautifulSoup(html_stream)
        ref_titles = []
        img_alt_titles = []
        for a in soup.find_all('a'):
            title = a.get_text().strip()
            if title and not title.isdigit():
                ref_titles.append(cleanUnicode(title))
        self.ref_titles = ref_titles
        for img in soup.find_all('img'):
            if img.attrs.has_key('alt'):
                title = img.attrs['alt'].strip()
                if title and not title.isdigit():
                    img_alt_titles.append( cleanUnicode(title))
        self.img_alt_titles = img_alt_titles
        if soup.title is not None and soup.title.string is not None:
            self.title = cleanUnicode(soup.title.string)
        for m in soup.find_all('meta'):
            attrs = m.attrs
            if attrs.has_key('name'):
                if attrs['name'].lower().startswith('descr'):
                    if attrs.has_key('content'):
                        self.meta_description = cleanUnicode( attrs['content'].strip())
                if attrs['name'].lower().startswith('keywords'):
                    if attrs.has_key('content'):
                        self.meta_keywords = [kw.strip().strip(',')
                                            for kw in attrs['content'].split()]
	for cont in soup.find_all('p'):
	    self.content.append(cleanUnicode(cont.text))
	self.soup = soup
        del soup

    def to_dict(self):
        j = {
            'title': self.title,
            'meta_description': self.meta_description,
            'meta_keywords': self.meta_keywords,
            'ref_titles': self.ref_titles,
            'img_alt_titles': self.img_alt_titles,
            'domain': self.domain,
	    'content': self.content	
        }
        return j
