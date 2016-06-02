import pandas as pd
from collections import OrderedDict
import re
from bs4 import BeautifulSoup 
import warnings
import xml.etree.cElementTree as ET
import gzip
import os

warnings.filterwarnings('error')

OUTPUT = 'output'
INPUT  = 'input'

for dir in [INPUT,OUTPUT]:
   if not os.path.exists(dir):
      os.makedirs(dir)

columns = {}
columns['posts'] = ['Id','PostTypeId','ParentId','AcceptedAnswerId','CreationDate','Score','ViewCount','Body','OwnerUserId','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate','CommunityOwnedDate','ClosedDate','Title','Tags','AnswerCount','CommentCount','FavoriteCount']
columns['comments'] = ['Id','PostId','Score','Text','CreationDate','UserId']
columns['posthistory'] = ['Id','PostHistoryTypeId','PostId','RevisionGUID','CreationDate','UserId','UserDisplayName','Comment','Text','CloseReasonId']
columns['users'] = ['Id','Reputation','CreationDate','DisplayName','EmailHash','LastAccessDate','WebsiteUrl', 'Location','AboutMe','Views','UpVotes','DownVotes','Age']

textcols =  ['Body','Title','Text','AboutMe','Location','Comment','WebsiteUrl']

def documents(context,cols):

    for event, elem in context:
        tag = elem.tag

        if event == 'start' and tag == "row":
            doc = elem.attrib
            row = OrderedDict()
            for c in cols:
                if doc.get(c) is None:
                    row[c] = ''
                else:
                    val = doc[c].encode('utf-8')

                    if c in textcols:
                        try:
                            val = re.sub("[^a-zA-Z]"," ", BeautifulSoup(val,"lxml").get_text().encode('utf-8')).lower()
                        except UserWarning:
                            if ',' in val: #hacky
                                val = val.replace(',','')

                    row[c] = val
            elem.clear()        
            yield row

def xml2csv(root, site, type, headers):
    with gzip.open(OUTPUT + '/' + site + '/' + type +'.csv.gz','w') as f:
        f.write(','.join(headers) + '\n')
        tc = 0
        for count, p in enumerate(documents(root,headers)):
            tc = count
            f.write(','.join(p.values()) + '\n')
            if count % 10000 == 0:
                print '...1k'
        print '...' + `int(tc/1000)` + 'k'

sources = ['Posts','Comments','PostHistory','Users']

def process(path):
    for (dirpath, dirnames, filenames) in os.walk(path):
        for dirname in dirnames:
            processed = INPUT + '/' + dirname + '/.processed'
            if not os.path.exists(processed):
                for source in sources:
                    print 'processing : ' + source + ' for site : ' + dirname 
                    site = dirname
                    name = INPUT + '/' + site + '/' + source + '.xml'
                    if os.path.exists(name):
                        output = OUTPUT + '/' + site
                        if not os.path.exists(output):
                            os.makedirs(output)
                        root = iter(ET.iterparse(name, events=("start", "end")))
                        xml2csv(root,site,source.lower(),columns[source.lower()])

                open(processed, 'a').close()
            else:
                print 'skipping ' + dirname + '...'
                

if __name__ == "__main__":
    start = os.getcwd() + '/' + INPUT
    print(start)
    process(start)