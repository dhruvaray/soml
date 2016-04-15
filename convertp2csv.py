import pandas as pd
import xml.etree.ElementTree as ET
from collections import OrderedDict
import base64

ptree = ET.parse('Posts.xml')
postcols = ['PostTypeId','AcceptedAnswerId','CreationDate','Score','ViewCount','Body','OwnerUserId','LastEditorUserId','LastEditDate','LastActivityDate','Title','Tags','AnswerCount','CommentCount','FavoriteCount']

def posts(post):
    for doc in post.iterfind('.//row'):
        row = OrderedDict()
        for c in postcols:
            if doc.attrib.get(c) is None:
                row[c] = ''
            else:
                val = doc.attrib[c].encode('utf-8')
                if c in ['Body','Title']:
                    val = base64.b64encode(val)
                row[c] = val.replace('\n','').replace('\r','').replace('\r\n','')
        yield row

print '~~~ xml -> csv ~~~\n'

with open('posts.csv','a') as f:
    f.write(','.join(postcols) + '\n')
    for count, p in enumerate(posts(ptree)):
        print 'record #' + `count`
        f.write(','.join(p.values()) + '\n')

print '~~~ Sample file generation ~~~\n'

with open('posts.sample.csv','w') as f:
    f.write(','.join(postcols) + '\n')
    for count, p in enumerate(posts(ptree)):
        f.write(','.join(p.values()) + '\n')
        if count > 100:
            break


