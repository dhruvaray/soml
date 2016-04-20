import pandas as pd
from collections import OrderedDict
import base64

import xml.etree.cElementTree as ET

columns = {}
columns['posts'] = ['Id','PostTypeId','ParentId','AcceptedAnswerId','CreationDate','Score','ViewCount','Body','OwnerUserId','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate','CommunityOwnedDate','ClosedDate','Title','Tags','AnswerCount','CommentCount','FavoriteCount']
columns['comments'] = ['Id','PostId','Score','Text','CreationDate','UserId']
columns['posthistory'] = ['Id','PostHistoryTypeId','PostId','RevisionGUID','CreationDate','UserId','UserDisplayName','Comment','Text','CloseReasonId']
columns['users'] = ['Id','Reputation','CreationDate','DisplayName','EmailHash','LastAccessDate','WebsiteUrl', 'Location','AboutMe','Views','UpVotes','DownVotes','Age']

textcols =  ['Body','Title','Text','AboutMe','Location','Comment']

#TODO : Stream v/s load whole document
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
                        val = base64.b64encode(val)
                    row[c] = val.replace('\n','').replace('\r','').replace('\r\n','')
            elem.clear()        
            yield row

def xml2csv(root, type, headers):
    with open(type +'.csv','w') as f:
        f.write(','.join(headers) + '\n')
        tc = 0
        print '...',
        for count, p in enumerate(documents(root,headers)):
            tc = count
            f.write(','.join(p.values()) + '\n')
        print tc

sources = ['Posts','Comments','PostHistory','Users']
#sources = ['Users']

for source in sources:
    print 'processing : ' + source
    root = iter(ET.iterparse(source + '.xml', events=("start", "end")))
    xml2csv(root,source.lower(),columns[source.lower()])

