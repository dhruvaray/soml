import pandas as pd
import xml.etree.ElementTree as ET
from collections import OrderedDict
import base64

columns = {}
columns['posts'] = ['Id','PostTypeId','ParentId','AcceptedAnswerId','CreationDate','Score','ViewCount','Body','OwnerUserId','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate','CommunityOwnedDate','ClosedDate','Title','Tags','AnswerCount','CommentCount','FavoriteCount']
columns['comments'] = ['Id','PostId','Score','Text','CreationDate','UserId']
columns['posthistory'] = ['Id','PostHistoryTypeId','PostId','RevisionGUID','CreationDate','UserId','UserDisplayName','Comment','Text','CloseReasonId']
columns['users'] = ['Id','Reputation','CreationDate','DisplayName','EmailHash','LastAccessDate','WebsiteUrl', 'Location','AboutMe','Views','UpVotes','DownVotes','Age']

textcols =  ['Body','Title','Text','AboutMe','Location']

#TODO : Stream v/s load whole document
def documents(post,cols):
    for doc in post.iterfind('.//row'):
        row = OrderedDict()
        for c in cols:
            if doc.attrib.get(c) is None:
                row[c] = ''
            else:
                val = doc.attrib[c].encode('utf-8')
                if c in textcols:
                    val = base64.b64encode(val)
                row[c] = val.replace('\n','').replace('\r','').replace('\r\n','')
        yield row


def xml2csv(root, type, headers):
    with open(type +'.csv','w') as f:
        f.write(','.join(headers) + '\n')
        for count, p in enumerate(documents(root,headers)):
            print 'record #' + `count`
            f.write(','.join(p.values()) + '\n')

    print '~~~ Sample file generation ~~~\n'

    with open(type + '.sample.csv','w') as f:
        f.write(','.join(headers) + '\n')
        for count, p in enumerate(documents(root,headers)):
            f.write(','.join(p.values()) + '\n')
            if count > 100:
                break


sources = ['Posts','Comments','PostHistory','Users']

for source in sources:
    print 'processing - ' + source
    root = ET.parse(source + '.xml')
    xml2csv(root,source.lower(),columns[source.lower()])