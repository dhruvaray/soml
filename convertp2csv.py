import pandas as pd
import xml.etree.ElementTree as ET
from collections import OrderedDict
import base64

postcols = ['Id','PostTypeId','ParentId','AcceptedAnswerId','CreationDate','Score','ViewCount','Body','OwnerUserId','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate','CommunityOwnedDate','ClosedDate','Title','Tags','AnswerCount','CommentCount','FavoriteCount']
commentcols = ['Id','PostId','Score','Text','CreationDate','UserId']
posthistcols = ['Id','PostHistoryTypeId','PostId','RevisionGUID','CreationDate','UserId','UserDisplayName','Comment','Text','CloseReasonId']
usercols = ['Id','Reputation','CreationDate','DisplayName','EmailHash','LastAccessDate','WebsiteUrl', 'Location','AboutMe','Views','UpVotes','DownVotes','Age']

textcols =  ['Body','Title','Text','AboutMe','Location']

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

print '~~~ xml -> csv ~~~\n'
#ptree = ET.parse('Posts.xml')
#xml2csv(ptree,'posts',postcols)

#tree = ET.parse('Comments.xml')
#ml2csv(ptree,'comments',commentcols)

#tree = ET.parse('PostHistory.xml')
#ml2csv(ptree,'posthistory',commentcols)

ptree = ET.parse('Users.xml')
xml2csv(ptree,'users',usercols)
