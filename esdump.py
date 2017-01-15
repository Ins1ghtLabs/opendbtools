from elasticsearch import Elasticsearch
import argparse
import os
import simplejson as json


TIMEOUT = "7d" 


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump ES index with custom scan_id')
    parser.add_argument('--host', help='ES host, host:port',required=True)
    parser.add_argument('--index',help='Index name',required=True)
    parser.add_argument('--size',help='Scroll size',default=500)

    args = parser.parse_args()

    es=Elasticsearch(args.host,timeout=120)
    
    if not os.path.isfile(args.host+'_'+args.index+'.txt'):
        r = es.search(args.index, search_type="scan",size=args.size, scroll=TIMEOUT)
    else:
        fs=open(args.host+'_'+args.index+'.txt','r')
        scanid=fs.readlines()[0].strip()
        r = es.scroll(scroll_id=scanid, scroll=TIMEOUT)
    while '_scroll_id' in r:
        sid=r['_scroll_id']
        f=open(args.host+'_'+args.index+'.txt','w')
        f.write(sid+"\n")
        f.close()
        for row in r['hits']['hits']:
            print json.dumps(row)
        try:
            r=es.scroll(scroll_id=sid, scroll=TIMEOUT)
        except:
            continue
    for row in r['hits']['hits']:
        print json.dumps(row)
    
