from collections import defaultdict
import dateutil.parser
import gzip
import os
import re
from sys import stdout
import time
import urllib
import urllib2


class Downloader:
    def __init__(self):
        self.dumpsurl="http://dumps.wikimedia.your.org/enwiki/latest/"
        self.headers={'User-agent' : 'JumpingSpider/0.0'}
        self.counters=[]
        self.trackers=[]
        self.matchups={}
        self.replaced_users=set()

    def run(self, filepaths): # to just use already-downloaded DB files
        for f in filepaths:
            print f
            self.countusers(f)
            open("wikicount_dump.txt","w").write(self.dump())

    def countusers(self,path,offset=0,cutoff=False):
        if path.endswith(".gz"):
            file=gzip.GzipFile(path)
        else:
            file=open(path)
        if offset:
            file.seek(offset)
        i=0
        reading=False
        reading_rev=False
        try:
            for line in file:
                i+=1 
                line=line.strip()
                if line.startswith("<page"):
                    reading=True
                    revisions=[]
                    reading_rev=False
                    thetitle=""
                    continue
                if reading is not True:
                    continue
                else:
                    if line.startswith("</page>"):
                        if revisions:
                            sortedrevs=list(revisions)
                            sortedrevs.sort()
                            username=sortedrevs[0][1]
                            if username != revisions[0][1]:
                                self.replaced_users.add((thetitle,username,revisions[0][1]))
                            self.matchups[thetitle]=username
                        else:
                            print "No revisions! ", thetitle
                        reading=False
                        reading_rev=False
                        stdout.write("\r") #put progress counter here to minimize waste
                        stdout.flush()
                        stdout.write(str(i))
                        continue
                    elif reading_rev is True:
                        if line.startswith("<timestamp>"):
                            timestamp=line.split(">")[1].split("<")[0]
                            continue
                        elif line.startswith("<ip") or line.startswith("<username"): 
                            if not timestamp: # just in case -- has not been triggered thus far
                                print "No timestamp!",thetitle
                            else:
                                thetime=dateutil.parser.parse(timestamp)
                                if line.startswith("<ip>"): #need to avoid counting pages created by IP for the first registered user to edit
                                    username="IP:"+line.split(">")[1].split("<")[0].strip()
                                elif line.startswith("<username />"):
                                    username=""
                                elif line.startswith("<username"):
                                    username=line.split(">")[1].split("<")[0].strip()
                                else:
                                    username="UNASSIGNED" #make sure there is no leak here
                                revisions.append((thetime,username))
                            reading_rev=False
                    elif line.startswith("<revision"):
                        reading_rev=True
                        timestamp=""
                        username=""
                        continue
                    elif line.startswith("<title>"):
                        thetitle=line.split(">")[1].split("<")[0].strip()
                        continue
                    elif line.startswith("<ns>"):
                        if not line.startswith("<ns>0<"):
                            reading=False
                            continue
                    elif line.startswith("<redirect"):
                        reading=False
                        continue
        except Exception, e:
            print str(e), line
            
    def process(self): # get URLs of all pre-combination stub-meta-history files
        request=urllib2.Request(self.dumpsurl,headers=self.headers)
        dumpspage=urllib2.urlopen(request,timeout=240).read()
        urlpaths=re.findall('"[^"]+-stub-meta-history\d.*?\.xml\.gz"',dumpspage)
        self.urls=[self.dumpsurl+x.replace('"','') for x in urlpaths]
        
    def go(self): # to download, process, and delete the segmented stub-meta-history files in sequence
        doneurls=[x[0] for x in self.counters]
        for u in self.urls:
            if u in doneurls:
                print u,"already done"
                continue
            filepath="stubhist_working.xml.gz"
            print "Downloading "+u
            done=False
            while not done:
                try:
                    urllib.urlretrieve(u, filepath)
                    done=True
                except Exception, e:
                    print str(e)
                    time.sleep(10)
            print "Reading...."
            gfile=gzip.GzipFile(filepath)
            with gfile:
                self.counters.append((u,self.countusers(gfile))) # avoid dict of dicts, too slippery
            print
            print "Deleting ...."
            os.unlink(filepath)
            
    def dump(self):
        output=""
        for c in self.counters:
            path=c[0]
            dixie=c[1]
            for d in dixie.keys():
                newline=path+"\t"+str(d)+"\t"+str(dixie[d])+"\n"
                output+=newline
        return output
        
    def countusers(self,path,offset=0,cutoff=False):
        import dateutil.parser
        if path.endswith(".gz"):
            file=gzip.GzipFile(path)
        else:
            file=open(path)
        if offset:
            file.seek(offset)
        i=0
        reading=False
        reading_rev=False
        try:
            for line in file:
                i+=1 
                line=line.strip()
                if line.startswith("<page"):
                    reading=True
                    revisions=[]
                    reading_rev=False
                    thetitle=""
                    continue
                if reading is not True:
                    continue
                else:
                    if line.startswith("</page>"):
                        if revisions:
                            sortedrevs=list(revisions)
                            sortedrevs.sort()
                            username=sortedrevs[0][1]
                            if username != revisions[0][1]:
                                self.replaced_users.add((thetitle,username,revisions[0][1]))
                            self.matchups[thetitle]=username
                        else:
                            print "No revisions! ", thetitle
                        reading=False
                        reading_rev=False
                        stdout.write("\r") #put progress counter here to minimize waste
                        stdout.flush()
                        stdout.write(str(i))
                        continue
                    elif reading_rev is True:
                        if line.startswith("<timestamp>"):
                            timestamp=line.split(">")[1].split("<")[0]
                            continue
                        elif line.startswith("<ip") or line.startswith("<username"): 
                            if not timestamp: # just in case -- has not been triggered thus far
                                print "No timestamp!",thetitle
                            else:
                                thetime=dateutil.parser.parse(timestamp)
                                if line.startswith("<ip>"): #need to avoid counting pages created by IP for the first registered user to edit
                                    username="IP:"+line.split(">")[1].split("<")[0].strip()
                                elif line.startswith("<username />"):
                                    username=""
                                elif line.startswith("<username"):
                                    username=line.split(">")[1].split("<")[0].strip()
                                else:
                                    username="UNASSIGNED" #make sure there is no leak here
                                revisions.append((thetime,username))
                            reading_rev=False
                    elif line.startswith("<revision"):
                        reading_rev=True
                        timestamp=""
                        username=""
                        continue
                    elif line.startswith("<title>"):
                        thetitle=line.split(">")[1].split("<")[0].strip()
                        continue
                    elif line.startswith("<ns>"):
                        if not line.startswith("<ns>0<"):
                            reading=False
                            continue
                    elif line.startswith("<redirect"):
                        reading=False
                        continue
        except Exception, e:
            print str(e), line

                
def sortusers(users):
    sorted=[]
    for u in users.keys():
        sorted.append((users[u],u))
    sorted.sort()
    sorted.reverse()
    return sorted
    

def summate3(matchups):
    values=list(set(matchups.values()))
    output=defaultdict(int)
    i=0
    for v in matchups.values():
        output[v]+=1
        i+=1
        if not i%100:            
            stdout.write("\r") 
            stdout.flush()
            stdout.write(str(i))
    return output
    
    
def truncate(summation,max=10000):
    userlist=[]
    for s in summation.keys():
        userlist.append((summation[s],s))
    print len(userlist)
    userlist.sort()
    userlist.reverse()
    userlist=userlist[:max]
    return userlist
    
    
def get_current_totals():
    output=[]
    pagename="Wikipedia:List_of_Wikipedians_by_article_count/Data"
    url="http://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=%s&rvprop=content&format=xml" % pagename
    page=urllib2.urlopen(url,timeout=60).read()
    page=page.split("<rev ")[1].split(">",1)[1].split("<")[0]
    pieces=page.split("|}")[0].split("|-")[2:]
    pieces=[x.strip() for x in pieces]
    for p in pieces:
        data=[x.strip() for x in p.split("|") if x.strip()]
        if not data: 
            continue
        rank=int(data[0])
        username=data[1]
        count=int(data[2].replace(",",""))
        output.append(tuple([rank,username,count]))
    return output
    
    
def get_mismatches(current,summation):
    mismatched=[] # list of tuples: (discrepancy,username,current,new)
    currentdict=dict([(x[1],x[2]) for x in current])
    for c in currentdict.keys():
        if c in summation.keys():
            if int(summation[c]) != int(currentdict[c]):
                diff=int(summation[c])-int(currentdict[c])
                mismatched.append((diff,c,currentdict[c],summation[c]))
    mismatched.sort()
    mismatched.reverse()
    return mismatched
    
    
def getanons():
    pagename="Wikipedia:List of Wikipedians by number of edits/Anonymous".replace(" ","_")
    url="http://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=%s&rvprop=content&format=xml" % pagename
    anonpage=urllib2.urlopen(url,timeout=60).read()
    anonpage=anonpage.split("==\n",1)[1]
    anons=[x.split("]]")[0] for x in anonpage.split("[[User:")[1:]]
    print str(len(anons))+" anons"
    return anons
    
    
def replaceanons(wikitext,anons=[]):
    if not anons:
        anons=getanons()
    for anon in anons:
        catchme="| %s\n" % anon
        if catchme in wikitext:
            print "Effacing "+anon
            wikitext=wikitext.replace(catchme, "| [Placeholder]\n")
    return wikitext


def dumpusers(foo,userlist=[]): # Downloader object
    outdict=defaultdict(set)
    for tracker in foo.trackers:
        path=tracker[0]
        for user in tracker[1].keys():
            outdict[user] |= tracker[1][user]
    outtext=""
    for user in outdict.keys():
        newline=user+"\t"
        newline="[["
        newline+="]] - [[".join(outdict[user])
        newline+="]]\n"
        outtext+=newline
    return outtext


def dumpdict(foo):
    keylist=foo.matchups.keys()
    keylist.sort()
    output=""
    for k in keylist:
        output+="%s\t%s\n" % (k,foo.matchups[k])
    return output


def makedatapage(userlist): #as returned by truncate()
    text="""{| class="wikitable sortable"
|- style="white-space:nowrap;"
! No.
! User
! Article count
|-"""
    for u in userlist:
        number=str(userlist.index(u)+1)
        count=str(u[0])
        newlines="""
| %s
| %s
| %s
|-""" % (number,u[1],count)
        text += newlines
    text += "\n|}"
    return text
    
    
def totalprep(downloader): # take completed Downloader and make Data page
    summation=summate3(downloader.matchups)
    print
    truncation=truncate(summation,5000)
    datapage=makedatapage(truncation)
    datapage=replaceanons(datapage)
    return datapage
