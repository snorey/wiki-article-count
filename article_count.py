from collections import defaultdict
import dateutil.parser
import gzip
import os
import re
from sys import stdout
import time
import unittest
import urllib
import urllib2

# seconds to wait if download raises error
STANDARD_DELAY = 10
# username initially assigned to a revision
DEFAULT_USERNAME = "UNASSIGNED"
# username should never have default value,
# so this should be noisy, not whitespace


class Revision(object):

    def __init__(self):
        self.username = DEFAULT_USERNAME
        self.is_article = False
        self.title = ""
        self.time = False
        self.timestamp = False


class Line(str):

    @property
    def is_start_of_page(self):
        return self.startswith("<page")

    @property
    def is_end_of_page(self):
        return self.startswith("</page")

    @property
    def is_start_of_revision(self):
        return self.startswith("<revision")

    @property
    def is_timestamp(self):
        return self.startswith("<timestamp>")

    @property
    def is_title(self):
        return self.startswith("<title>")

    @property
    def is_namespace(self):
        return self.startswith("<ns>")

    @property
    def is_article_namespace(self):
        return self.startswith("<ns>0<")

    @property
    def is_redirect(self):
        return self.startswith("<redirect")

    @property
    def is_username(self):
        return self.startswith("<ip") or self.startswith("<username")

    def get_title(self):
        title = self.split(">")[1].split("<")[0]
        title = title.strip()
        return title

    def get_timestamp(self):
        timestamp = self.split(">")[1].split("<")[0]
        return timestamp

    def extract_username_string(self):
        raw_username = self.split(">")[1].split("<")[0].strip()
        return raw_username

    def get_username(self):
        if self.startswith("<ip>"):
            # need to avoid giving first registered user credit for
            # pages created by IP
            username = "IP:" + self.extract_username_string()
        elif self.startswith("<username />"):
            # here the blank username is a deliberate MediaWiki feature
            username = ""
        elif self.startswith("<username"):
            username = self.extract_username_string()
        else:
            username = DEFAULT_USERNAME
        return username


class Downloader(object):

    def __init__(self):
        self.dumpsurl = "http://dumps.wikimedia.your.org/enwiki/latest/"
        self.headers = {'User-agent': 'JumpingSpider/0.0'}
        self.counters = []
        self.urls = []
        self.matchups = {}
        self.replaced_users = set()
        self.temp_filepath = "stubhist_working.xml.gz"

    def run_on_downloaded(self, filepaths):  # to just use already-downloaded DB files
        for f in filepaths:
            print f
            counter = self.count_creators_in_file(f)
            self.counters.append((f, counter))
            open("wikicount_dump.txt", "w").write(self.dump())

    @staticmethod
    def prep_input_file(path, offset):
        if type(path) not in [str, unicode]:
            raise TypeError
        if path.endswith(".gz"):
            input_file = gzip.GzipFile(path)
        else:
            input_file = open(path)
        if offset:
            input_file.seek(offset)
        return input_file

    def count_creators_in_file(self, path, offset=0):
        input_file = self.prep_input_file(path, offset)
        i = 0
        reading = False
        frame_is_open = False
        this_revision = Revision()
        oldest_revision = False
        line = Line("")
        counter = defaultdict(int)
        try:
            for raw_line in input_file:
                i += 1
                line = Line(raw_line.strip())
                if reading is not True:
                    if line.is_start_of_page:
                        reading = True
                        frame_is_open = False
                        oldest_revision = False
                        page_title = ""
                        continue
                    continue
                else:
                    if line.is_title: # note this depends on <title> coming before any <rev>s
                        page_title = line.get_title()
                        continue
                    elif line.is_namespace:
                        if not line.is_article_namespace:
                            reading = False
                        continue
                    elif line.is_redirect: # don't count redirects
                        reading = False
                        continue
                    elif line.is_start_of_revision:
                        frame_is_open = True
                        this_revision = Revision()
                        this_revision.title = page_title
                        continue
                    elif line.is_end_of_page:
                        if oldest_revision:
                            oldest_user = oldest_revision.username
                            self.matchups[oldest_revision.title] = oldest_user
                            counter[oldest_user] += 1
                        else:
                            print "No revisions!", pagetitle
                        reading = False
                        frame_is_open = False
                        simple_progress_counter(i)
                        continue
                    elif frame_is_open:
                        if line.is_timestamp:
                            this_revision.timestamp = line.get_timestamp()
                            continue
                        elif line.is_username:  # this comes last in a <rev>
                            if not this_revision.timestamp:
                                print "No timestamp!", page_title
                            else:
                                thetime = dateutil.parser.parse(this_revision.timestamp)  # returns datetime object
                                this_revision.time = thetime.isoformat()  # string operations faster than datetime
                                this_revision.username = line.get_username()
                                if not oldest_revision:
                                    oldest_revision = this_revision
                                else:
                                    if this_revision.time < oldest_revision.time:
                                        oldest_revision = this_revision
                            frame_is_open = False
        except Exception, e:
            print str(e)
            print line
        except KeyboardInterrupt:
            pass
        return counter
            
    def get_file_urls(self):  # get URLs of all pre-combination stub-meta-history files
        request = urllib2.Request(self.dumpsurl, headers=self.headers)
        dumpspage = urllib2.urlopen(request, timeout=240).read()
        urlpaths = re.findall('"[^"]+-stub-meta-history\d.*?\.xml\.gz"', dumpspage)
        self.urls = [self.dumpsurl+x.replace('"', '') for x in urlpaths]

    @staticmethod
    def download_single_file(url, filepath):
        print "Downloading " + url
        done = False
        while not done:
            try:
                urllib.urlretrieve(url, filepath)
                done = True
            except Exception, e:
                print str(e)
                time.sleep(STANDARD_DELAY)
        return filepath

    def go(self, prepped=False):  # handle all segmented stub-meta-history files in sequence
        if prepped is False:
            self.get_file_urls()
        urls_done = [x[0] for x in self.counters]
        for url in self.urls:
            try:
                if url in urls_done:
                    print url, "already done"
                    continue
                filepath = self.download_single_file(url, self.temp_filepath)
                print "Reading...."
                users_from_file = self.count_creators_in_file(filepath)
                self.counters.append((url, users_from_file))
                print
                print "Deleting ...."
                os.unlink(filepath)
            except KeyboardInterrupt:
                break
            
    def dump(self):
        output = ""
        for c in self.counters:
            path = c[0]
            article_counts = c[1]
            for username, count in article_counts.iteritems():
                new_line = self.generate_output_line(path, username, count)
                output += new_line
        return output

    @staticmethod
    def generate_output_line(path, username, count):
        new_line = path + "\t" + username + "\t" + str(count) + "\n"
        return new_line




def sortusers(users):
    sorted_users = []
    for u in users.keys():
        sorted_users.append((users[u], u))
    sorted_users.sort()
    sorted_users.reverse()
    return sorted_users
    

def summate(matchups):
    output = defaultdict(int)
    i = 0
    for v in matchups.values():
        output[v] += 1
        i += 1
        if not i % 100:
            simple_progress_counter(i)
    return output
    
    
def truncate(summation, max_users=10000):
    userlist = []
    for s in summation.keys():
        userlist.append((summation[s], s))
    print len(userlist)
    userlist.sort()
    userlist.reverse()
    userlist = userlist[:max_users]
    return userlist
    
    
def get_current_totals():
    output = []
    pagename = "Wikipedia:List_of_Wikipedians_by_article_count/Data"
    url = "http://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=%s&rvprop=content&format=xml" % pagename
    page = urllib2.urlopen(url, timeout=60).read()
    page = page.split("<rev ")[1].split(">", 1)[1].split("<")[0]
    pieces = page.split("|}")[0].split("|-")[2:]
    pieces = [x.strip() for x in pieces]
    for p in pieces:
        data = [x.strip() for x in p.split("|") if x.strip()]
        if not data: 
            continue
        rank = int(data[0])
        username = data[1]
        count = int(data[2].replace(",", ""))
        output.append(tuple([rank, username, count]))
    return output
    
    
def get_mismatches(current, summation):
    mismatched = []  # list of tuples: (discrepancy,username,current,new)
    currentdict = dict([(x[1], x[2]) for x in current])
    for c in currentdict.keys():
        if c in summation.keys():
            if int(summation[c]) != int(currentdict[c]):
                diff = int(summation[c]) - int(currentdict[c])
                mismatched.append((diff, c, currentdict[c], summation[c]))
    mismatched.sort()
    mismatched.reverse()
    return mismatched
    
    
def getanons():
    # some contributors desire to be excluded
    pagename = "Wikipedia:List of Wikipedians by number of edits/Anonymous".replace(" ", "_")
    url = "http://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=%s&rvprop=content&format=xml" % pagename
    anonpage = urllib2.urlopen(url, timeout=60).read()
    anonpage = anonpage.split("==\n", 1)[1]
    anons = [x.split("]]")[0] for x in anonpage.split("[[User:")[1:]]
    print str(len(anons))+" anons"
    return anons
    
    
def replaceanons(wikitext, anons=False):
    if anons is False:
        anons = getanons()
    for anon in anons:
        catchme = "| %s\n" % anon
        if catchme in wikitext:
            print "Effacing "+anon
            wikitext = wikitext.replace(catchme, "| [Placeholder]\n")
    return wikitext


def dumpdict(foo):
    keylist = foo.matchups.keys()
    keylist.sort()
    output = ""
    for k in keylist:
        output += "%s\t%s\n" % (k, foo.matchups[k])
    return output


def makedatapage(userlist):  # as returned by truncate()
    text = ("\n"
            "{| class = \"wikitable sortable\"\n"
            "|- style = \"white-space:nowrap;\"\n"
            "! No.\n"
            "! User\n"
            "! Article count\n"
            "|-")
    for u in userlist:
        number = str(userlist.index(u)+1)
        count = str(u[0])
        newlines = ("\n"
                    "| %s\n"
                    "| %s\n"
                    "| %s\n"
                    "|-") % (number, u[1], count)
        text += newlines
    text += "\n|}"
    return text
    
    
def totalprep(downloader):  # take completed Downloader and make Data page
    summation = summate(downloader.matchups)
    truncation = truncate(summation, 5000)
    datapage = makedatapage(truncation)
    datapage = replaceanons(datapage)
    return datapage


def simple_progress_counter(i):
    stdout.write("\r")
    stdout.flush()
    stdout.write(str(i))
