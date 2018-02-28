from collections import defaultdict
import dateutil.parser
import gzip
import os
import re
from sys import stdout
import time
import urllib
import urllib2


"""Script to provide number of articles created by Wikipedia users.
Will work on any WMF project generating a stub-meta-history dump.

NOTE: Because this must iterate over hundreds of millions of revisions, and billions
of lines of XML, in order to run efficiently in the laptop environment, the script  
is optimized against the *exact* format used in Wikimedia XML dumps.  Consequently, 
the results could be rendered unreliable by minor changes in Wikimedia XML dump format. 
Fortunately, as of 2018, there have not been any such changes in recent years.
 """


# to do:
# 1. add unit tests
# 2. run a check over the first <Page> in each input file to verify format assumptions


# seconds to wait if download raises error:
STANDARD_DELAY = 10
# username initially assigned to a revision:
DEFAULT_USERNAME = "UNASSIGNED"
# username should never have default value,
# so this should be noisy, not whitespace


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

    def get_content(self):
        content = self.split(">")[1].split("<")[0]
        content = content.strip()
        return content

    def get_username(self):
        if self.startswith("<ip>"):
            username = "IP:" + self.get_content()
        elif self.startswith("<username />"):
            # here the blank username is a deliberate MediaWiki feature
            username = ""
        elif self.startswith("<username"):
            username = self.get_content()
        else:
            username = DEFAULT_USERNAME
        return username


class Revision(object):

    def __init__(self):
        self.username = DEFAULT_USERNAME
        self.is_article = False
        self.title = ""
        self.time = False
        self.timestamp = False


class Page(object):

    def __init__(self):
        self.title = False
        self.oldest_revision = False
        raise NotImplementedError


class FileReader(object):

    def __init__(self, manager, filepath):
        self.manager = manager
        self.filepath = filepath
        self.current_page_title = ""
        self.reading = False
        self.oldest_revision = False
        self.frame_is_open = False
        self.this_revision = False
        self.counter = defaultdict(int)

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

    def start_new_page(self):
        self.current_page_title = ""
        self.reading = True
        self.oldest_revision = False
        self.frame_is_open = False
        self.this_revision = False

    def end_page(self):
        self.start_new_page()  # reset
        self.reading = False

    def start_new_revision(self):
        self.frame_is_open = True
        self.this_revision = Revision()
        self.this_revision.title = self.current_page_title

    def end_revision(self):
        self.frame_is_open = False
        self.this_revision = False

    def attribute_page(self):
        if self.oldest_revision is not False:
            oldest_user = self.oldest_revision.username
            self.manager.matchups[self.oldest_revision.title] = oldest_user
            self.counter[oldest_user] += 1
        else:
            print "No revisions!", self.current_page_title

    def process_line(self, raw_line):
        line = Line(raw_line.strip())
        if self.reading is not True:
            if line.is_start_of_page:
                self.start_new_page()
            return
        else:
            if line.is_title:  # note this depends on <title> coming before any <rev>s
                self.current_page_title = line.get_content()
                return
            elif line.is_namespace:
                if not line.is_article_namespace:
                    self.reading = False
                return
            elif line.is_redirect:  # don't count redirects
                self.reading = False
                return
            elif line.is_start_of_revision:
                self.start_new_revision()
                return
            elif line.is_end_of_page:
                self.attribute_page()
                self.end_page()
                return
            elif not self.frame_is_open:
                return
            elif line.is_timestamp:
                self.this_revision.timestamp = line.get_content()
                return
            elif line.is_username:  # this comes last in a <rev>, so timestamp should be there
                if not self.this_revision.timestamp:
                    print "No timestamp!", page_title
                else:
                    self.this_revision.username = line.get_username()
                    self.update_oldest_revision()
                self.end_revision()
                return

    def update_oldest_revision(self):
        if self.oldest_revision is False:
            self.oldest_revision = self.this_revision
        else:
            if self.this_revision.timestamp < self.oldest_revision.timestamp:
                # timestamps now in ISO format, which makes sorting much easier
                self.oldest_revision = self.this_revision

    def count_creators_in_file(self, offset=0):
        input_file = self.prep_input_file(path=self.filepath, offset=offset)
        i = 0
        j = 0
        self.start_new_page()
        for raw_line in input_file:
            try:
                self.process_line(raw_line)
            except KeyboardInterrupt:
                break
            except Exception, e:
                print str(e)
                print raw_line
            i += 1
            j += 1
            if j == 100:  # cheaper than a modulo operation
                simple_progress_counter(i)
                j = 0
        return self.counter


class Manager(object):

    def __init__(self, project="enwiki"):
        self.dumpsurl = "http://dumps.wikimedia.your.org/%s/latest/" % project
        self.headers = {'User-agent': 'JumpingSpider/0.0'}
        self.counters = []
        self.urls = []
        self.matchups = {}
        self.replaced_users = set()
        self.temp_filepath = "stubhist_working.xml.gz"
        self.current_reader = False

    def run_on_downloaded(self, filepaths):  # to just use already-downloaded DB files
        for f in filepaths:
            print f
            reader = FileReader(self, f)
            self.current_reader = reader
            counter = reader.count_creators_in_file()
            self.counters.append((f, counter))
            open("wikicount_dump.txt", "w").write(self.dump())

    def get_file_urls(self):  # get URLs of all pre-combination stub-meta-history files
        request = urllib2.Request(self.dumpsurl, headers=self.headers)
        dumps_page = urllib2.urlopen(request, timeout=240).read()
        self.urls = self.extract_stub_dump_urls(dumps_page, self.dumpsurl)

    @staticmethod
    def extract_stub_dump_urls(dumps_page, dumps_url):
        url_paths = re.findall('"[^"]+-stub-meta-history\d.*?\.xml\.gz"', dumps_page)
        urls = []
        for path in url_paths:
            path = path.replace('"', '')
            urls.append(dumps_url + path)
        return urls

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
        urls_done = self.get_completed_urls()
        for url in self.urls:
            if url in urls_done:
                print url, "already done"
                continue
            filepath = self.download_single_file(url, self.temp_filepath)
            print "Reading...."
            reader = FileReader(self, filepath)
            users_from_file = reader.count_creators_in_file()
            self.counters.append((url, users_from_file))
            print "Deleting ...."
            os.unlink(filepath)

    def get_completed_urls(self):
        return [x[0] for x in self.counters]

    @staticmethod
    def generate_output_line(path, username, count):
        new_line = path + "\t" + username + "\t" + str(count) + "\n"
        return new_line

    def dump(self):
        output = ""
        for c in self.counters:
            path = c[0]
            article_counts = c[1]
            for username, count in article_counts.iteritems():
                new_line = self.generate_output_line(path, username, count)
                output += new_line
        return output


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
    anon_page = urllib2.urlopen(url, timeout=60).read()
    anon_page = anon_page.split("==\n", 1)[1]
    anons = [x.split("]]")[0] for x in anon_page.split("[[User:")[1:]]
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
    
    
def totalprep(manager):  # take completed Manager and make Data page
    summation = summate(manager.matchups)
    truncation = truncate(summation, 5000)
    datapage = makedatapage(truncation)
    datapage = replaceanons(datapage)
    return datapage


def simple_progress_counter(i):
    stdout.write("\r")
    stdout.flush()
    stdout.write(str(i))
