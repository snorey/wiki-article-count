# wiki-article-count
Script for counting contributors to a Wikimedia project, using WMF stub-meta-history dump.

To serially download and process the 27 individual segmented dumps (takes a while), and save the final user list to a file in the current working directory named "output.txt":

```
import article_count as ac
foo = ac.Manager()
foo.go()
output = totalprep(foo)
output_file = open("output.txt","w")
output_file.write(output)
```

If you have 50 gigs or so of hard drive space to spare, you can download the stub-meta-history dump as a single file.  Supposing you call it "dump.gz", you can modify the above code as follows:

```
import article_count as ac
foo = ac.Manager()
foo.run_on_downloaded(["dump.gz"])      
output = totalprep(foo)
output_file = open("output.txt","w")
output_file.write(output)
```
