import sys
from os import remove
from os.path import dirname, join, isfile
from time import time

topMovies = """1,Toy Story (1995)
780,Independence Day (a.k.a. ID4) (1996)
590,Dances with Wolves (1990)
1210,Star Wars: Episode VI - Return of the Jedi (1983)
648,Mission: Impossible (1996)
344,Ace Ventura: Pet Detective (1994)
165,Die Hard: With a Vengeance (1995)
153,Batman Forever (1995)
597,Pretty Woman (1990)
1580,Men in Black (1997)
231,Dumb & Dumber (1994)
3793,X-Men (2000)
7,Sabrina (1995)
10,GoldenEye (1995)
17,Sense and Sensibility (1995)
25,Leaving Las Vegas (1995)
261,Little Women (1994)
432,City Slickers II: The Legend of Curly's Gold (1994)
454,Firm, The (1993)
457,Fugitive, The (1993)
480,Jurassic Park (1993)
508,Philadelphia (1993)
527,Schindler's List (1993)
539,Sleepless in Seattle (1993)
367,Mask, The (1994)
368,Maverick (1994)"""

parentDir = dirname(dirname(__file__))
ratingsFile = join(parentDir, "data/personalRatings.txt")

if isfile(ratingsFile):
    r = raw_input("Looks like you've already rated the movies. Overwrite ratings (y/N)? ")
    if r and r[0].lower() == "y":
        remove(ratingsFile)
    else:
        sys.exit()

prompt = "Please rate the following movie (1-5 (best), or 0 if not seen): "
print prompt

now = int(time())
n = 0

f = open(ratingsFile, 'w')
for line in topMovies.split("\n"):
    ls = line.strip().split(",")
    valid = False
    while not valid:
        rStr = raw_input(ls[1] + ": ")
        r = int(rStr) if rStr.isdigit() else -1
        if r < 0 or r > 5:
            print prompt
        else:
            valid = True
            if r > 0:
                f.write("0::%s::%d::%d\n" % (ls[0], r, now))
                n += 1
f.close()

if n == 0:
    print "No rating provided!"