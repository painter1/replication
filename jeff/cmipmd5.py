#!/usr/apps/esg/cdat6.0a/bin/python

import sys, os, os.path, time
import hashlib, subprocess

def md5( filename ):
    # returns the file's md5 checksum in hex format.
    # For performance on big files, this reads the file one chunk at a time.
    # based on something on the Web, tuned for large files on gdo2.
    # Note (jfp 2012.01.24): on a large test file, this (and wrapper code) took
    # 3.6 seconds, but /opt/sfw/bin/md5sum took 10.0 seconds.  As the core code of
    # each is probably written in C, this probably means md5sum does Sun I/O poorly.
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(256*md5.block_size), ''): 
            md5.update(chunk)
    return md5.hexdigest()

def md5_all_of_it( filename ):
    # reads in the whole file to compute its md5 checksum; slightly slower
    # than reading it in chunks
    with open( filename,'rb' ) as f:
        hash = hashlib.md5(f.read()).hexdigest()
    return hash

def print_all_md5s( dirname ):
    # walks the supplied directory, printing md5 checksums of all files found.
    if os.path.isfile(dirname):
        print md5(dirname)
    elif os.path.isdir(dirname):
        for ( dirpath, dirnames, filenames ) in os.walk( dirname ):
            for f in filenames:
                print f, md5(os.path.join(dirpath,f))
    else:
        print dirname,"is not a file or directory"
    
def check_md5s( filename ):
    # The supplied filename must be a filelist generated by Estani's script.
    # Each file, the second item of each line of the filelist, will be compared against
    # its checksum, the fourth item of the line if it exists.  The length of the file
    # will be compared against the third item of the line.  If the checksum or length fail
    # to match, a message will be printed.  Final statistics will also be printed
    # Results are printed to both stdout and a log file, filename+"_checksum_results".
    # The log file also will contain the names of files for which there is no checksum,
    # and stdout will also display progress information.
    # Finally, files will be printed containing (1) the checksums of all files which have
    # been verified to be good, (2) the checksums of all files for which no checksums are
    # available, but at least the length is known to be good.
    nfiles = 0
    ngood = 0
    nbad = 0
    nnock = 0
    nnofl = 0
    f = open(filename)
    g = open(filename+'_checksum_results','a')
    g.write("\n==== Checksum Results for "+filename+" ====\n")
    h = open(filename+'_good_checksums','a')
    i = open(filename+'_new_checksums','a')
    num_lines = sum(1 for line in f)
    f.seek(0)
    print num_lines,"files; now checking",
    ttime = time.time()
    for line in f:
        # e.g. line = 'gsiftp://cmip2.dkrz.de:2812/cmip5/...199912.nc    /.../cmip5/....nc 132768568'
        linelist = line.replace('\n','').replace(' ','\t').split('\t')
        if len(linelist)<3: continue    # invalid line, probably blank
        fnc = linelist[1]
        nfiles += 1
        if time.time()-ttime>10 or nfiles%100-1==0:   # progress to stdout
            ttime = time.time()
            print nfiles,
        if (not os.path.isfile(fnc)) or os.path.getsize(fnc)==0:
            nnofl += 1
            print "\nno file",fnc
            print num_lines,"files; now checking",nfiles,
            g.write( "no file "+fnc+"\n" )
        else:
            flen = eval(linelist[2])       # expected file size
            if os.path.getsize(fnc)!=flen:
                nbad += 1
                print "\nwrong file size,",fnc," is",os.path.getsize(fnc)," bytes, should be",flen
                print num_lines,"files; now checking",nfiles,
                g.write( "wrong file size, %s is %d bytes, should be %d\n"%(fnc,os.path.getsize(fnc),flen) )
            elif len(linelist)<=3:
                nnock +=1
                g.write( "no checksum available for "+fnc+"\n" )
                ckactual = md5(fnc)
                i.write( fnc+" | "+ckactual+"\n" )
            else:                    # checksum is available
                cksum = linelist[3]  # e.g. dfa5c368a6b76c80bf879ea178edcf5f
                if len(linelist)>4:
                    csumalg = linelist[4].lower()
                else:
                    csumalg = 'md5'
                if csumalg=='md5':
                    ckactual = md5(fnc)
                elif csumalg=='cksum':
                    proc = subprocess.Popen(['cksum',fnc],stdout=subprocess.PIPE)
                    ckout = proc.stdout.readlines()
                    proc.stdout.close()
                    # print "\njfp ckout=",ckout
                    ckactual = ckout[0].split('\t')[0]
                else:
                    nnock +=1
                    ckactual = None
                    print "\ndon't recognize checksum algorithm",csumalg," for",fnc
                    g.write( "don't recognize checksum algorithm %s\n"%csumalg )
                if ckactual!=None:
                    if ckactual==cksum:
                        ngood += 1
                        h.write( fnc+" | "+cksum+"\n" )
                        if csumalg!='md5':
                            ckactual = md5(fnc)
                            i.write( fnc+" | "+ckactual+"\n" )
                    else:
                        nbad += 1
                        print "\nbad checksum for",fnc
                        print num_lines,"files; now checking",nfiles,
                        g.write( "bad checksum for "+fnc+"\n" )

    print "\nsummary: %d good, %d bad, %d no checksums available, %d missing files"\
          % (ngood, nbad, nnock, nnofl )
    g.write ( "\nsummary: %d good, %d bad, %d no checksums available, %d missing files\n"\
              % (ngood, nbad, nnock, nnofl))
    f.close()
    g.close()
    h.close()
    i.close()
        

if len( sys.argv ) > 1:
#    file = sys.argv[1]
#    print md5( file ), file
    check_md5s( sys.argv[1] )
#    print_all_md5s( sys.argv[1] )

