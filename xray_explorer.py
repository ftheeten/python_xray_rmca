import os
import re
entry_point="//drosera/RX"
save_file="D:\\rayons_x\\output_rayx.csv"

try:
    from os import scandir
except ImportError:
    from scandir import scandir  # use scandir PyPI module on Python < 3.5

def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)  # see below for Python 2.x
        else:
            yield entry

def csv_exists(p_file):
    
    p = re.compile("/+|\\\\+")
    tmp=p.split(p_file)
    file_rel=tmp[-1] 
    tmp=tmp[0:-1]       
    tmp=os.path.join(*tmp)  
    
    list_f=os.listdir('\\\\'+tmp)
    to_search=file_rel+".csv"
    returned="NO_METADATA",""
    if  to_search in list_f:
        f = open('\\\\'+tmp+'\\'+to_search, "r")
        for line in f:
            #print(line)
            if line.startswith(";Image shot object;"):                
                tmp2=line.split(";")
                desc=tmp2[2]
                if(len(desc.strip())>0):
                    returned="METADATA_FOUND",desc.strip()
        f.close()     
    return returned
  
if __name__ == '__main__':
    import sys
    p = re.compile("/+|\\\\+")
    f = open(save_file, "w")
    header='full_path\tfile\tfolder\tmetadata_found\tobject\n'
    f.write(header)
    for entry in scantree(entry_point):
        path='\\'.join(p.split(entry.path)[0:-1])
        extension = os.path.splitext(entry)[1]
        if extension.lower() !=".csv":                       
            vals=csv_exists(entry.path)
            to_write=[entry.path, p.split(entry.path)[-1],p.split(entry.path)[-2], vals[0], vals[1]]
            f.write("\t".join(to_write)+'\n')
    f.close()