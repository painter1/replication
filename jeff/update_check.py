# compare a dict of current files, computed by get_tracking_id.py
# with a dict of the latest files.
# For now, the two dicts should be in files currentd.py and newd.py
# and named current_dict and new_dict
from currentd import *
from newd import *

for f in current_dict.keys():
    if f in new_dict.keys():
        if current_dict[f]!=new_dict[f]:
            print f,"is out of date"
    else:
        print f,"is not in new_dict"
