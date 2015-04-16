import os
import sys
import shutil
import time
import re

ephys_temp_dir = sys.argv[1] 
raw_temp_dir = sys.argv[2]
ephys_dir = sys.argv[3]
raw_dir = sys.argv[4]
delay = float(sys.argv[5])

data_re = re.compile('TM.*')

def get_matching_files(dir): 
    return sorted([f for f in os.listdir(dir) if data_re.match(f)])

for (f1, f2) in zip(get_matching_files(ephys_temp_dir), get_matching_files(raw_temp_dir)): 
    shutil.copy(os.path.join(ephys_temp_dir, f1), ephys_dir) 
    shutil.copy(os.path.join(raw_temp_dir, f2), raw_dir) 
    time.sleep(0.05)
