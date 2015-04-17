import os
import sys
import shutil
import time
import re

ephys_temp_dir = sys.argv[1] 
raw_temp_dir = sys.argv[2]
ephys_dir = sys.argv[3]
raw_dir = sys.argv[4]
img_re = re.compile(sys.argv[5])
behav_re = re.compile(sys.argv[6])
delay = float(sys.argv[7])

def get_matching_files(dir, regex):
    def matching_files_iter():
        for root, dirs, files in os.walk(dir):
            for file in files:
                if regex.mach(file):
                    yield os.path.join(root, file)
    return sorted([f for f in matching_files_iter()])

for (f1, f2) in zip(get_matching_files(ephys_temp_dir, behav_re), get_matching_files(raw_temp_dir, img_re)):
    shutil.copy(os.path.join(ephys_temp_dir, f1), ephys_dir) 
    shutil.copy(os.path.join(raw_temp_dir, f2), raw_dir) 
    time.sleep(0.05)
