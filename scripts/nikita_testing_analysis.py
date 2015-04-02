from thunder.streaming.shell.examples.lightning_updater import LightningUpdater
from thunder.streaming.site.configurations import *
from subprocess import Popen
from lightning import Lightning
import numpy as np
from numpy import zeros 
import os
import glob
import math
import shutil
import random
import time
import signal

SAMPLE_DIR = "/tier2/freeman/streaming/sample_data/" 

interrupted = False

int_handler = signal.getsignal(signal.SIGINT)
term_handler = signal.getsignal(signal.SIGTERM)

def new_handler(signum, stack): 
    global interrupted
    int_handler(signum, stack)
    interrupted = True

signal.signal(signal.SIGINT, new_handler) 
signal.signal(signal.SIGTERM, new_handler)

dirs = {
    "checkpoint": os.path.join(SAMPLE_DIR, "checkpoint"),
    "input": os.path.join(SAMPLE_DIR, "streaminginput"),
    "output": os.path.join(SAMPLE_DIR, "streamingoutput"),
    "images": os.path.join(SAMPLE_DIR, "images"),
    "behaviors": os.path.join(SAMPLE_DIR, "behaviors"),
    "temp": os.path.join(SAMPLE_DIR, "temp")
}

run_params = { 
    "checkpoint_interval": 10000, 
    "hadoop_block_size": 1, 
    "parallelism": 320, 
    "master": "spark://h08u01.int.janelia.org:7077",
    "batch_time": 20,
    "executor_memory": "80g"
}

feeder_params = { 
    "images_dir": "/tier2/ahrens/Nikita/realtime/imaging/", 
    "behaviors_dir": "/tier2/ahrens/Nikita/realtime/ephys/",
    "linger_time": -1, 
    "max_files": 40, 
    "mod_buffer_time": 5,
    "poll_time": 5,
    "check_size": None,
    "tmp": dirs['temp'],
    "spark_input_dir": dirs['input']
}

copier_params = { 
    "images_dir": "/nobackup/freeman/andrew/nikitatest/rawTemp/",
    "behaviors_dir": "/nobackup/freeman/andrew/nikitatest/ephysTemp/"
}

test_data_params = { 
    "prefix": "input_",
    "num_files": 100,
    "approx_file_size": 5.0,
    "records_per_file": 512 * 512,
    "copy_period": 10
}

##########################################
# Analysis configuration stuff starts here
##########################################

# TODO Need to insert the Lightning client here
lgn = Lightning("http://kafka1.int.janelia.org:3000/")
lgn.create_session('nikita_test')

image_size = (512, 512)
dims = [41, 1024, 2048]
#image_viz = lgn.imagedraw(zeros(image_size))
r2_viz = lgn.imagedraw(zeros(image_size))
#regression_viz = lgn.linestreaming(zeros((1, 1)), size=3)
behav_viz = lgn.linestreaming(zeros((1, 1)), size=3)

#analysis1 = Analysis.SeriesBatchMeanAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'images'), prefix="output", format="binary")\
#                    .toImage(dims=tuple(dims), preslice=slice(0,-3,1))\
#                    .toLightning(image_viz, image_size, only_viz=True, plane=10)
analysis2 = Analysis.SeriesRegressionAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'r_squared'),
                                              prefix="r", format="binary", dims=str(dims), num_regressors="3",
                                              selected="1")\
                    .toImage(dims=tuple(dims))\
                    .toLightning(r2_viz, image_size, only_viz=True)

#analysis2 = Analysis.SeriesFilteringRegressionAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'fitted_series'),
#                                                        prefix="fitted", format="binary", partition_size="6", dims=str([41, 1024, 2048]),
#                                                        num_regressors="3")\
#                    .toSeries().toLightning(regression_viz, only_viz=True)

#analysis2 = Analysis.SeriesFiltering2Analysis(input=dirs['input'], output=os.path.join(dirs['output'], 'filtered_series'), prefix="output", format="binary", partition_size="6", dims=str([2048, 1024, 41])).toSeries().toLightning(line_viz, only_viz=True)
#analysis3 = Analysis.SeriesNoopAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'no_means'), prefix="output", format="binary").toImage(dims=(512,512), plane=0).toLightning(no_mean_viz, only_viz=True)

#analysis2.receive_updates(analysis1)

#tssc.add_analysis(analysis1)
tssc.add_analysis(analysis2)
#tssc.add_analysis(analysis3)

updaters = [
#    LightningUpdater(tssc, image_viz, analysis1.identifier)
]

for updater in updaters: 
    tssc.add_updater(updater)

########################################
# Analysis configuration stuff ends here
########################################

# Attach all the parameters in the dictionary aboves to their respective objects
def attach_parameters(): 
    for key, value in run_params.items():
        tssc.__dict__['set_'+key](value)
    tssc.set_checkpoint(dirs['checkpoint'])

# Create the directories if they don't exist, clear them if they do
def set_up_directories(): 
    for directory in dirs.values(): 
        if not os.path.exists(directory): 
            os.makedirs(directory)
        else: 
            files = glob.glob(os.path.join(directory, "*"))
            try: 
                for f in files: 
                    os.unlink(f)
            except Exception as e:
                print e

# Populate the images/behaviors directories with test data 
def generate_test_series(dirs): 
    def write_file(directory, i): 
        file_path = os.path.join(directory, test_data_params['prefix'] + str(i))
        print "Generating test series in %s..." % file_path
        with open(file_path, 'w') as output_file: 
            approx_size = float(test_data_params['approx_file_size'] * 1000000)
            series_len = int((approx_size / test_data_params['records_per_file']) / 8.0) - 1 
            for j in xrange(test_data_params['records_per_file']): 
                output_file.write('%d ' % j)
                for k in xrange(series_len):
                    output_file.write('%.2f ' % (random.random() * 10))
                output_file.write('\n')
    for directory in dirs: 
        [write_file(directory, i) for i in xrange(test_data_params['num_files'])]

# Copy data into the input directory at a certain rate 
def copy_data():
    copy_period = test_data_params['copy_period']
    num_files = test_data_params['num_files']
    for f in os.listdir(dirs['temp']): 
        print "Copying %s to input directory..." % f
        shutil.copy(os.path.join(dirs['temp'], f), dirs['input'])
        time.sleep(copy_period)
        if interrupted: 
            break

def generate_raw_test_data(): 
    pass

def make_feeder(name="nick"): 
    conf = None
    if name == "nick": 
        conf = NicksFeederConf
    elif name == 'nikita': 
        conf = NikitasFeederConf
    else: 
        conf = FeederConfiguration()

    for key, value in feeder_params.items(): 
        if value is not None: 
            conf.__dict__['set_'+key](value)
        else: 
            conf.__dict__['set_'+key]()
   
    print "make_feeder returning: %s" % conf
    return conf

def launch_copier(delay):
    proc = Popen(['python', 'copier.py', copier_params['behaviors_dir'], copier_params['images_dir'],
                    feeder_params['behaviors_dir'], feeder_params['images_dir'], str(delay)]) 

def run(feeder=None): 

    attach_parameters()
    set_up_directories() 
    if feeder: 
        feeder_conf = make_feeder(name=feeder)
        tssc.set_feeder_conf(feeder_conf)
    if not feeder:
        generate_test_series([dirs['temp']])

    tssc.start()

    sleep_time = 10
    copy_delay = 0.5
    print "Sleeping for %d seconds..." % sleep_time
    time.sleep(sleep_time)
    if not feeder: 
        copy_data()
    else: 
        print "Copying data into feeder script's input directories..."
        #launch_copier(copy_delay)


