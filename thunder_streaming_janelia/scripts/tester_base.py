from thunder_streaming.shell.examples.lightning_updater import LightningUpdater
from thunder_streaming_janelia.feeder_configurations.configurations import FeederConfiguration
from subprocess import Popen
import os
import glob
import shutil
import random
import time
import signal

interrupted = False

int_handler = signal.getsignal(signal.SIGINT)
term_handler = signal.getsignal(signal.SIGTERM)

def new_handler(signum, stack):
    global interrupted
    int_handler(signum, stack)
    interrupted = True

signal.signal(signal.SIGINT, new_handler)
signal.signal(signal.SIGTERM, new_handler)

class AnalysisPipeline(object): 
    """
    Abstract base class for analysis pipeline, responsible for: 
    1) Setting up the feeder script (if necessary) 
    2) Copying data into the feeder scripts input directory
    3) Generating test data (if necessary)
    4) Cleaning up the results of the analysis, if requested, and resetting the pipeline
        to its initial state.
    """

    # These fields MUST be overridden by subclasses
    SAMPLE_DIR = None
    DATA_PATH = None

    @classmethod
    def getInstance(cls, tssc):
        """
        Factory method for generating an AnalysisPipeline instance with the default input/output directories
        """
        return cls(tssc, cls.DATA_PATH, cls.SAMPLE_DIR)

    def __init__(self, tssc, input_path, output_path, feeder_conf=FeederConfiguration()):
        self.tssc = tssc
        self.input_path = input_path
        self.output_path = output_path
        self.feeder_conf = feeder_conf

        self.dirs = {
            "checkpoint": os.path.join(self.output_path, "checkpoint"),
            "input": os.path.join(self.output_path, "streaminginput"),
            "output": os.path.join(self.output_path, "streamingoutput"),
            "images": os.path.join(self.output_path, "images"),
            "behaviors": os.path.join(self.output_path, "behaviors"),
            "temp": os.path.join(self.output_path, "temp")
        }

        self.run_params = {
            "master": self._get_master(),
            "executor_memory": "80g",
            "checkpoint_interval": 10000,
            "hadoop_block_size": 1
        }

        self.feeder_params = {
            "images_dir": self.dirs["images"],
            "behaviors_dir": self.dirs["behaviors"],
            "tmp": self.dirs["temp"],
            "spark_input_dir": self.dirs["input"]
        }

        self.copier_params = {}

        self.test_data_params = {}

    def setup_pipeline(self): 
        """
        Abstract method
        """
        pass

    def _get_master(self):
        """
        Parse the spark-master file to determine the URL of the master node
        """
        with open(os.path.expanduser('~/spark-master'), 'r') as f:
            return f.read().strip()

    # Attach all the parameters in the dictionary aboves to their respective objects
    def _attach_parameters(self): 
        for key, value in self.run_params.items():
            self.tssc.__dict__['set_'+key](value)
        self.tssc.set_checkpoint(self.dirs['checkpoint'])

    # Create the directories if they don't exist, clear them if they do
    def _set_up_directories(self): 
        for directory in self.dirs.values(): 
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
    def _generate_test_series(self, dirs): 
        def write_file(directory, i): 
            file_path = os.path.join(directory, self.test_data_params['prefix'] + str(i))
            print "Generating test series in %s..." % file_path
            with open(file_path, 'w') as output_file: 
                approx_size = float(self.test_data_params['approx_file_size'] * 1000000)
                series_len = int((approx_size / self.test_data_params['records_per_file']) / 8.0) - 1 
                for j in xrange(self.test_data_params['records_per_file']): 
                    output_file.write('%d ' % j)
                    for k in xrange(series_len):
                        output_file.write('%.2f ' % (random.random() * 10))
                    output_file.write('\n')
        for directory in dirs: 
            [write_file(directory, i) for i in xrange(self.test_data_params['num_files'])]

    # Copy data into the input directory at a certain rate 
    def _copy_data(self):
        copy_period = self.test_data_params['copy_period']
        num_files = self.test_data_params['num_files']
        for f in os.listdir(self.dirs['temp']): 
            print "Copying %s to input directory..." % f
            shutil.copy(os.path.join(self.dirs['temp'], f), self.dirs['input'])
            time.sleep(copy_period)
            if interrupted: 
                break

    def _generate_raw_test_data(self):
        pass

    def _make_feeder(self):
        conf = self.feeder_conf
        for key, value in self.feeder_params.items():
            if value is not None: 
                conf.__dict__['set_'+key](value)
            else: 
                conf.__dict__['set_'+key]()
       
        print "make_feeder returning: %s" % conf
        return conf

    def _build_regexes(self, regex_file): 
        regex_lines = open(regex_file, 'r').readlines()
        regexes = dict()
        for line in regex_lines: 
            splitted = line.split(' ')
            regexes[splitted[0].strip()] = splitted[1].strip()
        return regexes

    def _launch_copier(self, delay):
        """
        If the prefixes are specified, use those directly, else the regexes must be specified
        """

        image_prefix, behav_prefix = None, None

        if 'image_prefix' in self.feeder_params: 
            # If the prefixes are directly specified in the feeder params, use them
            image_prefix = self.feeder_params['image_prefix'] 
            behav_prefix = self.feeder_params['behaviors_prefix']
        elif 'image_prefix' in self.feeder_conf.params: 
            # If the prefixes are in the existing feeder configuration, use them 
            image_prefix = self.feeder_conf.params['image_prefix'] 
            behav_prefix = self.feeder_conf.params['behaviors_prefix']
        elif 'prefix_regexes' in self.feeder_conf.params:
            # If the prefixes aren't in either, then they must have been specified as prefix files 
            regexes = self._build_regexes(self.feeder_conf.params['prefix_regexes'])
            image_prefix = regexes['img']
            behav_prefix = regexes['behav']

        proc = Popen(['python', 'copier.py', self.copier_params['behaviors_dir'], self.copier_params['images_dir'],
                        self.feeder_params['behaviors_dir'], self.feeder_params['images_dir'],
                        image_prefix, behav_prefix, str(delay)])
            
    def run(self, copier=True):

        # Clean existing directories and do initial setup
        self._attach_parameters()
        self._set_up_directories() 

        # Configure the feeder
        feeder_conf = self._make_feeder()
        self.tssc.set_feeder_conf(feeder_conf)

        # Set up the analyses/outputs 
        self.setup_pipeline()

        # Start the Scala process
        self.tssc.start()

        # If data needs to be generated for the feeder, start the copier process
        if copier: 
            sleep_time = 10
            copy_delay = 0.5
            print "Sleeping for %d seconds before copying data..." % sleep_time
            time.sleep(sleep_time)
            print "Copying data into feeder script's input directories..."
            self._launch_copier(copy_delay)

