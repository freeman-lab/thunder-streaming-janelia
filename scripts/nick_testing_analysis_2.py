from thunder_streaming.shell.examples.lightning_updater import LightningUpdater
from thunder_streaming.site.configurations import *
from thunder import Colorize
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

class NicksAnalysis(AnalysisPipeline): 

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
        "master": "spark://h05u24.int.janelia.org:7077",
        "batch_time": 10,
        "executor_memory": "90g"
    }

    feeder_params = { 
        "images_dir": "/nobackup/freeman/andrew/nikitatest/raw/", 
        "behaviors_dir": "/nobackup/freeman/andrew/nikitatest/ephysSplitted/",
        "linger_time": -1, 
        "max_files": -1,
        "mod_buffer_time": 5,
        "poll_time": 5,
        "check_size": None,
        "tmp": self.dirs['temp'],
        "spark_input_dir": self.dirs['input']
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

    def setup_pipeline(self): 

        lgn = Lightning("http://kafka1.int.janelia.org:3000/")
        lgn.create_session('nikita_test')

        image_size = (512, 512)
        dims = [4, 512, 512]
        num_features = 2
        num_selected = 2
        #image_viz = lgn.imagedraw(zeros(image_size))
        regression_viz = lgn.imagedraw(zeros(image_size))
        #regression_viz = lgn.linestreaming(zeros((1, 1)), size=3)
        behav_viz = lgn.linestreaming(zeros((1, 1)), size=3)

        #analysis1 = Analysis.SeriesBatchMeanAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'images'), prefix="output", format="binary")\
        #                    .toImage(dims=tuple(dims), preslice=slice(0,-3,1))\
        #                    .toLightning(image_viz, image_size, only_viz=True, plane=10)
        #analysis2 = Analysis.SeriesMeanAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'mean'), 
        #                                        prefix="mean", format="binary")\
        #                    .toImage(dims=tuple(dims))\
        #                    .toLightning(regression_viz, image_size, only_viz=True, plane=10)
        #analysis2 = Analysis.SeriesStatsAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'stats'), 
        #                                        prefix="stats", format="binary")\
        #                    .toImage(dims=tuple(dims))\
        #                    .toLightning(regression_viz, image_size, only_viz=True, plane=10)

        analysis1 = Analysis.SeriesLinearRegressionAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'r_squared'),
                                                      prefix="r", format="binary", dims=str(dims), num_regressors=num_features,
                                                      selected=str([x for x in xrange(num_selected)]))\
                            .toImage(dims=tuple([num_selected + 2] + dims), preslice=slice(0, -num_features, 1))\
                            .toLightning(regression_viz, image_size, only_viz=True, plane=3)

        #analysis2 = Analysis.SeriesFilteringRegressionAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'fitted_series'),
        #                                                        prefix="fitted", format="binary", partition_size="6", dims=str([41, 1024, 2048]),
        #                                                        num_regressors="3")\
        #                    .toSeries().toLightning(regression_viz, only_viz=True)

        #analysis2.receive_updates(analysis1)

        tssc.add_analysis(analysis1)

        updaters = [
        #    LightningUpdater(tssc, image_viz, analysis1.identifier)
        ]

        for updater in updaters: 
            tssc.add_updater(updater)
