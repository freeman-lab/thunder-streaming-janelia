from thunder_streaming.shell.examples.lightning_updater import LightningUpdater
from thunder_streaming_janelia.feeder_configurations.configurations import *
from thunder_streaming_janelia.scripts.tester_base import *
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

class NikitasAnalysis(AnalysisPipeline): 

    SAMPLE_DIR = "/tier2/freeman/streaming/sample_data/" 
    DATA_PATH = "/nobackup/freeman/andrew/nikitatest/"

    def __init__(self, tssc, input_path, output_path): 
        feeder_conf = NikitasFeederConf
        super(NikitasAnalysis, self).__init__(tssc, input_path, output_path, feeder_conf=feeder_conf)

        self.run_params.update({ 
            "parallelism": 960, 
            "batch_time": 30,
        })

        self.feeder_params.update({ 
            "linger_time": -1, 
            "max_files": 40, 
            "mod_buffer_time": 5,
            "poll_time": 10,
            "check_size": None,
        })

        self.copier_params.update({ 
            "images_dir": "/nobackup/freeman/andrew/nikitatest/rawTemp/",
            "behaviors_dir": "/nobackup/freeman/andrew/nikitatest/ephysTemp/"
        })

        self.test_data_params.update({ 
            "prefix": "input_",
            "num_files": 100,
            "approx_file_size": 5.0,
            "records_per_file": 512 * 512,
            "copy_period": 10
        })

    def setup_pipeline(self):

        lgn = Lightning("http://kafka1.int.janelia.org:3000/")
        lgn.create_session('nikita_test')

        image_size = (512, 512)
        dims = [41, 1024, 2048]
        num_features = 3
        num_selected = 3
        #image_viz = lgn.imagepoly(zeros(image_size))
        regression_viz = lgn.imagepoly(zeros(image_size))
        #regression_viz = lgn.linestreaming(zeros((1, 1)), size=3)
        behav_viz = lgn.linestreaming(zeros((1, 1)), size=3)

        #analysis1 = Analysis.SeriesBatchMeanAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'images'), prefix="output", format="binary")\
        #                    .toImage(dims=tuple(dims), preslice=slice(0,-3,1))\
        #                    .toLightning(image_viz, image_size, only_viz=True, plane=10)
        #analysis2 = Analysis.SeriesMeanAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'mean'), 
        #                                        prefix="mean", format="binary")\
        #                    .toImage(dims=tuple(dims))\
        #                    .toLightning(regression_viz, image_size, only_viz=True, plane=10)
        #analysis2 = Analysis.SeriesStatsAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'stats'), 
        #                                        prefix="stats", format="binary")\
        #                    .toImage(dims=tuple(dims))\
        #                    .toLightning(regression_viz, image_size, only_viz=True, plane=10)
        analysis2 = Analysis.SeriesLinearRegressionAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'regression'),
                                      prefix="r", format="binary", dims=str(dims), num_regressors=str(num_features),
                                      selected=str([x for x in xrange(num_selected)]))\
                            .toImage(dims=tuple([num_selected + 2] + dims), preslice=slice(0, -num_features, 1))\
                            .getPlane(10)\
                            .colorize(vmax=1550)\
                            .toLightning(regression_viz, image_size, only_viz=True)

        #analysis2 = Analysis.SeriesFilteringRegressionAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'fitted_series'),
        #                                                        prefix="fitted", format="binary", partition_size="6", dims=str([41, 1024, 2048]),
        #                                                        num_regressors="3")\
        #                    .toSeries().toLightning(regression_viz, only_viz=True)

        #analysis2 = Analysis.SeriesFiltering2Analysis(input=dirs['input'], output=os.path.join(dirs['output'], 'filtered_series'), prefix="output", format="binary", partition_size="6", dims=str([2048, 1024, 41])).toSeries().toLightning(line_viz, only_viz=True)
        #analysis3 = Analysis.SeriesNoopAnalysis(input=dirs['input'], output=os.path.join(dirs['output'], 'no_means'), prefix="output", format="binary").toImage(dims=(512,512), plane=0).toLightning(no_mean_viz, only_viz=True)

        #analysis2.receive_updates(analysis1)

        #tssc.add_analysis(analysis1)
        self.tssc.add_analysis(analysis2)
        #tssc.add_analysis(analysis3)

        updaters = [
        #    LightningUpdater(tssc, image_viz, analysis1.identifier)
        ]

        for updater in updaters: 
            tssc.add_updater(updater)
