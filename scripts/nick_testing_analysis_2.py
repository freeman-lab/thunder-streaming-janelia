from thunder_streaming.shell.examples.lightning_updater import LightningUpdater
from thunder_streaming.site.configurations import *
from tester_base import *
from lightning import Lightning
from numpy import zeros
import os

class NicksAnalysis(AnalysisPipeline):

    SAMPLE_DIR = "/tier2/freeman/streaming/sample_data/"
    DATA_PATH = "/groups/freeman/freemanlab/Streaming/anm_0216166_2013_07_17_run_01/"

    def __init__(self, tssc, input_path, output_path):
        super(NicksAnalysis, self).__init__(tssc, input_path, output_path)
        self.run_params.update({
            "parallelism": 320,
            "batch_time": 10,
        })

        self.feeder_params.update({
            "linger_time": -1,
            "max_files": -1,
            "mod_buffer_time": 5,
            "poll_time": 5,
            "image_prefix": "images",
            "behaviors_prefix": "behaviour"
        })

        self.copier_params.update({
            "images_dir": os.path.join(self.input_path, "registered_im"),
            "behaviors_dir": os.path.join(self.input_path, "registered_bv")
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
        dims = [4, 512, 512]
        num_features = 2
        num_selected = 1
        #image_viz = lgn.imagedraw(zeros(image_size))
        regression_viz = lgn.imagedraw(zeros(image_size))
        mean_viz = lgn.imagedraw(zeros(image_size))
        behav_viz = lgn.linestreaming(zeros((1, 1)), size=1)

        #analysis1 = Analysis.SeriesBatchMeanAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'images'), prefix="output", format="binary")\
        #                    .toImage(dims=tuple(dims), preslice=slice(0,-3,1))\
        #                    .toLightning(image_viz, image_size, only_viz=True, plane=10)
        analysis2 = Analysis.SeriesMeanAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'mean'),
                                                prefix="mean", format="binary")\
                            .toImage(dims=tuple(dims))\
                            .toLightning(mean_viz, image_size, only_viz=True, plane=3)
        #analysis2 = Analysis.SeriesStatsAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'stats'), 
        #                                        prefix="stats", format="binary")\
        #                    .toImage(dims=tuple(dims))\
        #                    .toLightning(regression_viz, image_size, only_viz=True, plane=10)

        analysis1 = Analysis.SeriesBinnedRegressionAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'weighted_mean'),
                                                      prefix="m", format="binary", dims=str(dims), num_regressors=str(num_features),
                                                      selected=str(num_selected), edges=str([x for x in xrange(0, 30, 2)]))\
                            .toImage(dims=tuple([num_selected + 2] + dims), preslice=slice(0, -num_features, 1))\
                            .colorize()\
                            .toLightning(regression_viz, image_size, only_viz=True, plane=3)

        #analysis2 = Analysis.SeriesFilteringRegressionAnalysis(input=self.dirs['input'], output=os.path.join(self.dirs['output'], 'fitted_series'),
        #                                                        prefix="fitted", format="binary", partition_size="6", dims=str([41, 1024, 2048]),
        #                                                        num_regressors="3")\
        #                    .toSeries().toLightning(regression_viz, only_viz=True)

        #analysis2.receive_updates(analysis1)

        self.tssc.add_analysis(analysis1)
        self.tssc.add_analysis(analysis2)

        updaters = [
        #    LightningUpdater(self.tssc, image_viz, analysis1.identifier)
        ]

        for updater in updaters: 
            self.tssc.add_updater(updater)
