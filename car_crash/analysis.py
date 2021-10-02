"""
Car Crash Analysis
"""
import os.path
import logging

from car_crash.utils import *

logger = logging.getLogger(__name__)

class Analysis:

    def __init__(self,spark):
        self.spark = spark
        self.df_dict = {}

    def start(self):

        logger.info("Reading configuration file...")
        self.conf = read_json("config/config.json")

        root_dir = self.conf['input_path']

        for df_name, filename in self.conf['input_files'].items():
            path = os.path.join(root_dir, filename)
            logger.info("Reading file: {}".format(df_name))
            try:
                self.df_dict[df_name] = self.read_csv_file(path)
            except:
                logger.info("filepath not present {}".format(path))

        # Solution of analytics 1
        count_of_accidents = analytics1(self.df_dict['driver_df'],
                                        self.conf['analytics1']['gender'])

        logger.info("""the number of crashes (accidents) in 
                      which number of persons killed are {} :{}"""
                    .format(self.conf['analytics1']['gender'], count_of_accidents))

        # Solution of analytics 2
        #self.df_dict['driver_df'], self.conf['analytics1']['gender']
        count_of_vehicle = analytics2(self.df_dict['vehicle_df'],
                                      self.conf['analytics2']['vehicle_type'])
        logger.info("""two wheelers are booked for crashes :{}""".format(count_of_vehicle))

        #solution of analytics 3
        state = analytics3(self.df_dict['driver_df'],
                           self.conf['analytics3']['gender'])

        logger.info("""State has highest number of accidents 
        in which {} are involved: {}""".format(self.conf['analytics3']['gender'], state))

        #Solution of Analytics 4
        df = analytics4(self.df_dict['vehicle_df'],
                        self.conf['analytics4']['top_n_vals'])

        logger.info("""Top 5th to 15th VEH_MAKE_IDs that contribute
         to a largest number of injuries including death""")
        df.show(20,False)

        #Solution of analytics 5
        df = analytics5(self.df_dict['vehicle_df'],
                        self.df_dict['driver_df'])

        logger.info("""top ethnic user group of each unique body style:""")
        df.show(20, False)

        #Solution of analytics 6
        df = analytics6(self.df_dict['driver_df'],
                        self.conf['analytics6']['top_n_zip_codes'],
                        self.conf['analytics6']['alchol_result'])
        logger.info("""Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash""")
        df.show(20,False)

        # Solution of analytics 7
        count = analytics7(self.df_dict['damages_df'],
                           self.df_dict['vehicle_df'])
        logger.info("""Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) 
        is above 4 and car avails Insurance: {}""".format(count))

        # Solution of analytics 8
        df = analytics8(self.df_dict['vehicle_df'],
                        self.df_dict['driver_df'],
                        self.df_dict['charges_df'],
                        self.conf['analytics8']['top_n_state'],
                        self.conf['analytics8']['top_n_colour'],
                        self.conf['analytics8']['top_n_models'])
        logger.info("""Top 5 Vehicle Makes where drivers are charged with speeding
         related offences, has licensed Drivers, uses top 10 used vehicle colours
          and has car licensed with the Top 25 states with highest number of offences """)
        df.show(5,False)

    def read_csv_file(self, path):
        return self.spark.read.csv(path, header="true")