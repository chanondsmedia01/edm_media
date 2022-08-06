# Databricks notebook source
dbutils.fs.rm('dbfs:/FileStore/media/campaign_eval/02_gofresh/Mar_2022/2022_0124_M01E_pepsi_wobbler', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Go Fresh Evaluation --> Using Promo Week to do evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC #Import Library

# COMMAND ----------

# MAGIC %md ## Standard Library

# COMMAND ----------

## import function
##---------------------------------------------------
## Need to import all function in each notebook
##---------------------------------------------------

## import pyspark sql

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

from pyspark import StorageLevel


## import longging and traceback
import logging
import traceback
import errno

## datetime lib
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time

## pandas and numpy
import pandas as pd
import numpy as np
import math as math

## os path
import os
import sys
import string
import subprocess
import importlib
import shutil
import urllib
import pathlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Function

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

sys.path

# COMMAND ----------

from instore_eval import get_cust_activated, get_cust_movement, get_cust_brand_switching_and_penetration, get_cust_sku_switching, get_profile_truprice, get_customer_uplift

# COMMAND ----------

import inspect
src_txt = inspect.getsource(get_cust_activated)
print(src_txt)

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all parameter

# COMMAND ----------

#dbutils.widgets.removeAll()

## Test create and read input argurment
dbutils.widgets.text('cmp_id', defaultValue='', label='01_cmp_id value = :')
dbutils.widgets.text('cmp_nm', defaultValue='', label='02_cmp_nm value = :')
dbutils.widgets.text('eval_type', defaultValue='', label='03_eval_type value = :')
dbutils.widgets.text('cmp_start', defaultValue='', label='04_cmp_start value = :')
dbutils.widgets.text('cmp_end', defaultValue='', label='05_cmp_end value = :')
dbutils.widgets.text('cmp_month', defaultValue='', label='06_cmp_month value = :')
dbutils.widgets.text('store_fmt', defaultValue='', label='07_store_fmt value = :')
dbutils.widgets.text('cmp_objective', defaultValue='', label='08_cmp_objective value = :')
dbutils.widgets.text('media_fee', defaultValue='', label='09_media_fee value = :')
dbutils.widgets.text('cate_lvl', defaultValue='', label='10_cate_lvl value = :')
dbutils.widgets.text('use_reserved_store', defaultValue='', label='11_use_reserved_store value = :')
dbutils.widgets.text('resrv_store_class', defaultValue='', label='12_resrv_store_class value = :')
dbutils.widgets.text('cross_cate_flag', defaultValue='', label='13_cross_cate_flag value = :')
dbutils.widgets.text('cross_cate_cd', defaultValue='', label='14_cross_cate_cd value = :')
dbutils.widgets.text('media_mechanic', defaultValue='', label='15_media_mechanic value = :')
dbutils.widgets.text('media_loc', defaultValue='', label='16_media_loc value = :')
dbutils.widgets.text('mechanic_count', defaultValue='', label='17_mechanic_count value = :')
dbutils.widgets.text('media_stores', defaultValue='', label='18_media_stores value = :')
dbutils.widgets.text('hv_ctrl_store', defaultValue='', label='19_hv_ctrl_store value = :')
dbutils.widgets.text('resrv_store_file', defaultValue='', label='20_resrv_store_file value = :')
dbutils.widgets.text('adjacency_file', defaultValue='', label='21_adjacency_file value = :')
dbutils.widgets.text('dbfs_project_path', defaultValue='', label='22_dbfs_project_path_spark_api value = :')
dbutils.widgets.text('input_path', defaultValue='', label='23_Input file path value = :')
dbutils.widgets.text('sku_file', defaultValue='', label='24_SKU file path value = :')
dbutils.widgets.text('target_file', defaultValue='', label='25_Target stores file value = :')
dbutils.widgets.text('control_file', defaultValue='', label='26_Control stores file value = :')
dbutils.widgets.text('gap_start_date', defaultValue='', label='27_gap start date value = :')
dbutils.widgets.text('gap_end_date', defaultValue='', label='28_gap end date value = :')
dbutils.widgets.text('svv_table', defaultValue='', label='29_survival_rate_table value = :')
dbutils.widgets.text('pcyc_table', defaultValue='', label='30_purchase_cycle_table value = :')

## get value from widgets to variable
cmp_id             = dbutils.widgets.get('cmp_id').strip()
cmp_nm             = dbutils.widgets.get('cmp_nm').strip()
eval_type          = dbutils.widgets.get('eval_type').strip().lower()
cmp_start          = dbutils.widgets.get('cmp_start')
cmp_end            = dbutils.widgets.get('cmp_end')
cmp_month          = dbutils.widgets.get('cmp_month')
store_fmt          = dbutils.widgets.get('store_fmt').strip().lower()
cmp_objective      = dbutils.widgets.get('cmp_objective').strip()
media_fee          = dbutils.widgets.get('media_fee')
cate_lvl           = dbutils.widgets.get('cate_lvl').strip().lower()
use_reserved_store = dbutils.widgets.get('use_reserved_store')
resrv_store_class  = dbutils.widgets.get('resrv_store_class').strip()
cross_cate_flag    = dbutils.widgets.get('cross_cate_flag').strip().lower()
cross_cate_cd      = dbutils.widgets.get('cross_cate_cd').strip()
media_mechanic     = dbutils.widgets.get('media_mechanic').strip()
media_loc          = dbutils.widgets.get('media_loc').strip()
mechanic_count     = dbutils.widgets.get('mechanic_count')
media_stores       = dbutils.widgets.get('media_stores')
hv_ctrl_store      = dbutils.widgets.get('hv_ctrl_store')
resrv_store_file   = dbutils.widgets.get('resrv_store_file').strip()
adjacency_file     = dbutils.widgets.get('adjacency_file').strip()
dbfs_project_path  = dbutils.widgets.get('dbfs_project_path')
input_path         = dbutils.widgets.get('input_path')
sku_file           = dbutils.widgets.get('sku_file').strip()
target_file        = dbutils.widgets.get('target_file')
control_file       = dbutils.widgets.get('control_file')
gap_start_date     = dbutils.widgets.get('gap_start_date')
gap_end_date       = dbutils.widgets.get('gap_end_date')
svv_table          = dbutils.widgets.get('svv_table')
pcyc_table         = dbutils.widgets.get('pcyc_table')

# COMMAND ----------

print( ' cmp_id value = : '  +  cmp_id + '\n')
print( ' cmp_nm value = : '  +  cmp_nm + '\n')
print( ' eval_type value = : '  +  eval_type + '\n')
print( ' cmp_start value = : '  +  cmp_start + '\n')
print( ' cmp_end value = : '  +  cmp_end + '\n')
print( ' cmp_month value = : '  +  cmp_month + '\n')
print( ' store_fmt value = : '  +  store_fmt + '\n')
print( ' cmp_objective value = : '  +  cmp_objective + '\n')
print( ' media_fee value = : '  +  media_fee + '\n')
print( ' cate_lvl value = : '  +  cate_lvl + '\n')
print( ' use_reserved_store value = : '  +  use_reserved_store + '\n')
print( ' resrv_store_class value = : '  +  resrv_store_class + '\n')
print( ' cross_cate_flag value = : '  +  cross_cate_flag + '\n')
print( ' cross_cate_cd value = : '  +  cross_cate_cd + '\n')
print( ' media_mechanic value = : '  +  media_mechanic + '\n')
print( ' media_loc value = : '  +  media_loc + '\n')
print( ' mechanic_count value = : '  +  mechanic_count + '\n')
print( ' media_stores value = : '  +  media_stores + '\n')
print( ' hv_ctrl_store value = : '  +  hv_ctrl_store + '\n')
print( ' resrv_store_file value = : '  +  resrv_store_file + '\n')
print( ' adjacency_file value = : '  +  adjacency_file + '\n')
print( ' dbfs_project_path value = : '  +  dbfs_project_path + '\n')
print( ' input_path value = : '  +  input_path + '\n')
print( ' sku_file value = : '  +  sku_file + '\n')
print( ' target_file value = : '  +  target_file + '\n')
print( ' control_file value = : '  +  control_file + '\n')
print( ' gap start date value = : '  +  gap_start_date + '\n')
print( ' gap end date value = : '  +  gap_end_date + '\n')
print( ' survival rate table value = : ' + svv_table + '\n')
print( ' purchase_cycle table value = : ' + pcyc_table + '\n')

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup date period parameter

# COMMAND ----------

## cmp_start = campaign start date in format yyyy-mm-dd
## cmp_end   = campaign end date in format yyyy-mm-dd

## get date and convert back to string
## fis_week
cmp_st_wk   = wk_of_year_ls(cmp_start)
cmp_en_wk   = wk_of_year_ls(cmp_end)

## promo_wk
cmp_st_promo_wk   = wk_of_year_promo_ls(cmp_start)
cmp_en_promo_wk   = wk_of_year_promo_ls(cmp_end)

## Gap Week (fis_wk)
if ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == '')) & ((str(gap_end_date).lower == 'nan') | (str(gap_end_date).strip() == '')):
    print('No Gap Week for campaign :' + str(cmp_nm))
    gap_flag    = False
    chk_pre_wk  = cmp_st_wk
    chk_pre_dt  = cmp_start
elif( (not ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == ''))) &
      (not ((str(gap_end_date).lower() == 'nan')   | (str(gap_end_date).strip() == ''))) ):
    print('\n Campaign ' + str(cmp_nm) + ' has gap period between : ' + str(gap_start_date) + ' and ' + str(gap_end_date) + '\n')
    ## fis_week
    gap_st_wk   = wk_of_year_ls(gap_start_date)
    gap_en_wk   = wk_of_year_ls(gap_end_date)

    ## promo
    gap_st_promo_wk  = wk_of_year_promo_ls(gap_start_date)
    gap_en_promo_wk  = wk_of_year_promo_ls(gap_end_date)

    gap_flag         = True

    chk_pre_dt       = gap_start_date
    chk_pre_wk       = gap_st_wk
    chk_pre_promo_wk = gap_st_promo_wk

else:
    print(' Incorrect gap period. Please recheck - Code will skip !! \n')
    print(' Received Gap = ' + str(gap_start_date) + " and " + str(gap_end_date))
    raise Exception("Incorrect Gap period value please recheck !!")
## end if

pre_en_date = (datetime.strptime(chk_pre_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
pre_en_wk   = wk_of_year_ls(pre_en_date)
pre_st_wk   = week_cal(pre_en_wk, -12)                       ## get 12 week away from end week -> inclusive pre_en_wk = 13 weeks
pre_st_date = f_date_of_wk(pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data
## promo week
pre_en_promo_wk = wk_of_year_promo_ls(pre_en_date)
pre_st_promo_wk = promo_week_cal(pre_en_promo_wk, -12)

ppp_en_wk       = week_cal(pre_st_wk, -1)
ppp_st_wk       = week_cal(ppp_en_wk, -12)
##promo week
ppp_en_promo_wk = promo_week_cal(pre_st_promo_wk, -1)
ppp_st_promo_wk = promo_week_cal(ppp_en_promo_wk, -12)

ppp_st_date = f_date_of_wk(ppp_en_wk).strftime('%Y-%m-%d')
ppp_en_date = f_date_of_wk(ppp_st_wk).strftime('%Y-%m-%d')



print('\n' + '-'*80 + '\n Date parameter for campaign ' + str(cmp_nm) + ' shown below \n' + '-'*80 )
print('Campaign period between ' + str(cmp_start) + ' and ' + str(cmp_end) + '\n')
print('Campaign is during Promo week ' + str(cmp_st_promo_wk) + ' to ' + str(cmp_en_promo_wk) + '\n')
print('Campaign pre-period (13 weeks) between week ' + str(pre_st_wk) + ' and week ' + str(pre_en_wk) + ' \n')
print('Campaign pre-period (13 weeks) between promo week ' + str(pre_st_promo_wk) + ' and week ' + str(pre_en_promo_wk) + ' \n')
print('Campaign pre-period (13 weeks) between date ' + str(pre_st_date) + ' and week ' + str(pre_en_date) + ' \n')

print('Campaign prior period (13+13 weeks) between week ' + str(ppp_st_wk) + ' and week ' + str(ppp_en_wk) + ' \n')
print('Campaign prior period (13+13 weeks) between promo week ' + str(ppp_st_promo_wk) + ' and week ' + str(ppp_en_promo_wk) + ' \n')
print('Campaign prior period (13+13 weeks) between date ' + str(ppp_st_date) + ' and week ' + str(ppp_en_date) + ' \n')

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup data path to run

# COMMAND ----------

## ----------------------------------------------
## setup file path Spark API
## ----------------------------------------------

mpath      = 'dbfs:/FileStore/media/campaign_eval/'
stdin_path = mpath + '00_std_inputs/'
eval_path  = mpath + '02_gofresh/'
## input path for evaluaton
incmp_path   = eval_path + '00_cmp_inputs/'
input_path = incmp_path + 'inputs_files/'

cmp_out_path = eval_path + cmp_month + '/' + cmp_nm + '/'
print(cmp_out_path)

## ----------------------------------------------
## setup file path File API
## ----------------------------------------------

mpath_fl     = '/dbfs/FileStore/media/campaign_eval/'
stdin_path_fl = mpath_fl + '00_std_inputs/'
eval_path_fl  = mpath_fl + '02_gofresh/'
## input path for evaluaton
incmp_path_fl = eval_path_fl + '00_cmp_inputs/'
input_path_fl = incmp_path_fl + 'inputs_files/'
cmp_out_path = eval_path_fl + cmp_month + '/' + cmp_nm + '/'

## ----------------------------------------------
## setup path for noteboook to run
## ----------------------------------------------
dbs_nb_path = '/EDM_Share/EDM_Media/Campaign_Evaluation/Instore/gofresh/'



# COMMAND ----------

# MAGIC %md
# MAGIC # Get all input file name

# COMMAND ----------

print('\n' + '-'*80 + '\n' )
in_sku_file = input_path_fl + sku_file
in_trg_file = input_path + target_file
##
#in_ai_file  = stdin_path + adjacency_file
##

print(' Input sku file = ' + in_sku_file )
print(' Input target file = ' + in_trg_file )
#print(' Input Product Adjacency file = ' + in_ai_file )

## control need check
if (eval_type == 'full') & (hv_ctrl_store == 'true'):
    in_ctl_file = input_path + control_file
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Control store file for campaign ' + str(cmp_nm) + ' : ' + in_ctl_file + '\n')
elif (eval_type == 'full') & (hv_ctrl_store != 'true') & (use_reserved_store == 'true'):
    in_ctl_file = stdin_path + resrv_store_file
    flg_use_oth = False
    flg_use_rsv = True
    print('\n Campaign will use standard reserved store . \n ')
elif (eval_type == 'full') & (hv_ctrl_store  != 'true') & (use_reserved_store != 'true'):
    flg_use_oth = True
    flg_use_rsv = False
    print('\n Campaign will use the rest store for matching !! . \n ')
elif (eval_type == 'std'):
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Campaign ' + str(cmp_nm) + ' do not need control store. \n')
else:
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Campaign ' + str(cmp_nm) + ' do not need control store. \n')
## end if


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prep feature product, brand, class, subclass, aisle_use

# COMMAND ----------

#Test function get product info
##----------------------------------------------
## read sku file to list
##----------------------------------------------
feat_pd = pd.read_csv(in_sku_file)
#feat_pd.display()
feat_list = feat_pd['feature'].drop_duplicates().to_list()
print('-'*80 + '\n List of feature SKU show below : \n ' + '-'*80)
print(feat_list)

## Import Adjacency file
#std_ai_df    = spark.read.csv(in_ai_file, header="true", inferSchema="true")

# function get product info - no aisle info for Go Fresh

feat_df, brand_df, class_df, sclass_df, cate_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list = _get_prod_df_no_aisle( feat_list
                                                                                                                                                                                                                                              ,cate_lvl
                                                                                                                                                                                                                                              ,cross_cate_flag
                                                                                                                                                                                                                                              ,cross_cate_cd)

## return feat_df, brand_df, class_df, sclass_df, cate_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list

# COMMAND ----------

feat_df.printSchema()

# COMMAND ----------

#feat_df.display(5)
feat_detail = feat_df.select( feat_df.div_nm.alias('division_name')
                             ,feat_df.dept_nm.alias('department_name')
                             ,feat_df.sec_nm.alias('section_name')
                             ,feat_df.class_nm.alias('class_name')
                             ,feat_df.sclass_nm.alias('subclass_name')
                             ,feat_df.brand_nm.alias('brand_name')
                             ,feat_df.upc_id
                             ,feat_df.prod_en_desc
                             ,feat_df.mfr_name.alias('manufactor_name')
                            )
feat_detail.display()

# COMMAND ----------

# MAGIC %md ##Prep input files

# COMMAND ----------

#trg_str_df.printSchema()
# store_dim = sqlContext.table('tdm.v_store_dim')
# store_dim.printSchema()

# COMMAND ----------

##---------------------
## Prep store dim
##---------------------
## for HDE only, for gofresh need o change store filer to 5 and combine region (central + west)

if store_fmt == 'hde':
    print('Prepare store dim for HDE')
    store_dim = sqlContext.table('tdm.v_store_dim').where(F.col('format_id').isin(1,2,3))\
                                                   .select( F.col('store_id')
                                                           ,F.col('format_id')
                                                           ,F.col('date_opened')
                                                           ,F.col('date_closed')
                                                           ,lower(F.col('region')).alias('store_region')
                                                           ,lower(F.col('region')).alias('store_region_orig'))

elif store_fmt == 'gofresh' :
    print('Prepare store dim for Go Fresh')
    store_dim = sqlContext.table('tdm.v_store_dim').where((F.col('format_id').isin(5)) &
                                                          (F.col('date_closed').isNull()) &
                                                          (~(F.col('store_id').isin(9884, 3068)))  ## Dark Store and DC store
                                                         )\
                                                   .select( F.col('store_id')
                                                           ,F.col('format_id')
                                                           ,F.col('date_opened')
                                                           ,F.col('date_closed')
                                                           ,F.when(lower(F.col('region')) == 'west', lit('central'))
                                                             .otherwise(lower(F.col('region')))
                                                             .alias('store_region')
                                                           ,lower(F.col('region')).alias('store_region_orig'))

else :
    print('Store Format is not correct code will skip evaluation for campaign ' + str(cmp_nm) + ' !!\n')
    raise Exception("Incorrect store format value !!")
## end if

## Import target file
in_trg_df = spark.read.csv(in_trg_file, header="true", inferSchema="true")

## end if

## Import control file - if full evaluation
if (eval_type == 'full') & (hv_ctrl_store == 'true'):
    ## use self control
    in_ctl_str_df = spark.read.csv(in_ctl_file, header="true", inferSchema="true")

elif (eval_type == 'full') & (flg_use_rsv) :
    ## use reserved will need to filter category reserved
    all_rsv_df    = spark.read.csv(in_ctl_file, header="true", inferSchema="true")
    in_ctl_str_df = all_rsv_df.where((all_rsv_df.class_code == resrv_store_class.upper()) &
                                  (all_rsv_df.rs_flag == 'reserved')
                                 )\
                           .select(all_rsv_df.store_id)
elif (eval_type == 'full') & (flg_use_oth) :
    in_ctl_str_df = store_dim.join  (in_trg_df, [store_dim.store_id == in_trg_df.store_id], 'left_anti')\
                             .select(store_dim.store_id)
## end if


## get region for target & control store

trg_str_df = in_trg_df.join  ( store_dim, [in_trg_df.store_id == store_dim.store_id], 'inner')\
                      .select( in_trg_df.store_id
                              ,store_dim.store_region_orig
                              ,store_dim.store_region
                              ,in_trg_df.c_start
                              ,in_trg_df.c_end
                              ,in_trg_df.mech_count
                              ,in_trg_df.mech_name)

if (eval_type == 'full'):
    u_ctl_str_df = in_ctl_str_df.join ( store_dim, [in_ctl_str_df.store_id == store_dim.store_id], 'inner')\
                                .select( in_ctl_str_df.store_id
                                        ,store_dim.store_region_orig
                                        ,store_dim.store_region)
    print('\n Check control store \n')
    u_ctl_str_df.limit(10).display()

## end if

#dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0136_M02E.csv
#dbfs:/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0136_M02E.csv

# COMMAND ----------

## check control
#u_ctl_str_df.limit(10).display()

# COMMAND ----------

# MAGIC %md # Create transaction

# COMMAND ----------

#---- Get total data number of week, and each start - end period date
# tot_data_week_num, cmp_st_date, cmp_end_date, gap_st_date, gap_en_date, pre_st_date, pre_en_date, prior_st_date, prior_en_date = \
# get_total_data_num_week(c_st_date_txt=cmp_start, c_en_date_txt=cmp_end, gap_week_txt=gap_week)

#---- Try loding existing data table, unless create new
try:
    # Test block
    # raise Exception('To skip try block')
    txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
    print(f'Load data table for period : Ppp - Pre - Gap - Cmp, All store All format \n from : tdm_seg.media_campaign_eval_txn_data_{cmp_id}')

except:

    print(f'Create intermediate transaction table for period Prior - Pre - Dur (Promo Week), all store format : tdm_seg.media_campaign_eval_txn_data_{cmp_id}')

    txn_all = get_trans_itm_wkly_promo(start_week_id=ppp_st_promo_wk, end_week_id=cmp_en_promo_wk, store_format=[1,2,3,4,5],
                                       prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name',
                                                        'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                                        'department_code', 'section_code', 'class_code', 'subclass_code'])
    #---- Add period column
    if gap_flag:
        print('Data with gap week')
        txn_all = (txn_all.withColumn('period_fis_wk',
                                      F.when(F.col('week_id').between(cmp_st_wk, cmp_en_wk), F.lit('cmp'))
                                       .when(F.col('week_id').between(gap_st_wk, gap_en_wk), F.lit('gap'))
                                       .when(F.col('week_id').between(pre_st_wk, pre_en_wk), F.lit('pre'))
                                       .when(F.col('week_id').between(ppp_st_wk, ppp_en_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                          .withColumn('period_promo_wk',
                                      F.when(F.col('promoweek_id').between(cmp_st_promo_wk, cmp_en_promo_wk), F.lit('cmp'))
                                       .when(F.col('promoweek_id').between(gap_st_promo_wk, gap_en_promo_wk), F.lit('gap'))
                                       .when(F.col('promoweek_id').between(pre_st_promo_wk, pre_en_promo_wk), F.lit('pre'))
                                       .when(F.col('promoweek_id').between(ppp_st_promo_wk, ppp_en_promo_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                  )
    else:
        txn_all = (txn_all.withColumn('period_fis_wk',
                                      F.when(F.col('week_id').between(cmp_st_wk, cmp_en_wk), F.lit('cmp'))
                                       .when(F.col('week_id').between(pre_st_wk, pre_en_wk), F.lit('pre'))
                                       .when(F.col('week_id').between(ppp_st_wk, ppp_en_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                          .withColumn('period_promo_wk',
                                      F.when(F.col('promoweek_id').between(cmp_st_promo_wk, cmp_en_promo_wk), F.lit('cmp'))
                                       .when(F.col('promoweek_id').between(pre_st_promo_wk, pre_en_promo_wk), F.lit('pre'))
                                       .when(F.col('promoweek_id').between(ppp_st_promo_wk, ppp_en_promo_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                  )

    txn_all.write.saveAsTable(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
    ## Pat add, delete dataframe before re-read
    del txn_all
    ## Re-read from table
    txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')

# COMMAND ----------

txn_all.printSchema()

# COMMAND ----------

# MAGIC %md #Check region - test store, Combine 'store_region' if GoFresh

# COMMAND ----------

test_store_sf = spark.read.csv(os.path.join(input_path, target_file), header=True, inferSchema=True)
test_vs_all_store_count, txn_all = check_combine_region(store_format_group=store_fmt, test_store_sf=test_store_sf, txn = txn_all)
test_vs_all_store_count.display()
test_vs_all_store_count_df = to_pandas(test_vs_all_store_count)
pandas_to_csv_filestore(test_vs_all_store_count_df, f'test_vs_all_store_count.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Check feature SKU details
# MAGIC ##2. Create adjacency product group / adjacency upc_id

# COMMAND ----------

# # path of adjacency mapping file
# adjacency_file_path = os.path.join(stdin_path, adjacency_file)

# adj_prod_sf, adj_prod_group_name_sf, featues_product_and_exposure_sf, mfr_promoted_product_str = \
# get_adjacency_product_id(promoted_upc_id=feat_list , adjacecy_file_path=adjacency_file_path)

# # Save adjacency product
# adj_prod_df = to_pandas(adj_prod_sf)
# pandas_to_csv_filestore(adj_prod_df, 'adj_prod_id.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'output'))

# # Save adjacency group name
# adj_prod_group_name_df = to_pandas(adj_prod_group_name_sf)
# pandas_to_csv_filestore(adj_prod_group_name_df, 'adj_group_name.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# # Detail of feature products + exposure
# featues_product_and_exposure_df = to_pandas(featues_product_and_exposure_sf)
# pandas_to_csv_filestore(featues_product_and_exposure_df, 'feature_product_and_exposure_details.csv',
#                         prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# path of adjacency mapping file
#adjacency_file_path = os.path.join(stdin_path, adjacency_file)

#adj_prod_sf, adj_prod_group_name_sf, featues_product_and_exposure_sf, mfr_promoted_product_str = \
#get_adjacency_product_id(promoted_upc_id=feat_list , adjacecy_file_path=adjacency_file_path)

# Save adjacency product
#adj_prod_sf = use_ai_df
#adj_prod_df = to_pandas(use_ai_df)
#pandas_to_csv_filestore(adj_prod_df, 'adj_prod_id.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'output'))

# Save adjacency group name
#adj_prod_group_name_df = to_pandas(adj_prod_group_name_sf)
#pandas_to_csv_filestore(adj_prod_group_name_df, 'adj_group_name.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# Detail of feature products + exposure
# featues_product_and_exposure_df = to_pandas(feat_detail)
# pandas_to_csv_filestore(featues_product_and_exposure_df, 'feature_product_and_exposure_details.csv',
#                         prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Report : Awareness

# COMMAND ----------

# cmp_st_date = datetime.strptime(cmp_start, '%Y-%m-%d')
# cmp_end_date = datetime.strptime(cmp_end, '%Y-%m-%d')
# exposure_all, exposure_region = get_awareness(txn_all, cp_start_date=cmp_st_date, cp_end_date=cmp_end_date,
#                                               store_fmt=store_fmt, test_store_sf=test_store_sf, adj_prod_sf=adj_prod_sf,
#                                               media_spend=float(media_fee))
# exposure_all_df = to_pandas(exposure_all)
# pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))
# exposure_region_df = to_pandas(exposure_region)
# pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# test_store_sf.printSchema()
# trg_str_df.printSchema()

# COMMAND ----------

cmp_st_date = datetime.strptime(cmp_start, '%Y-%m-%d')
cmp_end_date = datetime.strptime(cmp_end, '%Y-%m-%d')
# exposure_all, exposure_region = get_awareness_promo_wk(txn_all, cp_start_date=cmp_st_date, cp_end_date=cmp_end_date,
#                                               store_fmt=store_fmt, test_store_sf=test_store_sf,
#                                               media_spend=float(media_fee))
# exposure_all_df = to_pandas(exposure_all)
# pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))
# exposure_region_df = to_pandas(exposure_region)
# pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Report : Customer Movement & Switching

# COMMAND ----------

#---- Customer movement , New to sku for customer switching
cust_mv, new_sku, activated = cust_movement_promo_wk(switching_lv=cate_lvl,
                                            txn=txn_all,
                                            cp_start_date=cmp_st_date,
                                            cp_end_date=cmp_end_date,
                                            brand_df=brand_df,
                                            test_store_sf=test_store_sf,
                                            feat_list=feat_list
                                           )

# Save customer movement for input in customer switching
cust_mv.write.format('parquet').mode('overwrite').saveAsTable(f'tdm_seg.media_camp_eval_{cmp_id}_cust_mv')

# Save customer movement
cust_mv_count = cust_mv.groupBy('customer_macro_flag', 'customer_micro_flag').count().orderBy('customer_macro_flag', 'customer_micro_flag')
cust_mv_count_df = to_pandas(cust_mv_count)
pandas_to_csv_filestore(cust_mv_count_df, 'customer_movement.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))
pandas_to_csv_filestore(activated, 'customer_exposed_activate.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

#----- Customer brand switching & brand penetration
cust_mv = spark.table(f'tdm_seg.media_camp_eval_{cmp_id}_cust_mv')
cust_brand_switching, chk, cust_brand_penetration = cust_switching_promo_wk(switching_lv=cate_lvl,
                                                                   cust_movement_sf=cust_mv,
                                                                   txn=txn_all,
                                                                   cp_start_date=cmp_st_date,
                                                                   cp_end_date=cmp_end_date,
                                                                   feat_list=feat_list
                                                                  )

cust_brand_switching_df = to_pandas(cust_brand_switching)
pandas_to_csv_filestore(cust_brand_switching_df, 'customer_brand_switching.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

cust_brand_penetration_df = to_pandas(cust_brand_penetration)
pandas_to_csv_filestore(cust_brand_penetration_df, 'customer_brand_penetration.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# create combine brand switching and penetration
if cate_lvl == 'subclass':
    cust_brand_sw_pen_df = cust_brand_switching_df.merge(cust_brand_penetration_df, how='left', left_on='oth_brand_in_subclass', right_on='brand_name')
elif cate_lvl == 'class':
    cust_brand_sw_pen_df = cust_brand_switching_df.merge(cust_brand_penetration_df, how='left', left_on='oth_brand_in_class', right_on='brand_name')

pandas_to_csv_filestore(cust_brand_sw_pen_df, 'customer_brand_switching_penetration.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

#---- Customer SKU switching
sku_switcher = cust_sku_switching_promo_wk(switching_lv=cate_lvl,
                                  txn=txn_all,
                                  cp_start_date=cmp_st_date,
                                  cp_end_date=cmp_end_date,
                                  test_store_sf=test_store_sf,
                                  feat_list=feat_list
                                 )

cust_sku_switching_df = to_pandas(sku_switcher)
pandas_to_csv_filestore(cust_sku_switching_df, 'customer_sku_switching.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Break-point for Standard report (Exposure report only)

# COMMAND ----------

if eval_type == 'std':

    create_zip_from_dbsf_prefix_indir(cmp_out_path, f'{cmp_nm}_all_eval_result.zip')

    dbutils.notebook.exit('Finish Standard Evaluation for campaign ' + str(cmp_nm) + ', Exit status = 0 .')

# COMMAND ----------

# MAGIC %md #Full Evaluation

# COMMAND ----------

# MAGIC %md ##Store matching

# COMMAND ----------

## for testing only
# txn_all = sqlContext.table('tdm_seg.media_campaign_eval_txn_data_2022_0136_M02E')
# print(dbfs_project_path)

# COMMAND ----------

# txn_all.printSchema()

# COMMAND ----------

## call new matching auto select product level to do matching

ctr_store_list, store_matching_df = get_store_matching_promo_at( txn_all
                                                                ,pre_en_promowk = pre_en_promo_wk
                                                                ,brand_df = brand_df
                                                                ,sel_sku = feat_list
                                                                ,test_store_sf = trg_str_df
                                                                ,reserved_store_sf=u_ctl_str_df
                                                                ,matching_methodology='varience')

## Export to csv file
pandas_to_csv_filestore(store_matching_df, 'store_matching.csv', prefix= os.path.join(dbfs_project_path, 'output'))

#print('-'*80 + '\n Store Matching information Show below \n' + '-'*80)



# COMMAND ----------

# MAGIC %md ## Customer Share and KPI

# COMMAND ----------

combined_kpi, kpi_df, df_pv = cust_kpi_promo_wk(txn_all, store_fmt=store_fmt, test_store_sf=trg_str_df, ctr_store_list=ctr_store_list,feat_list=feat_list)

pandas_to_csv_filestore(kpi_df, 'kpi_test_ctrl_pre_dur.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))
pandas_to_csv_filestore(df_pv, 'customer_share_test_ctrl_pre_dur.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md ##Customer Uplift

# COMMAND ----------

## Cust Uplift at brand

uplift_brand = get_customer_uplift_promo_wk(txn_all,
                                            ctr_store_list=ctr_store_list,
                                            test_store_sf=test_store_sf,
                                            cust_uplift_lv='brand',
                                            feat_list =feat_list)

uplift_brand_df = to_pandas(uplift_brand)
pandas_to_csv_filestore(uplift_brand_df, 'customer_uplift_brand.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

## Cust Uplift at feature

uplift_feature = get_customer_uplift_promo_wk(txn_all,
                                              ctr_store_list=ctr_store_list,
                                              test_store_sf=test_store_sf,
                                              cust_uplift_lv='sku',
                                              feat_list =feat_list)

uplift_feature_df = to_pandas(uplift_feature)
pandas_to_csv_filestore(uplift_feature_df, 'customer_uplift_features_sku.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md ##CLTV

# COMMAND ----------

uplift_brand_df = pd.read_csv(os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result', 'customer_uplift_brand.csv'))
uplift_brand = spark.createDataFrame(uplift_brand_df)

## call get brand_cltv
brand_cltv, brand_svv = get_customer_cltv_promo_wk(txn_all,
                                                   test_store_sf=test_store_sf,
                                                   lv_cltv=cate_lvl,
                                                   uplift_brand=uplift_brand,
                                                   media_spend=float(media_fee),
                                                   feat_list=feat_list,
                                                   svv_table = svv_table,
                                                   pcyc_table = pcyc_table)


pandas_to_csv_filestore(brand_cltv, 'cltv.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))
pandas_to_csv_filestore(brand_svv, 'brand_survival_rate.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category average survival rate

# COMMAND ----------

svv_df       = sqlContext.table(svv_table)
cate_avg_svv = get_avg_cate_svv(svv_df, cate_lvl, cate_cd_list)

cate_avg_svv.display()

## export to csv to output path
cate_avg_svv_pd = cate_avg_svv.toPandas()
outfile         = cmp_out_path + 'result/' + 'cate_avg_svv.csv'
cate_avg_svv_pd.to_csv(outfile, index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uplift by region

# COMMAND ----------

# #del txn_all

# txn_all = sqlContext.table('tdm_seg.media_campaign_eval_txn_data_2022_0136_M02E')
# matching_spkapi  = 'dbfs:/FileStore/media/promozone_eval/2022_0136_M02E_HYGIENE/output/store_matching.csv'
# matching_fileapi =  '/dbfs/FileStore/media/promozone_eval/2022_0136_M02E_HYGIENE/output/store_matching.csv'


# COMMAND ----------

# ## check schema txn
# txn_all.printSchema()
# txn_all.display(10)

# COMMAND ----------

# store_matching_df = pd.read_csv(matching_fileapi)
# store_matching_df.display()

# COMMAND ----------

# MAGIC %md ###Recall util2 for test will be removed

# COMMAND ----------

#%run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

# MAGIC %md ###uplift fisweek SKU

# COMMAND ----------

# # ## call sale uplift by region -- Pat 25 May 2022

# ## SKU Level
# sku_sales_matching_df, sku_uplift_table, sku_uplift_wk_graph, kpi_table, uplift_reg_pd = sales_uplift_reg( txn_all
#                                                                                                          ,sales_uplift_lv='sku'
#                                                                                                          ,brand_df = brand_df
#                                                                                                          ,feat_list = feat_list
#                                                                                                          ,matching_df=store_matching_df
#                                                                                                          ,matching_methodology='varience')


# COMMAND ----------

# print('Display sku_sales_matching_df ')
# sku_sales_matching_df.display()

# COMMAND ----------

# ## Convert uplift df from row to columns and add period identifier

# sku_wk_g = sku_uplift_wk_graph.reset_index()
# sku_wk_g.rename(columns = {'index' : 'week'}, inplace = True)
# #sku_wk_g.display()

# sku_wk_t              = sku_wk_g.T.reset_index()
# hdr                   = sku_wk_t.iloc[0]  ## get header from first row
# sku_wk_uplift         = sku_wk_t[1:]      ## get data start from row 1 (row 0 is header)
# sku_wk_uplift.columns = hdr         ## set header to df

# #sku_wk_uplift.display()

# #sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
# sku_wk_uplift['wk_period'] = np.where(sku_wk_uplift.loc[:, ('week')].astype(int) < chk_pre_wk, 'pre', 'dur')   ## change to use chk_pre_week instead of campaign start week

# print('\n' + '-'*80)
# print(' Display sku_wk_uplift for Trend chart : column mode ')
# print('-'*80)
# sku_wk_uplift.display()

# ## KPI table transpose
# kpi_fiswk_t = kpi_table.T.reset_index()
# kpi_fiswk_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
# print('-'*80)
# print(' Display kpi_promo_table : column mode ')
# print('-'*80)
# kpi_fiswk_t.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Uplift promo SKU

# COMMAND ----------

# ## call sale uplift by region -- Pat 25 May 2022

## SKU Level
sku_sales_matching_promo_df, sku_uplift_promo_table, sku_uplift_promowk_graph, kpi_table_promo, uplift_promo_reg_pd = sales_uplift_promo_reg( txn_all
                                                                                                                                             ,sales_uplift_lv='sku'
                                                                                                                                             ,brand_df = brand_df
                                                                                                                                             ,feat_list = feat_list
                                                                                                                                             ,matching_df=store_matching_df
                                                                                                                                             ,matching_methodology='varience')


# COMMAND ----------

##---------------------------
## Trend chart by promo week

sku_promowk_g = sku_uplift_promowk_graph.reset_index()
sku_promowk_g.rename(columns = {'index' : 'promo_week'}, inplace = True)
#sku_promowk_g.display()

sku_promowk_t              = sku_promowk_g.T.reset_index()
hdr                        = sku_promowk_t.iloc[0]  ## get header from first row
sku_promowk_uplift         = sku_promowk_t[1:]      ## get data start from row 1 (row 0 is header)
sku_promowk_uplift.columns = hdr                    ## set header to df

#sku_wk_uplift.display()

#sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
sku_promowk_uplift['wk_period'] = np.where(sku_promowk_uplift.loc[:, ('promo_week')].astype(int) < chk_pre_wk, 'pre', 'dur')  ## change to use chk_pre_week instead of campaign start week

print('\n' + '-'*80)
print(' Display sku_wk_uplift for Trend chart : column mode ')
print('-'*80)
sku_promowk_uplift.display()

## KPI table transpose
kpi_promo_t = kpi_table_promo.T.reset_index()
kpi_promo_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
print('-'*80)
print(' Display kpi_promo_table : column mode ')
print('-'*80)
kpi_promo_t.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Export file - uplift at level feature SKU

# COMMAND ----------

#pandas_to_csv_filestore(sku_uplift_table, 'sales_sku_uplift_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))

#sku_uplift_promo_table
pandas_to_csv_filestore(sku_uplift_promo_table, 'sales_sku_uplift_promo_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))

#pandas_to_csv_filestore(sku_uplift_wk_graph.reset_index(), 'sales_sku_uplift_wk_graph.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat change to column mode Dataframe for weekly trend
#pandas_to_csv_filestore(sku_wk_uplift, 'sales_sku_uplift_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sku_promowk_uplift, 'sales_sku_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## uplift region
#pandas_to_csv_filestore(uplift_reg_pd, 'sales_sku_uplift_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(uplift_promo_reg_pd, 'sales_sku_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat add KPI table -- 8 May 2022

#pandas_to_csv_filestore(kpi_fiswk_t, 'sale_kpi_target_control_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(kpi_promo_t, 'sale_kpi_target_control_promo_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign

#pandas_to_csv_filestore(sku_sales_matching_df, 'sku_sales_matching_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))
pandas_to_csv_filestore(sku_sales_matching_promo_df, 'sku_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))


# COMMAND ----------

# MAGIC %md ###uplift fisweek Brand

# COMMAND ----------

#brand_df.printSchema()

# COMMAND ----------

# # ## call sale uplift by region -- Pat 25 May 2022

# ## Brand Level

# bnd_sales_matching_df, bnd_uplift_table, bnd_uplift_wk_graph, kpi_table_bnd, uplift_reg_bnd_pd = sales_uplift_reg( txn_all
#                                                                                                                   ,sales_uplift_lv='brand'
#                                                                                                                   ,brand_df = brand_df
#                                                                                                                   ,feat_list = feat_list
#                                                                                                                   ,matching_df=store_matching_df
#                                                                                                                   ,matching_methodology='varience')



# COMMAND ----------

# ## Convert uplift df from row to columns and add period identifier

# bnd_wk_g = bnd_uplift_wk_graph.reset_index()
# bnd_wk_g.rename(columns = {'index' : 'week'}, inplace = True)
# #sku_wk_g.display()

# bnd_wk_t              = bnd_wk_g.T.reset_index()
# hdr                   = bnd_wk_t.iloc[0]  ## get header from first row
# bnd_wk_uplift         = bnd_wk_t[1:]      ## get data start from row 1 (row 0 is header)
# bnd_wk_uplift.columns = hdr         ## set header to df


# #sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
# bnd_wk_uplift['wk_period'] = np.where(bnd_wk_uplift.loc[:, ('week')].astype(int) < chk_pre_wk, 'pre', 'dur')

# print('\n' + '-'*80)
# print(' Display brand_wk_uplift for Trend chart : column mode ')
# print('-'*80)
# bnd_wk_uplift.display()

# ## KPI table transpose
# kpi_fiswk_bnd_t = kpi_table_bnd.T.reset_index()
# kpi_fiswk_bnd_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
# print('-'*80)
# print(' Display kpi_promo_table : column mode ')
# print('-'*80)
# kpi_fiswk_bnd_t.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Uplift Promo Brand

# COMMAND ----------

# ## call sale uplift promo week by region -- Pat 31 May 2022
## promo will not have KPI

# ## call sale uplift by region -- Pat 25 May 2022

## Brand Level
bnd_sales_matching_promo_df, bnd_uplift_promo_table, bnd_uplift_promowk_graph, kpi_table_promo_bnd, uplift_promo_reg_bnd_pd = sales_uplift_promo_reg( txn_all
                                                                                                                                                 ,sales_uplift_lv='brand'
                                                                                                                                                 ,brand_df = brand_df
                                                                                                                                                 ,feat_list = feat_list
                                                                                                                                                 ,matching_df=store_matching_df
                                                                                                                                                 ,matching_methodology='varience')


# COMMAND ----------

##---------------------------
## promo wk - brand Transpose
##---------------------------

bnd_promowk_g = bnd_uplift_promowk_graph.reset_index()
bnd_promowk_g.rename(columns = {'index' : 'promo_week'}, inplace = True)
#bnd_promowk_g.display()

bnd_promowk_t              = bnd_promowk_g.T.reset_index()
hdr                        = bnd_promowk_t.iloc[0]  ## get header from first row
bnd_promowk_uplift         = bnd_promowk_t[1:]      ## get data start from row 1 (row 0 is header)
bnd_promowk_uplift.columns = hdr                    ## set header to df

bnd_promowk_uplift['wk_period'] = np.where(bnd_promowk_uplift.loc[:, ('promo_week')].astype(int) < chk_pre_wk, 'pre', 'dur')  ## change to use chk_pre_week instead of campaign start week

print('\n' + '-'*80)
print(' Display brand_promowk_uplift for Trend chart : column mode ')
print('-'*80)
bnd_promowk_uplift.display()

## KPI table transpose
kpi_promowk_bnd_t = kpi_table_promo_bnd.T.reset_index()
kpi_promowk_bnd_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
print('-'*80)
print(' Display kpi_promo_table : column mode ')
print('-'*80)
kpi_promowk_bnd_t.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Export file - uplift at level Brand

# COMMAND ----------

#pandas_to_csv_filestore(bnd_uplift_table, 'sales_brand_uplift_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(bnd_uplift_promo_table, 'sales_brand_uplift_promo_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))

#pandas_to_csv_filestore(sku_uplift_wk_graph.reset_index(), 'sales_sku_uplift_wk_graph.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat change to column mode Dataframe for weekly trend
#pandas_to_csv_filestore(bnd_wk_uplift, 'sales_brand_uplift_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(bnd_promowk_uplift, 'sales_brand_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## uplift region
#pandas_to_csv_filestore(uplift_reg_bnd_pd, 'sales_brand_uplift_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(uplift_promo_reg_bnd_pd, 'sales_brand_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat add KPI table -- 8 May 2022

#pandas_to_csv_filestore(kpi_fiswk_bnd_t, 'sale_kpi_target_control_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(kpi_promowk_bnd_t, 'sale_kpi_target_control_promo_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign

#pandas_to_csv_filestore(bnd_sales_matching_df, 'brand_sales_matching_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))
pandas_to_csv_filestore(bnd_sales_matching_promo_df, 'brand_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))


# COMMAND ----------

class_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Growth fis week

# COMMAND ----------

# ## Code
# ## Change function, and call function 4 times manually, may excluded some line later

# ## def get_sales_mkt_growth( txn
# ##                          ,prod_df
# ##                          ,cate_df
# ##                          ,prod_level
# ##                          ,cate_level
# ##                          ,week_type
# ##                          ,store_format
# ##                          ,store_matching_df
# ##                         ):
# ## convert matching pandas dataframe to spark for this function

# store_matching_spk = spark.createDataFrame(store_matching_df)

# sales_brand_class_fiswk = get_sales_mkt_growth( txn_all
#                                                   ,brand_df
#                                                   ,class_df
#                                                   ,'brand'
#                                                   ,'class'
#                                                   ,'fis_wk'
#                                                   ,store_fmt
#                                                   ,store_matching_spk
#                                                  )

# sales_brand_subclass_fiswk = get_sales_mkt_growth( txn_all
#                                                     ,brand_df
#                                                     ,class_df
#                                                     ,'brand'
#                                                     ,'subclass'
#                                                     ,'fis_wk'
#                                                     ,store_fmt
#                                                     ,store_matching_spk
#                                                  )

# sales_sku_class_fiswk = get_sales_mkt_growth( txn_all
#                                                ,feat_df
#                                                ,class_df
#                                                ,'sku'
#                                                ,'subclass'
#                                                ,'fis_wk'
#                                                ,store_fmt
#                                                ,store_matching_spk
#                                               )

# sales_sku_subclass_fiswk = get_sales_mkt_growth( txn_all
#                                                   ,feat_df
#                                                   ,sclass_df
#                                                   ,'sku'
#                                                   ,'subclass'
#                                                   ,'fis_wk'
#                                                   ,store_fmt
#                                                   ,store_matching_spk
#                                                  )

# #sales_brand_class_fiswk.display()

# ## Export File

# pandas_to_csv_filestore(sales_brand_class_fiswk, 'sales_brand_class_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
# pandas_to_csv_filestore(sales_brand_subclass_fiswk, 'sales_brand_subclass_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
# pandas_to_csv_filestore(sales_sku_class_fiswk, 'sales_sku_class_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
# pandas_to_csv_filestore(sales_sku_subclass_fiswk, 'sales_sku_subclass_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Growth promo week

# COMMAND ----------

## Code
## Change function, and call function 4 times manually, may excluded some line later

## def get_sales_mkt_growth( txn
##                          ,prod_df
##                          ,cate_df
##                          ,prod_level
##                          ,cate_level
##                          ,week_type
##                          ,store_format
##                          ,store_matching_df
##                         ):
## convert matching pandas dataframe to spark for this function

store_matching_spk = spark.createDataFrame(store_matching_df)

sales_brand_class_promowk = get_sales_mkt_growth( txn_all
                                                  ,brand_df
                                                  ,class_df
                                                  ,'brand'
                                                  ,'class'
                                                  ,'promo_wk'
                                                  ,store_fmt
                                                  ,store_matching_spk
                                                 )

sales_brand_subclass_promowk = get_sales_mkt_growth( txn_all
                                                    ,brand_df
                                                    ,class_df
                                                    ,'brand'
                                                    ,'subclass'
                                                    ,'promo_wk'
                                                    ,store_fmt
                                                    ,store_matching_spk
                                                 )

sales_sku_class_promowk = get_sales_mkt_growth( txn_all
                                               ,feat_df
                                               ,class_df
                                               ,'sku'
                                               ,'subclass'
                                               ,'promo_wk'
                                               ,store_fmt
                                               ,store_matching_spk
                                              )

sales_sku_subclass_promowk = get_sales_mkt_growth( txn_all
                                                  ,feat_df
                                                  ,sclass_df
                                                  ,'sku'
                                                  ,'subclass'
                                                  ,'promo_wk'
                                                  ,store_fmt
                                                  ,store_matching_spk
                                                 )

#sales_brand_class_promowk.display()

## Export File

pandas_to_csv_filestore(sales_brand_class_promowk, 'sales_brand_class_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_brand_subclass_promowk, 'sales_brand_subclass_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_sku_class_promowk, 'sales_sku_class_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_sku_subclass_promowk, 'sales_sku_subclass_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Zip file for send out

# COMMAND ----------

create_zip_from_dbsf_prefix_indir(cmp_out_path, f'{cmp_nm}_all_eval_result.zip')

# COMMAND ----------


