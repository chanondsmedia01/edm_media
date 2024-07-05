# Databricks notebook source
dbfs_prefix_to_zip = "/dbfs/FileStore/media/amp/2024/mini_foremost"

import os
import zipfile
import shutil
import pprint
from pathlib import Path

source_dbfs_prefix  = dbfs_prefix_to_zip

source_filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=source_dbfs_prefix , topdown=False) for name in files]
pprint.pprint(source_filenames)

# COMMAND ----------

tx = txnItem(end_wk_id=202225, range_n_week=1)

# COMMAND ----------

tx._LKP_LIFE_CYC_DESC.display()

# COMMAND ----------

tx._LKP_LIFE_CYC_LV2_DESC.display()

# COMMAND ----------

tx.txn.select("channel").drop_duplicates().display()

# COMMAND ----------

tx.txn.select("offline_online_other_channel").drop_duplicates().display()

# COMMAND ----------


