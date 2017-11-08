# This is a code scrap from a project I worked on during my last internship.
# The function of this code is to collect and pre-process players' gameplay data from storage base.
# Part of this code has been deleted or modified for security consideration.

import pandas as pd
import numpy as np
import math
import sys, os, time
import requests
import json
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta

# Download data to disk for training
class CollectData:
	def __init__(self):
		# API
		self.user = 'xxxxx'
		self.token = 'xxxxx'
		self.outPath = os.path.realpath('/home/xxxxx/mab_results/')

	def collect_result(self, jobIDs , max_trial = 20, interval = 60):
		print "get results from base"
		try:
			base.wait_until_jobs_complete( self.user, self.token, jobIDs , max_trial, interval )
		except IOError:
			raise
		## Download results to disk
		for jobID in jobIDs:
			base.get_results(self.user, self.token, jobID, self.outPath)

	# For win ratios of seeds
	# dt0, dt1: all games between start and end dates (not limit on starting cohorts)
	# results save to outPath
	def collect_news_summary(self, dt, numDays, environment):
		print "collect_news_summary(): %s" % (dt)
		## Submit Query
		jobIDs = [] # the jobs to add partitions in Base
		queryName = 'news_summary_%s_%s_%s' % (dt.strftime('%Y-%m-%d'), str(unmDays), environment)
		query = sql.q_news_summary(condi_generator(dt, numDays), environment)
		print query
		jobID = base.submit_query(self.user, self.token, query, queryName)
		jobIDs.append(jobID)
		## Wait for those Query to be complete
		self.collect_result(jobIDs, max_trial = 200, interval = 60)


def condi_generator(dt, numDays):
	condi = '('
	for i in range(numdays - 1):
		condi += 'dt = ' + dt.strftime('%Y-%m-%d') + 'OR '
		dt -= timedelta(1)
	condi += 'dt = ' + dt.strftime('%Y-%m-%d') + ')'
	return condi


def q_news_summary(condi, environment):
	query = """
SELECT t.title_id AS title_id,
	   t.status AS status
	   t.dt
FROM (
  SELECT title_id,
			 SUM(cnt) AS total_cnt,
			 status,
			 dt
	  FROM (SELECT title_id,
				   GET_JSON_OBJECT(event_params,'$.count') AS cnt,
				   GET_JSON_OBJECT(event_params,'$.status') AS status
			FROM pin.r_events
			WHERE %s
			AND   GET_JSON_OBJECT(event_params,'$.environment') = '%s'
		   ) p
	  WHERE item_type IS NOT NULL
	  AND   item_name IS NOT NULL
	  GROUP BY title_id,
			   status,
			   dt) t

  """ % (condi, environment)
	return query


if __name__ == "__main__":
	if len(argv) < 3:
		print IOError('Number of arguments is wrong \n \
			\targv[1]: latestDate\n \
			\targv[2]: numDays\n')
		sys.exit(1)

	latestDate = datetime.strptime(argv[1], '%Y-%m-%d')
	numDays = int(argv[2])
	cd = CollectData()
	cd.collect_news_summary(latestDate, numDays, 'PRODUCTION'):
