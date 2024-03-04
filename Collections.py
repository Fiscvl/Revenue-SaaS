from csv import reader
from datetime import *
from msilib.schema import Class
from dateutil.relativedelta import *
#from utilities import *
import warnings

import pandas as pd
import numpy as np
from Constants import *
from Formats import *


class CCollections():

	def __init__(self, existing_df, inputs):
	
		#copy the existing revenue datafile

		self.collectionsIndexs = []
		self.collectionsList = []

		#get collections data from the Existing invoices data
		df2 = existing_df[[kExistingClientID, kExistingInvoiceDate, kExistingCollectionDate, kExistingAmount]].copy(deep=True)
		df2.dropna(subset=[kExistingCollectionDate], inplace=True)
		#calculate days to collect and weights & sort
		df2[kCollectionsDays] = (df2[kExistingCollectionDate] - df2[kExistingInvoiceDate])
		df2[kCollectionsDays] = df2[kCollectionsDays] / np.timedelta64(1, 'D')
		df2[kCollectionsWeight] = df2[kCollectionsDays] * df2[kExistingAmount]
		df2 = df2.sort_values(kExistingClientID)

		#group the same clients and calculate weighted days overall
		df3 = df2.groupby([kExistingClientID], as_index=False)[[kCollectionsWeight, kExistingAmount]].sum()
		df3[kCollectionsWtdAvgDays] = df3[kCollectionsWeight] / df3[kExistingAmount]
		self.df4 = df3[ (df3[kCollectionsWtdAvgDays] > kCollectionDaysBad)]
		self.collectionsList = self.df4.values.tolist()

		#create index for later use
		for row in self.collectionsList:
			clientID = row[kCollectionsWtdAvgDaysClientIndex]
			clientID = clientID.replace("'", "")
			self.collectionsIndexs.append(clientID)

		#write collections output
		file_output = inputs.full_path_output + kCollections_file
		with pd.ExcelWriter(file_output) as writer:
			df2.to_excel(writer, sheet_name = kCollectionsTab)
			self.df4.to_excel(writer, sheet_name = kCollectionsWtgAvgDays)
			self.newclientdays = inputs.new_client_days

	def collectionsGetDOS(self, clientID):

		try:
			#have data
			client_index = self.collectionsIndexs.index(clientID)
			daystocollect = round(self.collectionsList[client_index][kCollectionsWtdAvgDaysIndex],0)

		except:
			#new client, or no data - use estimated days from inputs
			daystocollect = self.newclientdays

		return daystocollect