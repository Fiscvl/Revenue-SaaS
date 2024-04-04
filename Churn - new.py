
import datetime
from msilib.schema import Class
from dateutil.relativedelta import *
#from utilities import *
import warnings

import pandas as pd
from Constants import *
from Formats import *
from Collections import *

class CChurn():

    #  Product:
    #  invoice_number:
    
    def __init__(self, inputs, products):

        pass
        # create a churn list for all client/products, start/end, mrr to analyze - each row is an invoice
        # create a churn count for all client/products: churned, restart

        #Open Existing Invoices tab as Dataframe

        # Filter out unneeded invoices: Ends before start of churn calc, Start after close date, mrr <= 0

        # Sort by client, then product, then end date (ascending)

        self.productIndexs = []

        #try:
        excel_book = (inputs.full_path_input + kRev_input_file)
        df = pd.read_excel(excel_book, kExisting)

        indexAmount = df[ (df['Amount'] <= 0) ].index
        df.drop(indexAmount , inplace=True)

        months_lookback = inputs.ChurnMonthsLookback
        months_lookback_delta = relativedelta(months=-months_lookback, days=1)
        churn_start_date = inputs.projections_date + months_lookback_delta
        #print("Start date type : ", type(inputs.start_date))
        indexStart = df[df['Recognition End'] < churn_start_date].index
        df.drop(indexStart , inplace=True)
        indexEnd = df[df['Recognition Start'] > inputs.projections_date].index
        df.drop(indexEnd , inplace=True)

        print("Start: ", churn_start_date)
        print("Projections: ", inputs.projections_date)
        
        df = df.drop(kExistingType, axis=1)
        df = df.drop(kExistingInvoiceDate, axis=1)
        df = df.drop(kExistingCollectionDate, axis=1)
        df = df.drop(kExistingAmount, axis=1)
        df = df.drop(kExistingContractMonths, axis=1)
        temp_date = datetime.min
         
        df.sort_values(by = [kExistingClientID, kExistingProduct, kExistingRecognitionEnd], ascending = [True, True, True], inplace = True)

        writer = pd.ExcelWriter(inputs.full_path_output + kChurn_file, engine = 'xlsxwriter')
        df.to_excel(writer, sheet_name = kChurnInvoices, index = False)

        #add empty date column for future use
        df.insert(kChurnOutMidPointIndex, kChurnOutMidPoint, temp_date)  
        
        self.invoices = []
        client_churn = []
        product_churn_list = []
        row = []
        same_invoice = False


        self.invoices = df.values.tolist()
        self.num_invoices = len(self.invoices) - 1
        invoice_number = 0
        churn_column_names = [kChurnOutClientID, kChurnOutProduct, kChurnOutStart, kChurnOutEnd,  kChurnOutMidPoint, kChurnOutMRR, kChurnOutCommission, kChurnOutInvoicesEnd, kChurnOutSameInvoice, kChurnOutEndInvoice, kChurnOutEndInvoiceChurn, kChurnOutProductChurn, kChurnOutOverallChurn]

        while invoice_number <= self.num_invoices:

            same_invoice = False        
            end_invoice = False
            end_invoice_churn = False
            product_churn = False
            overall_churn = False
            midpoint_date = datetime.min
            temp_list = []

            row = self.invoices[invoice_number]
            if invoice_number == self.num_invoices:
                #end of list
                row.append("End of list")

            elif invoice_number == 0:
                row.append("Begin of list")

            else:
                #not end of list
                row.append("Not end of list")
                

            temp_list = self.check_churn(invoice_number, inputs)

            index = 0
            for item in temp_list:
                if index == 0:
                    midpoint = temp_list[0]
                    row[kChurnOutMidPointIndex] = midpoint
                    index = 1
                else:
                    row.append(item)

            product_churn_list.append(row)   
            invoice_number += 1

        df = pd.DataFrame(product_churn_list, columns = churn_column_names)
        df.to_excel(writer, sheet_name = kChurnOutput, index = False)

        df_invoice_summary = df.copy(deep=True)
        df_invoice_summary.drop([kChurnOutStart, kChurnOutEnd, kChurnOutMRR, kChurnOutInvoicesEnd, kChurnOutSameInvoice, kChurnOutEndInvoice, kChurnOutEndInvoiceChurn, kChurnOutProductChurn], axis=1, inplace=True)

        df_invoice_summary = df_invoice_summary[(df_invoice_summary[kChurnOutMidPoint] >= churn_start_date) & (df_invoice_summary[kChurnOutMidPoint] <= inputs.dates.projections_date)]
        invoice_groupby = df_invoice_summary.groupby([kChurnOutProduct], as_index = False)[kChurnOutOverallChurn].count()
        invoice_groupby = invoice_groupby.rename({kChurnOutOverallChurn: kChurnOutInvoiceCount}, axis=1)
        
        df_churn_summary = df_invoice_summary.copy(deep=True)
        df_churn_summary = df_churn_summary[df_churn_summary[kChurnOutOverallChurn] == True]
        churn_groupby = df_churn_summary.groupby([kChurnOutProduct], as_index = False)[kChurnOutOverallChurn].count()

        churn_groupby[kChurnOutChurnPercent] = churn_groupby[kChurnOutOverallChurn]/invoice_groupby[kChurnOutInvoiceCount]/months_lookback*kMonthsInYear

        # loop thru each of the two dataframes (or convert to a list) and insert any missomg products with either
        # a count of zero or a churn of zero - all products must have an entry

        invoice_groupby.to_excel(writer, sheet_name = kChurnOccuranceSummary, index = False)
        churn_groupby.to_excel(writer, sheet_name = kChurnInvoiceSummary, index = False)

        self.churn_list = churn_groupby.values.tolist()
        self.churn_product_list(self.churn_list)
        
        writer.close()

    def churn_product_list(self, churn_list):

        #create index for later use
        print("Churn list: ", churn_list)
        for row in churn_list:
            productID = row[kProductChurnProductIndex]
            productID = productID.replace("'", "")
            self.productIndexs.append(productID)

    def check_churn(self, list_index, inputs):
        # assumes the index is not at the end of the list - or array error
        # check if the next invoice is the same client and product as the next one
        # and it's not more than two months between the invoices (considered a churn)
        months_between_renewals = 0
        same_invoice = False
        end_invoice = False
        end_invoice_churn = False
        product_churn = False
        overall_churn = False
        midpoint_date = self.invoices[list_index][kChurnStart] + (self.invoices[list_index][kChurnEnd] - self.invoices[list_index][kChurnStart]) / 2

        if list_index == 0: #first
            if (inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) - inputs.projections_start > 2):
                end_invoice_churn = True
            overall_churn = end_invoice_churn | product_churn
            temp_list = midpoint_date, same_invoice, end_invoice, end_invoice_churn, product_churn, overall_churn
            return temp_list

        if list_index == self.num_invoices:
            # last invoice in list
            end_invoice = True

            if (inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) - inputs.projections_start > 2):
                end_invoice_churn = True
            overall_churn = end_invoice_churn | product_churn
            temp_list = midpoint_date, same_invoice, end_invoice, end_invoice_churn, product_churn, overall_churn
            return temp_list

        if self.invoices[list_index][kChurnClientIDindex] == self.invoices[list_index+1][kChurnClientIDindex]:
            #same client
            if self.invoices[list_index][kChurnProductIndex] == self.invoices[list_index+1][kChurnProductIndex]:
                #same client and product
                #check month from current invoice to next invoice
                same_invoice = True
                if (inputs.dates.GetMonthNum(self.invoices[list_index+1][kChurnStart]) - inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) > 1): # replace with an input
                    #time between two renewals
                    inputs.dates.GetMonthNum(self.invoices[list_index+1][kChurnStart]) - inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) 
                    product_churn = True

                elif (inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) - inputs.projections_start > 2): # replace with an input
                        end_invoice_churn = True

                else:
                    pass
            else:
                # next invoice is different - products
                end_invoice = True
                if inputs.projections_start - inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) > 2: # replace with an input
                    end_invoice_churn = True
                
        else:
            # next invoice is different - clients
            #test if the current invoice end two or more months before the closing date
            end_invoice = True
            if inputs.projections_start - inputs.dates.GetMonthNum(self.invoices[list_index][kChurnEnd]) > 2: # replace with an input
                end_invoice_churn = True

        overall_churn = end_invoice_churn | product_churn
        temp_list = midpoint_date, same_invoice, end_invoice, end_invoice_churn, product_churn, overall_churn
        return temp_list

    def getClientChurn(self, client):

        pass
        #dummy choices
        client_churn = .5 
        client_peviously_churned = false

        return client_churn

    def getProductChurn(self, product):

        print(self.productIndexs)
        try:
            #product match
            product_index = self.productIndexs.index(product)
            churn = self.churn_list[product_index][kProductChurnPercentIndex]
            return churn

        except:
            #new client, or no data - use etimate from in
            print("Couldn't find the Product in Product list - oops", product)
            return kChurnZero