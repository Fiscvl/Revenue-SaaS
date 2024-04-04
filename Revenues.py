from datetime import *
from msilib.schema import Class
from dateutil.relativedelta import *
#from utilities import *
import warnings

import pandas as pd
from BaseProjections.Constants import *
from RevenueSaaS.Collections import *
from RevenueSaaS.Churn import *

#test/test

# Revenue Notes

#  3 types of Revenue, 3 seperate list/sheets for inputs of each type
#  Existing Revenues - list of existing invoices; with collection info
#  Renewal Revenues - renewals of existing contracts, with churn; list of contracts
#  New Revenues - new contracts; scale with employees, employee scale list

#  5 lists in each dictonary
#  1)   Revenues/MRR
#  2)   Invoices
#  3)   Deferred
#  4)   Accrued         - Not used for Renewal or New
#  5)   Collections

#  blank row is used for each input line, has 6 vectors for each list,
#  after the line is processed append each  list from blank_row to each of the 6 lists


warnings.filterwarnings("ignore")


class CRevenues():

    def __init__(self, inputs, ac_revenues, journal_entry, formats, products, rev_expense_log):
        self.existing = {}
        self.renew = {}
        self.new = {}
        self.current_row = {}
        self.blank_row = []
        existing_dict = {}
        renew_dict = {}
        new_dict = {}
        self.revenue_dict = {}      
        self.revenue_dict_to_TB = {}
        self.revenue_products = {}
        self.rev_explog = rev_expense_log
        self.transaction_log = []
        
        base_df = pd.DataFrame()
        self.ac_revenues = ac_revenues

        self.journal_entry = journal_entry
        self.products = products

        # Create a header list for the DataFrames
        sheet_header = [kRevLineNum, kRevClientId, kRevProduct, kRevInvoiceDate, kRevInvoiceAmt, kRevCollectDate,
                        kRevStartMRR, kRevEndMRR, kRevMRRAmt, kRevMRRTerm, kRevRenewNum, kRevCommission, kRevCommissionType]

        new_sheet_header = [kNewLineNum, kNewClientID, kNewProduct, kNewInvoiceDate, kNewInvoiceAmt, kNewCollectDate,
                        kNewStartMRR, kNewEndMRR, kNewMRRAmt, kNewMRRTerm, kNewRenewNum, kRevCommission, kRevCommissionType]
    
        # Add Month's to it - this is the monthly data
        sheet_header.extend(inputs.months_header)
        new_sheet_header.extend(inputs.months_header)
                            
        base_df =  pd.DataFrame(columns = sheet_header)
        new_base_df = pd.DataFrame(columns = new_sheet_header)
        zero_row = inputs.zero_row
        blank_row = [None]*kRevColumns
        blank_new_row = [None]*kNewColumns
        
        self.blank_row = list(0 for i in range(inputs.months_total))

        try:
            excel_book = (inputs.full_path_input + kRev_input_file)
            existing_df = pd.read_excel(excel_book, kExisting)
            renew_df = pd.read_excel(excel_book, kContracts)
            new_df = pd.read_excel(excel_book, kNew)
            
        except:
            print("Can't open the revenue Excel file")

        try:
            excel_book = (inputs.full_path_input + kInputs_file)
            commissions_df = pd.read_excel(excel_book, kCommissionsTab)
            self.commissions_dict = self.setup_commissions(commissions_df, inputs)

        except:
            print("Can't open the Commisions Sheet")

        #create Churn instrance based on historical 
        self.churn = CChurn(inputs, products)

        #create Collections instance based on historical client payments
        self.collections = CCollections(existing_df, inputs)

        #run the 3 types of Revenue        
        self.existing = self.init_existing(existing_df, inputs, base_df,  blank_row, zero_row, existing_dict)
        self.revenue_dict[kExisting] = self.existing

        self.renew = self.init_renew(renew_df, inputs, base_df,  blank_row, zero_row, renew_dict)
        self.revenue_dict[kRenew] = self.renew

        self.new = self.init_new(new_df, inputs, new_base_df,  blank_new_row, zero_row, new_dict)
        self.revenue_dict[kNew] = self.new

        #write to Excel files
        self.revenue_dict_to_TB = self.write_dict(self.revenue_dict, sheet_header, self.revenue_dict_to_TB, inputs)

    def CRevenuesAddMonthsTransactions(self, month, TB, inputs):

        for key, rev_list in self.revenue_dict_to_TB.items():

            for row in rev_list:
                rev_type = row.pop(kFirst)
                amount = round(row[month],2)
                TB = self.PrepJEforTB(rev_type, row, month, TB, inputs, amount, key)
                row.insert(kFirst, rev_type)
                
        return TB
    
    def init_existing(self, existing_df, inputs, base_df,  data_row, zero_row, existing_dict):
        #loop thru looking for current invoices
        invoices_active = 0
        projections_month = inputs.projections_start

        #setup the empty Lists
        existing_dict[kRevenue] = []   #base_df.copy()
        existing_dict[kInvoices] = []  #base_df.copy()
        existing_dict[kDeferred] = []  #base_df.copy()
        existing_dict[kAccrual] = []  #base_df.copy()
        existing_dict[kCollections] = []  #base_df.copy()
        existing_dict[kCommissions] = []  

        #this should iterate thru a list rather than a dataframe
        for i, row in existing_df.iterrows():
             
            inv_num = 0
            invoice_month = inputs.dates.GetMonthNum(row[kExistingInvoiceDate])
            end_month = inputs.dates.GetMonthNum(row[kExistingRecognitionEnd])
            collection_date = row[kExistingCollectionDate]
            mrr = row[kExistingMRR]
            projections_month = inputs.projections_start

            if ((invoice_month >= projections_month) or (end_month >= projections_month) or (collection_date is pd.NaT))  and (mrr != 0): #  

                data_row[kRevLineNumIndex] = i        #kLineNum      
                data_row[kRevClientIdIndex] = row[kExistingClientID]               
                data_row[kRevProductIndex] = row[kExistingProduct]    
                data_row[kRevInvoiceDateIndex] = row[kExistingInvoiceDate]
                data_row[kRevInvoiceAmtIndex] = row[kExistingAmount]
                data_row[kRevCollectDateIndex] = row[kExistingCollectionDate]
                data_row[kRevStartMRRIndex] = row[kExistingRecognitionStart]
                data_row[kRevEndMRRIndex] = row[kExistingRecognitionEnd]
                data_row[kRevMRRAmtIndex] = row[kExistingMRR]
                data_row[kRevMRRTermIndex] = row[kExistingContractMonths]
                data_row[kRevRenewNumIndex] = 0       #kRevRenewNum
                
                invoice_date = row[kExistingInvoiceDate]
                end_date = row[kExistingRecognitionEnd]

#Here's the commission section - Existing

                if pd.isnull(row[kExistingCommission]):
                    commission = self.commissions_dict[kExisting][kCommRenewIndex]
                else:
                    commission = row[kExistingCommission]

                data_row[kRevCommissionIndex] = commission
                data_row[kRevCommissionTypeIndex] =  self.commissions_dict[kExisting][kCommCommTypeIndex]

                existing_dict, row = self.process_invoice(data_row, zero_row, inputs, existing_dict)
                invoices_active += 1 

        return existing_dict

    def init_renew(self, renew_df, inputs, base_df,  data_row, zero_row, renew_dict):
        
        #loop thru looking for current invoices
        contracts_active = 0
        projections_date = inputs.projections_date
        projections_month = inputs.projections_start
        close_date = inputs.projections_date

        #setup the empty DataFrames
        renew_dict[kRevenue] = []       #base_df.copy()
        renew_dict[kInvoices] = []      #base_df.copy()
        renew_dict[kDeferred] = []      #base_df.copy()
        renew_dict[kAccrual] = []       #base_df.copy()
        renew_dict[kCollections] = []   #base_df.copy()
        renew_dict[kCommissions] = [] 

        contract_info = [""] * 3

        #this should iterate thru a list rather than a dataframe
        for i, row in renew_df.iterrows():

            start_date = row[kContractsStartDate]
            end_date = row[kContractsEndDate]
            contract_type = row[kContractsType]
            renewal = row[kContractsRenewalInfo]
            mrr = round(row[kContractsMRR],2)
            contract_amount = round(row[kContractsTotalContract],2)
            product = row[kContractsProduct]
            frequency = row[kContractsFrequency]
            clientID = row[kContractsAcctID]
            collection_date = row[kContractsCollectionDate]
            invoice_date = row[kContractsInvoiceDate]
            commission = row[kContractsCommission]

            if mrr != 0:
                #add a term and eliminate this
                term = round(contract_amount/mrr,0)

            else:
                term = 0
            
            if (end_date <= close_date) or term == 0 or frequency != "Recurring":
                #non active or passed contracts
                good_contract = False

            else:
                #contracts to 
                good_contract = True
                contracts_active += 1
                data_row[kRevLineNumIndex] = i   
                data_row[kRevClientIdIndex] = clientID  
                data_row[kRevProductIndex] = product             
                data_row[kRevInvoiceAmtIndex] = contract_amount
                data_row[kRevStartMRRIndex] = row[kContractsStartDate]
                data_row[kRevEndMRRIndex] = row[kContractsEndDate]
                data_row[kRevMRRAmtIndex] = row[kContractsMRR]
                data_row[kRevMRRTermIndex] = term
                data_row[kRevRenewNumIndex] = 0 
                data_row[kRevCollectDateIndex] = collection_date
                data_row[kRevInvoiceDateIndex] = invoice_date
                data_row[kRevCommissionIndex] = commission
                
                #These fields are only for Contract Renewals, so they go into a seperate list
                contract_info[kContractInfoFrequency] = row[kContractsFrequency]      #[0]
                contract_info[kContractInfoRenewalInfo] = row[kContractsRenewalInfo]  #[1]
                contract_info[kContractInfoFrequency] = row[kContractsFrequency]                #[2]

                renew_dict = self.prepare_renewal_invoices(data_row, zero_row, inputs, renew_dict, contract_info)
                
        return renew_dict

    def init_new(self, new_df, inputs, base_df,  data_row, zero_row, new_dict):
        
        projections_date = inputs.projections_date
        projections_month = inputs.projections_start
        close_date = inputs.end_date
        
        #setup the empty DataFrames
        new_dict[kRevenue] = []       #base_df.copy()
        new_dict[kInvoices] = []      #base_df.copy()
        new_dict[kDeferred] = []      #base_df.copy()
        new_dict[kAccrual] = []       #base_df.copy()
        new_dict[kCollections] = []   #base_df.copy()
        new_dict[kCommissions] = []

        contract_info = []

        #this should iterate thru a list rather than a dataframe
        for i, row in new_df.iterrows():

            book_date = row[kNewBookDate]
            
            if book_date != pd.NaT: 
                start_date = book_date
                term = round(inputs.new_client_term,0)
                end_date = start_date + relativedelta(months = term -1)
                contract_amount = round(row[kNewTotals],2)
                mrr = round(contract_amount/term, 2)
                commission = row[kNewCommission]

                #this is where products need to be included

            else:

                #start date is 1/1/1900
                start_date = datetime.datetime(1, 1, 1)
                print(f"start_date: {start_date}")
            
            if start_date >= projections_date and start_date <= close_date:         #and mrr <= 0

                data_row[kNewStartMRRIndex] = start_date
                data_row[kNewEndMRRIndex] = end_date
                data_row[kNewInvoiceAmtIndex] = contract_amount
                data_row[kNewMRRAmtIndex] = mrr
                data_row[kNewMRRTermIndex] = term
                data_row[kNewRenewNumIndex] = 0
                data_row[kNewInvoiceDateIndex] = start_date
                data_row[kNewCommissionIndex] = commission
  
                #its a valid date, process the row

                new_dict = self.prepare_new_invoices(data_row, zero_row, inputs, new_dict)

            else:
                print("There's a date error in the data - book date", start_date)

        return new_dict

    def prepare_renewal_invoices(self, data_row, zero_row, inputs, dict, contract_info):

        #data about the invoice
        start_date = data_row[kRevStartMRRIndex]
        end_date = data_row[kRevEndMRRIndex]
        client = data_row[kRevClientIdIndex]
        product = data_row[kRevProductIndex]
        invoice_amt = data_row[kRevInvoiceAmtIndex]
        term = data_row[kRevMRRTermIndex]
        mrr = data_row[kRevMRRAmtIndex]
        invoice_date = data_row[kRevInvoiceDateIndex]
        collection_date = data_row[kRevCollectDateIndex]
        collection_percent_in = data_row[kRevCommissionIndex]
        
        #Data about the contract
        contract_frequency = contract_info[kContractInfoFrequency]
        contract_renew = contract_info[kContractInfoRenewalInfo]
        contract_frequency = contract_info[kContractInfoFrequency]
        
        skip_contract = False

        #replace assignments if this works
        if start_date.day != 1:
            start_date = start_date.replace(day=1)

        if contract_renew > 0:
            term = contract_renew
            invoice_amt = mrr * term
            skip_contract = False

        if contract_renew == pd.NaT or contract_renew == "" or pd.isnull(contract_renew):
            skip_contract = False

        else:
            if not(type(contract_renew) is int):
                # if the the number of months to change the contract term is not and int
                if type(contract_renew) is float:
                    #then is better be a float
                    contract_renew = int(contract_renew)
                    skip_contract = False
                else:
                    skip_contract = True
                    print("Contract renewal change term is invlaid (not a number)")

        if contract_renew == kSkipContract:
            skip_contract = True
            print("bogus skip contract")

        if contract_frequency != kContractRenewing:
            skip_contract = True
            print("bogus skip contract renewing")        
        
        if self.ContractRenewing(end_date, inputs):

            if mrr >= 0:
                term = round(invoice_amt/mrr,0)
            else:
                term = 0

            end_date = start_date + relativedelta(months = term, days = -1)

            #this needs to change to an inputs

            if end_date >= inputs.projections_date:
                renewal = 0
            else:
                renewal = 1

            if renewal == 0:
                if pd.isnull(invoice_date):   #pd.NaT: 
                    invoice_date = start_date
                    invoice_month = inputs.dates.GetMonthNum(invoice_date) 

                else:
                    invoice_date =  data_row[kRevInvoiceDateIndex]
                    invoice_month = inputs.dates.GetMonthNum(invoice_date)

                if not (pd.isnull(collection_date)):
                    collection_date =  data_row[kRevCollectDateIndex]

                else:
                    collection_date =  invoice_date + relativedelta(days = inputs.new_client_days)

            else:
                invoice_date = start_date
                collection_date = pd.NaT

            #Calculate New End date with updated term (if requireed)
            #Determine if the Invoice has ended - for all purposes: Revenue, Invoice & Deferred/Accrued
            
            #If the original contract end date isn't the end of the month, then there are too many months and
            #we calculate a start date one month too late - so adjust both date one month in advance

            contract_ended = self.GetContractTermEnded(end_date, inputs)          #check
            revenues_ended = self.GetTermEnded(start_date, term, invoice_month, inputs)  #check

            #First test if the Invoice is current/relevant
            if not(revenues_ended) and mrr >= 0  and not(skip_contract):

                while not(contract_ended):
                            
                    #Add Contract Line

                    churn = self.churn.getProductChurn(product)
                    invoice_churn = round((invoice_amt * (1 - churn ) ** renewal), 2)
                    mrr_churn = round((mrr * (1 - churn ) ** renewal), 2)
                    
                    data_row[kRevStartMRRIndex] = start_date
                    data_row[kRevEndMRRIndex] = end_date
                    data_row[kRevMRRAmtIndex] = mrr_churn
                    data_row[kRevInvoiceAmtIndex] = invoice_churn
                    data_row[kRevMRRTermIndex] = term
                    data_row[kRevRenewNumIndex] = renewal

#Here's the commission percentage section - Renew

                    if pd.isnull(collection_percent_in):
                        if renewal == kFirst:
                            commission = self.commissions_dict[kRenew][kCommInitialIndex]
                        else:
                            commission = self.commissions_dict[kRenew][kCommRenewIndex]
                    else:
                        commission = collection_percent_in

                    data_row[kRevCommissionIndex] = commission
                    data_row[kRevCommissionTypeIndex] = self.commissions_dict[kRenew][kCommCommTypeIndex]

                    if renewal == 0:
                        data_row[kRevInvoiceDateIndex] = invoice_date
                        data_row[kRevCollectDateIndex] = collection_date

                    else:
                        data_row[kRevInvoiceDateIndex] = start_date
                        data_row[kRevCollectDateIndex] = invoice_date + relativedelta(days = inputs.new_client_days) 
                    
                    dict, row = self.process_invoice(data_row, zero_row, inputs, dict)

                    #test if the contract has ended before the next renewal dates are generated
                    
                    contract_ended = self.GetContractTermEnded(end_date, inputs)
                    
                    #update dates
                    start_date = end_date + relativedelta(days = 1)
                    end_date = start_date + relativedelta(months = term, days = -1)
                    invoice_date = start_date
                    temp_date = end_date + relativedelta(days = 1)
                    renewal +=1

        return dict

    def prepare_new_invoices(self, data_row, zero_row, inputs, dict):

        #'Now do the 1st MRR Invoice           
        #'1st MRR Term is Not a Renewal, so both Start/End Dates are same
        revenues_ended = False
        term = data_row[kNewMRRTermIndex] 
        start_date = data_row[kNewStartMRRIndex]
        end_date = start_date + relativedelta(months = term, days = -1) 
        renewal_number = 0
        invoice_date = start_date
        collection_date = invoice_date + relativedelta(days = inputs.new_client_days)
        invoice_date = data_row[kNewInvoiceDateIndex]
        invoice_month = inputs.dates.GetMonthNum(invoice_date)      
        client_name = pd.Timestamp(start_date).strftime('%Y-%m-%d')
        collection_percent_in = data_row[kNewCommissionIndex]
        
        #'First test if the Invoice is current
        mrr = data_row[kNewMRRAmtIndex]
        invoice_amount = data_row[kNewInvoiceAmtIndex] 

        if (not(revenues_ended) and (mrr != 0)):  

            #here we need to loop and process invoices split by product

            revenues_ended = self.GetTermEnded(start_date, term, invoice_month, inputs)
            contract_ended = self.GetContractTermEnded(end_date, inputs)      

            #'now do renewals of the new invoices
            while not(contract_ended):                
                    
                #these are the recurring renewals, repeating until projections end
                #with an increasing churn for each renewal (1-c)**n 
                    
                contract_ended = self.GetContractTermEnded(end_date, inputs)

                #Update variables to data row
                data_row[kNewClientIDIndex] = client_name
                data_row[kNewStartMRRIndex] = start_date
                data_row[kNewEndMRRIndex] = end_date
                data_row[kNewInvoiceDateIndex] = start_date
                data_row[kNewCollectDateIndex] = collection_date
                data_row[kRevRenewNumIndex] = renewal_number

#Here's the commission percentage section - New

                if pd.isnull(collection_percent_in):

                    if renewal_number == kFirst:
                        commission = self.commissions_dict[kNew][kCommInitialIndex]
                    else:
                        commission = self.commissions_dict[kNew][kCommRenewIndex]
                else:
                    commission = collection_percent_in

                data_row[kRevCommissionIndex] = commission
                data_row[kRevCommissionTypeIndex] = self.commissions_dict[kNew][kCommCommTypeIndex]

                # for now set all of the product equal to "Product 1
                # but eventually the new rows for revenue need to be split up by the number of products
                # and then allocated by the distribution of new products amongst there respective %'s

                #'Add Invoice Line
                # here we need to loop and process invoices split by product for renewals

                for row in self.products.products_list:

                    product = row[kProductNameIndex]
                    product_churn = self.churn.getProductChurn(product)
                    total_after_churn = invoice_amount  * ((1 - product_churn) ** renewal_number)
                    mrr_after_churn = mrr * ((1 - product_churn) ** renewal_number)

                    data_row[kNewMRRAmtIndex] = round(mrr_after_churn * row[kProductPercentIndex] , 2)
                    data_row[kNewInvoiceAmtIndex] = round(total_after_churn * row[kProductPercentIndex] , 2)
                    data_row[kNewProductIndex] = product
                    
                    dict, row = self.process_invoice(data_row, zero_row, inputs, dict)

                contract_ended = self.GetContractTermEnded(end_date, inputs) 

                start_date = end_date + relativedelta(days = 1)
                end_date = start_date + relativedelta(months = term, days = -1)

                #contract_ended = self.GetContractTermEnded(end_date, inputs)  

                invoice_date = start_date
                collection_date = invoice_date + relativedelta(days = inputs.new_client_days)
                #print("end date: ", end_date) 
                renewal_number += 1

        return dict
            
    def process_invoice(self, data_row, zero_row, inputs, dict):

        revenue_row = data_row + zero_row.copy()
        invoice_row = data_row + zero_row.copy()
        deferred_row = data_row + zero_row.copy()
        accrued_row =  data_row + zero_row.copy()
        collection_row = data_row + zero_row.copy()
        commissions_row = data_row + zero_row.copy()        

        
        invoice_date = data_row[kRevInvoiceDateIndex]  
        invoice_month = inputs.dates.GetMonthNum(data_row[kRevInvoiceDateIndex])
        start_month  = inputs.dates.GetMonthNum(data_row[kRevStartMRRIndex])
        #end_month = inputs.dates.GetMonthNum(data_row[kRevEndMRRIndex])
        term = data_row[kRevMRRTermIndex]
        end_month = start_month + term - 1
        #print("months, start, end: ",start_month,  end_month)
        collection_date = data_row[kRevCollectDateIndex]
        clientID = data_row[kRevClientIdIndex]
        commission = data_row[kRevCommissionIndex]
        commission_type = data_row[kRevCommissionTypeIndex]

        # if empty then based on renewal number, get a percentage
    
        invoice_amount = round(data_row[kRevInvoiceAmtIndex],2)
        mrr = round(data_row[kRevMRRAmtIndex],2)
        
        proj_date = inputs.projections_date
        proj_month = inputs.projections_start
        proj_end = inputs.months_total

  
        #this needs to be thought thru and fixed

        if pd.isnull(collection_date):
            #It's blank - determine the collection month
            daystocollect = self.collections.collectionsGetDOS(clientID)

            if invoice_date + timedelta(days=daystocollect)  < proj_date:
                # Payment is late (later there will be a distribution around expected collections)
                collection_month = proj_month + kCollectionsMonthsLate

            else:
                collection_date = invoice_date + timedelta(days=daystocollect) 
                #need to convert this to collection month
                collection_month = inputs.dates.GetMonthNum(collection_date)

        elif collection_date >= proj_date:
            #error, collections shouldn't be after latest close date, that's in the future
            collection_month = inputs.dates.GetMonthNum(collection_date)

        else:
            collection_month = inputs.dates.GetMonthNum(collection_date)
            
        sim_term = proj_end - proj_month
        accrual_amount = 0
        deferred_amount = 0
        
        if (invoice_month > start_month) and (invoice_month >= proj_month):
            # its an accrual as of the start of the Simulation
            accrued = True
            deferred = False
            
            # Now check to see if there is any accrual amount before the start of the simulation
            # why why why???        
            #if start_month < proj_month:

            accrual_amount = -(invoice_month - start_month + 1) * mrr
            #if mrr == -666.67:
                
                #print("accrual amount: ", accrual_amount)
                #print("start month ", start_month)
                #print("end month: ", end_month)
                #print("invoice month: ", invoice_month)
                    
        else:
            accrued = False
            deferred = True
            
            if start_month < proj_month:          
                deferred_amount = -(proj_month - start_month) * mrr

        invoice_before_at_mrr = False
        invoice_middle_mrr = False
        invoice_after_mrr = False
              
        # determine where the invoice month occurs
        if (invoice_month <= start_month):     
            # Deferred
            invoice_before_at_mrr = True
            deferred_row[kRevProductIndex] = "Beginning" 
                
        if ((invoice_month > start_month) and (invoice_month <= end_month)):
            # Accrual then Deferred
            invoice_middle_mrr = True
            deferred_row[kRevProductIndex] = "Middle"

            #print("projection month: ", proj_month)
            #print("row number: ", data_row[kRevLineNumIndex])
            #print("mrr : ", mrr)
            #print("accrued months: ", start_month)
            #print("deferred months: ", end_month)
            #print("invoice month: ", invoice_month)
            #print("total term - calculated: ", end_month - start_month + 1)
            #print("total term: ", term)
            #accrued_term = invoice_month - start_month
            #deferred_term = end_month - invoice_month + 1
            #print("accrued term: ", accrued_term)
            #print("deferred term: ", deferred_term)            
                
        if (invoice_month > end_month):
            # Accrual
            invoice_after_mrr = True
            deferred_row[kRevProductIndex] = "End" 

        if not(invoice_before_at_mrr) and not(invoice_middle_mrr) and not(invoice_after_mrr):
            print("Error in begin/middle/end")

        #Calculate min and max months open - in order to minumize loop & save execution time
                   
        for i in range(proj_month, proj_end):


            #record mrr & DR side
            if (i >= start_month) and (i <= end_month):

                revenue_row[i+kRevColumns] = -mrr
                if commission_type == kCommMRR:
                    commissions_row[i+kRevColumns] = mrr * commission

                if deferred:
                    deferred_row[i+kRevColumns] = mrr
                else:
                    accrued_row[i+kRevColumns] = mrr

            # invoice month
            if i == invoice_month:
                #DR to AR
                invoice_row[i+kRevColumns] = invoice_amount
                if commission_type == kCommInvoice:
                    commissions_row[i+kRevColumns] = invoice_amount * commission

                #CR to Accrued/Deferred
                if invoice_before_at_mrr:

                    if invoice_month < start_month:
                        deferred_amount = 0

                    else:
                        #already recorded the mrr, so eliminate
                         deferred_amount = mrr    # used to be 'mrr'

                    deferred_row[i+kRevColumns] = round( - invoice_amount + deferred_amount, 2)                

                elif invoice_middle_mrr:

                    #eliminate accruals
                    accrued_row[i+kRevColumns] = round(accrued_row[i+kRevColumns] + accrual_amount, 2)
                    #add remainder to deferred
                    deferred_row[i+kRevColumns] = -round(invoice_amount + accrual_amount, 2)
                    deferred = True
                    accrued = False
                    
                else:
                    accrued_row[i+kRevColumns] = - invoice_amount                    

            #collection month
            if i == collection_month and collection_month >= proj_month:
                # use the above created collection month
                collection_row[i+kRevColumns] = invoice_amount
                if commission_type == kCommCollection:
                    commissions_row[i+kRevColumns] = -invoice_amount * commission

        #get existing df
        revenue_df = dict[kRevenue]
        invoice_df = dict[kInvoices]
        deferred_df =dict[kDeferred]
        accrual_df =dict[kAccrual]
        collections_df = dict[kCollections]
        commissions_df = dict[kCommissions]

        #append current row
        revenue_df.append(revenue_row)
        invoice_df.append(invoice_row)
        deferred_df.append(deferred_row)
        accrual_df.append(accrued_row)
        collections_df.append(collection_row)
        commissions_df.append(commissions_row)
            
        #update dict items
        dict[kRevenue] = revenue_df
        dict[kInvoices] = invoice_df
        dict[kDeferred] = deferred_df
        dict[kAccrual] = accrual_df
        dict[kCollections] = collections_df
        dict[kCommissions] = commissions_df
        
        return dict, invoice_row

    def setup_commissions(self, commissions_df, inputs):

        commissions_dict = {}
        row = [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT]

        if len(commissions_df) == 0 :
            row[kCommUseIndex] = False
            commissions_dict[kExisting] = row
            commissions_dict[kRenew] = row
            commissions_dict[kNew] = row

        else:
            commissions_list = commissions_df.values.tolist()
            for row in commissions_list:
                if row[kCommRevTypeIndex] == kExisting:
                    commissions_dict[kExisting] = row
                elif row[kCommRevTypeIndex] == kRenew:
                    commissions_dict[kRenew] = row
                elif row[kCommRevTypeIndex] == kNew:
                    commissions_dict[kNew] = row
                else:
                    print("Bad revenue type data in commission rows")

        return commissions_dict

    def split_revenue_by_product(self, sheet, key, df, inputs):

        #new dict for product revenue vector 
        products_rev_Dict = {}
        writer = pd.ExcelWriter(inputs.full_path_output + key + kProducts_file, engine = 'xlsxwriter')
        for row in self.products.products_list:
            product_name = row[kProductNameIndex]
            df2 = df.copy()
            #df2.to_excel(writer, sheet_name = product_name, index = False)

            if (key == kExisting):
                #print(key, product_name)
                df2 = df2[df2[kRevProduct] == product_name]
                df2.to_excel(writer, sheet_name = product_name, index = False)
                df2.drop(df2.iloc[:, 0:kRevColumns], inplace = True, axis = 1)
                df2 = df2.sum(axis=0)
                rev_list = df2.tolist()

            elif (key == kRenew):
                #print(key, product_name)
                df2 = df2[df2[kRevProduct] == product_name]
                df2.to_excel(writer, sheet_name = product_name, index = False)
                df2.drop(df2.iloc[:, 0:kRevColumns], inplace = True, axis = 1)
                df2 = df2.sum(axis=0)
                rev_list = df2.tolist()

            elif (key == kNew):
                #print(key, product_name)
                df2.drop(df2.iloc[:, 0:kNewColumns], inplace = True, axis = 1)       
                df2 = df2.sum(axis=0)
                rev_list = df2.tolist()
                #Order must be redone versus above due to nature of new (not individual product invoices)
                df3 = df.copy()
                df3 = df3[df3[kRevProduct] == product_name]
                df3.to_excel(writer, sheet_name = product_name, index = False)

            else:
                print("The is a nonexistant key: ", key)

            # add product sheets to proper key
            products_rev_Dict[product_name] = rev_list
        
        self.revenue_products[key] = products_rev_Dict
        writer.close()

    def write_dict(self, revenue_dict, sheet_header, revenue_dict_to_TB, inputs):

        for key, dict in revenue_dict.items():
            
            if key == kExisting:
                writer = pd.ExcelWriter(inputs.full_path_output + kExisting_file, engine = 'xlsxwriter')
                existing_totals = []
                existing_collections_totals = []
                for sheet, list  in dict.items():
                    # create a dictionary of totals by product revenue
                    # note that the "Revenue" key has it's own dictionary, by product
                    # all other keys are simple sum of the sheets

                    df = pd.DataFrame(list, columns = sheet_header)
                    df.to_excel(writer, sheet_name = sheet, index = False)
                    df.drop(df.iloc[:, 0:kRevColumns], inplace = True, axis = 1)
                    df2 = df.sum(axis=0)
                    sum_list = df2.tolist()
                    sum_list.insert(kFirst,sheet)
                    existing_totals.append(sum_list)
                    revenue_dict_to_TB[key] = existing_totals

                    if sheet == kRevenue:
                        #print("this is the revenues sheet for rev type: ", sheet, key)
                        #use original df as it has the product columns
                        df2 = pd.DataFrame(list, columns = sheet_header)
                        self.split_revenue_by_product(sheet, key, df2, inputs)

                writer.close()

            if key == kRenew:
                writer = pd.ExcelWriter(inputs.full_path_output + kRenew_file, engine = 'xlsxwriter')
                renew_totals = []
                renew_collections_totals = []
                for sheet, list  in dict.items():
                    df = pd.DataFrame(list, columns = sheet_header)
                    df.to_excel(writer, sheet_name = sheet, index = False)
                    df.drop(df.iloc[:, 0:kRevColumns], inplace = True, axis = 1)
                    df2 = df.sum(axis=0)
                    sum_list = df2.tolist()
                    sum_list.insert(kFirst,sheet)
                    renew_totals.append(sum_list)

                    if sheet == kRevenue:
                        df2 = pd.DataFrame(list, columns = sheet_header)
                        self.split_revenue_by_product(sheet, key, df2, inputs)

                revenue_dict_to_TB[key] = renew_totals
                writer.close()

            if key == kNew:
                writer = pd.ExcelWriter(inputs.full_path_output + kNew_file, engine = 'xlsxwriter')
                new_totals = []
                new_collections_totals = []
                for sheet, list  in dict.items():
                    
                    df = pd.DataFrame(list, columns = sheet_header)
                    df.to_excel(writer, sheet_name = sheet, index = False)
                    df.drop(df.iloc[:, 0:kNewColumns], inplace = True, axis = 1)
                    df2 = df.sum(axis=0)
                    sum_list = df2.tolist()
                    sum_list.insert(kFirst,sheet)
                    new_totals.append(sum_list)

                    if sheet == kRevenue:
                        df2 = pd.DataFrame(list, columns = sheet_header)
                        self.split_revenue_by_product(sheet, key, df2, inputs)

                revenue_dict_to_TB[key] = new_totals
                writer.close()
     
        return revenue_dict_to_TB

    def create_revenue_TB_list(self, revenue_dict, sheet_header):

        pass
        #takes full list, with data columns
        #
        # strips data columns
        # sums each column
        # ends up with a revenue type dict, 5 entries for each worksheet/df
        # each of those dicts are housed in a master dict with the 3 types of revenues: self.revenue_dict_to_TB

#
# Here are the testing subroutines - to determine the state of the contract/renweal
#


    def ContractRenewing(self, end_date, inputs):
        if inputs.dates.GetMonth(end_date) - inputs.dates.GetMonth(inputs.projections_date) >= -1:
            contract_renewing = True
        else:    
            contract_renewing = False
        
        return contract_renewing

    def GetContractTermEnded(self, current_end_date, inputs):

        proj_end_date = inputs.end_date
        new_start_date = current_end_date + relativedelta(days = 1)
        
        if new_start_date > proj_end_date:
            contract_ended = True
        else:    
            contract_ended = False
        
        return contract_ended

    def GetTermEnded(self, start_date, term, invoice_month, inputs):
        
        end_month = inputs.dates.GetMonthNum(start_date) + term
        collections_month = invoice_month +kCollectionsMonthsLag

        if (collections_month < inputs.projections_start) and (end_month < inputs.projections_start):
            return True
        else:
            return False

    def PrepJEforTB(self, rev_type, row, month, TB, inputs, amount, key):

        filtered_df = self.ac_revenues[self.ac_revenues.Sheet == rev_type]
        accounts_ok = True

        if len(filtered_df) != kJELines:
            accounts_ok = False
            
        else:        
            if filtered_df.iat[0,kDRCRIndex] == kDR and filtered_df.iat[1,kDRCRIndex] == kCR:
                pass

            elif filtered_df.iat[0,kDRCRIndex] == kCR and filtered_df.iat[1,kDRCRIndex] == kDR:
                pass

            else:
                accounts_ok = False
                
        #there needs to be code to confirm that the accounts to be used in the JE are in the COAccounts

            if accounts_ok:

                if (rev_type == kRevenue):
                    #loop thru all the products 
                    #how does this work for "New" ?
                    for row in self.products.products_list:
                        product_name = row[kProductNameIndex]
                        DR_acct = filtered_df.iat[0,kAccountIndex]
                        CR_acct = row[kProductAccountIndex]

                        #get amount from product revenue data structure
                        products_rev = self.revenue_products[key]
                        products_list = products_rev[product_name]
                        amount = - products_list[month]
                        

                        if key == kNew:
                            #print("Amount: ", amount)
                            percent_product = row[kProductPercentIndex]
                            amount = round(amount * percent_product, 2)
                            #print("Amount after percent: ", amount, percent_product)

                        #call the JE for each product

                        TB = self.journal_entry.performJE(month, TB, DR_acct, CR_acct, amount)

                else:

                    if (rev_type == kDeferred):
                        amount = - amount
                    
                    DR_acct = filtered_df.iat[0,kAccountIndex]
                    CR_acct = filtered_df.iat[1,kAccountIndex]
                    
                    #call the JE
                    TB = self.journal_entry.performJE(month, TB, DR_acct, CR_acct, amount)

                if self.rev_explog:
                    log_date = inputs.get_date(month)
                    log_list = [log_date, DR_acct, CR_acct, amount]
                    self.transaction_log.append(log_list)
            else:
                print("something is wrong with how the accounts are setup")
                
        return TB
