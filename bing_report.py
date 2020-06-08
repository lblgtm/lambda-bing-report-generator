
from bingads.v13.reporting import *
from bingads.authorization import *
from suds import WebFault
import asyncpg
import asyncio
import webbrowser
import datetime
import pytz
import time
import sys
import csv
import re

#from output_helper import output_bing_ads_webfault_error, output_webfault_errors, output_status_message
from bingads.service_client import ServiceClient
from bingads.authorization import AuthorizationData, OAuthDesktopMobileAuthCodeGrant

import sys
import webbrowser
from time import gmtime, strftime
from suds import WebFault

# You must provide credentials in auth_helper.py.
# TODO - move this to a config file
# The ACCOUNT_ID controls which site data we pull. You can pull the site account id from the URL in the Bing Ads UI

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.client').setLevel(logging.DEBUG)
logging.getLogger('suds.transport.http').setLevel(logging.DEBUG)


import ssl

ssl._create_default_https_context = ssl._create_unverified_context


DEVELOPER_TOKEN='1154X0F726384383'
ENVIRONMENT='production'
CLIENT_SECRET='fI-4M1Im1-cD.tKQQVIsJWW8e.r8-kZd8_'
CLIENT_STATE='cbsi_bing_ads_upload'
REFRESH_TOKEN_PATH="refresh.txt"
REDIRECT_URI='https://retool.com'

#ACCOUNT_ID=146004742
ACCOUNT_ID=146004157
ACCOUNT_IDS=[
       146001562,	
       146003062,	
       146004157,	
       146004171,	
       146004172,	
       146004426,	
       146004560,	
       146004709,	
       146004935,	
       146004976,
       146005113,	
       146005232,	
       146005747,	
       146005748,	
       146005809,	
       146006020	
]

CUSTOMER_ID=18186246
CLIENT_ID='53ac2d03-a48e-4768-8c79-d4dee2049f99'
REFRESH_TOKEN="refresh.txt"



# The directory for the report files.
FILE_DIRECTORY='/tmp'

# The name of the report download file.
DOWNLOAD_FILE_NAME='download.csv'

# The report file extension type.
REPORT_FILE_FORMAT='Csv'

# The maximum amount of time (in milliseconds) that you want to wait for the report download.
TIMEOUT_IN_MILLISECONDS=3600000


# These methods instantiate api helpers from the sdk
authentication=OAuthWebAuthCodeGrant(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirection_uri=REDIRECT_URI,
    require_live_connect=True
)



authorization_data=AuthorizationData(
    account_id=ACCOUNT_ID,
    customer_id=CUSTOMER_ID,
    developer_token=DEVELOPER_TOKEN,
    authentication=authentication,
)

reporting_service, reporting_service_manager = None, None

class BingReport():

    def __init__(self):
        pass
    # This main method handles the entire process
    def generate(self,start_date,end_date):
        global reporting_service, reporting_service_manager
        #start_time = time.time()

        # authenticates user
        self.authenticate_with_oauth()

        reporting_service_manager=ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment=ENVIRONMENT
        )

        # In addition to ReportingServiceManager, you will need a reporting ServiceClient
        # to build the ReportRequest.

        reporting_service=ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment=ENVIRONMENT,
        )

        try:
            aggregation = 'Daily'
            exclude_column_headers=False
            exclude_report_footer=False
            exclude_report_header=False
            return_only_complete_data=False
            account_id=ACCOUNT_ID
            report_file_format=REPORT_FILE_FORMAT
            
    
            time = self.toCustomTime(start_date,end_date)
            #report_request = self.get_keyword_performance_report_request(start_date,end_date)
            
            report_request=self.get_campaign_performance_report_request(
                            account_id,
                            aggregation,
                            exclude_column_headers,
                            exclude_report_footer,
                            exclude_report_header,
                            report_file_format,
                            return_only_complete_data,
                            time)
            

            reporting_download_parameters = ReportingDownloadParameters(
                report_request=report_request,
                result_file_directory = FILE_DIRECTORY,
                result_file_name = DOWNLOAD_FILE_NAME,
                overwrite_result_file = True, # Set this value true if you want to overwrite the same file.
                timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
            )

            #Option A - Background Completion with ReportingServiceManager
            #You can submit a download request and the ReportingServiceManager will automatically
            #return results. The ReportingServiceManager abstracts the details of checking for result file
            #completion, and you don't have to write any code for results polling.

            #output_status_message("Awaiting Background Completion . . .")
            self.background_completion(reporting_download_parameters)

            #Option B - Submit and Download with ReportingServiceManager
            #Submit the download request and then use the ReportingDownloadOperation result to
            #track status yourself using ReportingServiceManager.get_status().

            #output_status_message("Awaiting Submit and Download . . .")
            #submit_and_download(report_request)

            #Option C - Download Results with ReportingServiceManager
            #If for any reason you have to resume from a previous application state,
            #you can use an existing download request identifier and use it
            #to download the result file.

            #For example you might have previously retrieved a request ID using submit_download.
            #reporting_operation=reporting_service_manager.submit_download(report_request)
            #request_id=reporting_operation.request_id

            #Given the request ID above, you can resume the workflow and download the report.
            #The report request identifier is valid for two days.
            #If you do not download the report within two days, you must request the report again.
            #output_status_message("Awaiting Download Results . . .")
            #download_results(request_id, authorization_data)

           # output_status_message("Program execution completed")

        except WebFault as ex:
            print(" HIT ERROR!!!!", ex.fault.detail.ApiFaultDetail.OperationErrors.OperationError.Message)
            #output_webfault_errors(ex)
        except Exception as ex:
            print(" HIT GEN ERROR", ex )
            #output_status_message(ex)
        finally:
            # Once the data is retrieved, we pass it to a csv file
            #data = csv_to_list()

            downloaded_file = FILE_DIRECTORY + "/" + DOWNLOAD_FILE_NAME
        
            #with open('download.csv', 'r') as f:
            with open(downloaded_file, mode='r', encoding='utf-8-sig') as f:
                
                # convert the data to a list
                data = list(csv.reader(f, delimiter=',', quotechar='"'))
                data = data[11:]
            
            
            metrics = []                        
            for row in data:
                if len(row) <= 12: break
                metrics.append({
                                "TimePeriod":row[0], 
                                "AccountName":row[1], 
                                "AccountId":row[2], 
                                "CampaignName":row[3], 
                                "CustomerName":row[4], 
                                "CurrencyCode":row[5], 
                                "Impressions":row[6], 
                                "DeviceType":row[7], 
                                "CampaignStatus":row[8], 
                                "Clicks":row[9],
                                "ReturnOnAdSpend":row[10],  
                                "Revenue":row[11],
                                "Spend":row[12]           
                            })     
            return metrics                       


    def get_keyword_performance_report_request(self,start_date,end_date):
        '''
        Build a keyword performance report request, including Format, ReportName, Aggregation,
        Scope, Time, Filter, and Columns.
        '''
        global reporting_service
        report_request=reporting_service.factory.create('KeywordPerformanceReportRequest')
        report_request.Format=REPORT_FILE_FORMAT
        report_request.ReportName='Keyword Performance Report'
        report_request.ReturnOnlyCompleteData=False
        report_request.Aggregation='Daily'
        #report_request.Language='English'
        report_request.ExcludeColumnHeaders=True
        report_request.ExcludeReportHeader=True
        report_request.ExcludeReportFooter=True

        scope=reporting_service.factory.create('AccountThroughCampaignReportScope')
        scope.AccountIds=None #{'long': [authorization_data.account_id] }
        scope.Campaigns=None
        scope.AdGroups=None
        report_request.Scope=scope

        # We create a time object
        report_time=reporting_service.factory.create('ReportTime')
        report_time.ReportTimeZone='PacificTimeUSCanadaTijuana'

        # We create a end date
        report_date_end=reporting_service.factory.create('Date')
        #now = datetime.datetime.now(pytz.timezone('US/Pacific'))
        end_date = end_date.split('-')
        report_date_end.Day= int(end_date[2])
        report_date_end.Month= int(end_date[1])
        report_date_end.Year= int(end_date[0])
        report_time.CustomDateRangeEnd=report_date_end

        # We create a date a start date
        report_date_start=reporting_service.factory.create('Date')
        #yesterday = now - datetime.timedelta(days=1)
        start_date = start_date.split('-')
        report_date_start.Day= int(start_date[2])
        report_date_start.Month= int(start_date[1])
        report_date_start.Year= int(start_date[0])
        report_time.CustomDateRangeStart=report_date_start

        # https://docs.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#aggregation-time
        # We set the predefinedTime to None in order for it to accept the custom range
        report_time.PredefinedTime=None #"ThisMonth" 
        report_request.Time=report_time

        # Specify the attribute and data report columns.
        report_columns=reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
        report_columns.CampaignPerformanceReportColumn.append([
            'TimePeriod',
        ])
        report_request.Columns=report_columns

        # You may optionally sort by any KeywordPerformanceReportColumn, and optionally
        # specify the maximum number of rows to return in the sorted report.

        #report_sorts=reporting_service.factory.create('ArrayOfKeywordPerformanceReportSort')
        #report_sort=reporting_service.factory.create('KeywordPerformanceReportSort')
        #report_sort.SortColumn='TimePeriod'
        #report_sort.SortOrder='Ascending'
        #report_sorts.KeywordPerformanceReportSort.append(report_sort)
        #report_request.Sort=report_sorts
        #
        # report_request.MaxRows=10

        return report_request


    def get_campaign_performance_report_request(self,
        account_id,
        aggregation,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time):
        
        report_request=reporting_service.factory.create('CampaignPerformanceReportRequest')
        report_request.Aggregation=aggregation
        report_request.ExcludeColumnHeaders=exclude_column_headers
        report_request.ExcludeReportFooter=exclude_report_footer
        report_request.ExcludeReportHeader=exclude_report_header
        report_request.Format=report_file_format
        report_request.ReturnOnlyCompleteData=return_only_complete_data
        report_request.Time=time    
        report_request.ReportName="Campaign Performance Report"
        scope=reporting_service.factory.create('AccountThroughCampaignReportScope')
        scope.AccountIds= {'long': ACCOUNT_IDS }
        scope.Campaigns=None
        report_request.Scope=scope     

        report_columns=reporting_service.factory.create('ArrayOfCampaignPerformanceReportColumn')
        report_columns.CampaignPerformanceReportColumn.append([
            'TimePeriod',
            'AccountName',
            'AccountId',
            'CampaignName',
            'CustomerName',
            'Impressions',
            'CurrencyCode',
            'DeviceType',
            'CampaignStatus',
            'Clicks',
            'ReturnOnAdSpend',
            'Revenue',
            'Spend'
        ])
        report_request.Columns=report_columns

    
        return report_request

    def toCustomTime(self,start_date,end_date):
           
        # We create a time object
        # https://docs.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#aggregation-time
        # We set the predefinedTime to None in order for it to accept the custom range
        report_time=reporting_service.factory.create('ReportTime')
        report_time.ReportTimeZone='PacificTimeUSCanadaTijuana'
        report_time.PredefinedTime=None #Option: "ThisMonth"

        # We create a end date
        report_date_end=reporting_service.factory.create('Date')
        #now = datetime.datetime.now(pytz.timezone('US/Pacific'))
        end_date = end_date.split('-')
        report_date_end.Day= int(end_date[2])
        report_date_end.Month= int(end_date[1])
        report_date_end.Year= int(end_date[0])
        report_time.CustomDateRangeEnd=report_date_end

        # We create a date a start date
        report_date_start=reporting_service.factory.create('Date')
        #yesterday = now - datetime.timedelta(days=1)
        start_date = start_date.split('-')
        report_date_start.Day= int(start_date[2])
        report_date_start.Month= int(start_date[1])
        report_date_start.Year= int(start_date[0])
        report_time.CustomDateRangeStart=report_date_start

        return report_time


    def background_completion(self,reporting_download_parameters):
        '''
        You can submit a download request and the ReportingServiceManager will automatically 
        return results. The ReportingServiceManager abstracts the details of checking for result file 
        completion, and you don't have to write any code for results polling.
        '''
        global reporting_service_manager
        result_file_path = reporting_service_manager.download_file(reporting_download_parameters)
        #output_status_message("Download result file: {0}\n".format(result_file_path))

    def submit_and_download(self,report_request):
        '''
        Submit the download request and then use the ReportingDownloadOperation result to 
        track status until the report is complete e.g. either using
        ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status().
        '''
        global reporting_service_manager
        reporting_download_operation = reporting_service_manager.submit_download(report_request)

        # You may optionally cancel the track() operation after a specified time interval.
        reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)

        # You can use ReportingDownloadOperation.track() to poll until complete as shown above,
        # or use custom polling logic with get_status() as shown below.
        #for i in range(10):
        #    time.sleep(reporting_service_manager.poll_interval_in_milliseconds / 1000.0)

        #    download_status = reporting_download_operation.get_status()

        #    if download_status.status == 'Success':
        #        break

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory = FILE_DIRECTORY,
            result_file_name = DOWNLOAD_FILE_NAME,
            decompress = True,
            overwrite = True,  # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
        )

        #output_status_message("Download result file: {0}\n".format(result_file_path))


    def download_results(self,request_id, authorization_data):
        '''
        If for any reason you have to resume from a previous application state, 
        you can use an existing download request identifier and use it 
        to download the result file. Use ReportingDownloadOperation.track() to indicate that the application 
        should wait to ensure that the download status is completed.
        '''
        reporting_download_operation = ReportingDownloadOperation(
            request_id = request_id,
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=1000,
            environment=ENVIRONMENT,
        )

        # Use track() to indicate that the application should wait to ensure that
        # the download status is completed.
        # You may optionally cancel the track() operation after a specified time interval.
        reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory = FILE_DIRECTORY,
            result_file_name = DOWNLOAD_FILE_NAME,
            decompress = True,
            overwrite = True,  # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
        )

        #output_status_message("Download result file: {0}".format(result_file_path))
        #output_status_message("Status: {0}\n".format(reporting_operation_status.status))

    def authenticate_with_oauth(self):
        # It is recommended that you specify a non guessable 'state' request parameter to help prevent
        # cross site request forgery (CSRF).
        authentication.state=CLIENT_STATE

        # Assign this authentication instance to the authorization_data.
        authorization_data.authentication=authentication

        # Register the callback function to automatically save the refresh token anytime it is refreshed.
        # Uncomment this line if you want to store your refresh token. Be sure to save your refresh token securely.

        authorization_data.authentication.token_refreshed_callback=self.save_refresh_token

        refresh_token=self.get_refresh_token()

        try:
            # If we have a refresh token let's refresh it
            if refresh_token is not None:
                authorization_data.authentication.request_oauth_tokens_by_refresh_token(refresh_token)
            else:
                self.request_user_consent(authorization_data)
        except OAuthTokenRequestException:
            # The user could not be authenticated or the grant is expired.
            # The user must first sign in and if needed grant the client application access to the requested scope.
            self.request_user_consent(authorization_data)

        return authorization_data

    def request_user_consent(self,authorization_data):
        authorization_endpoint = authorization_data.authentication.get_authorization_endpoint()
        webbrowser.open(authorization_endpoint, new=1)
        # For Python 3.x use 'input' instead of 'raw_input'
        if(sys.version_info.major >= 3):
            response_uri=input(
                "You need to provide consent for the application to access your Bing Ads accounts. " \
                "After you have granted consent in the web browser for the application to access your Bing Ads accounts, " \
                "please enter the response URI that includes the authorization 'code' parameter: \n"
            )
        else:
            response_uri=raw_input(
                "You need to provide consent for the application to access your Bing Ads accounts. " \
                "After you have granted consent in the web browser for the application to access your Bing Ads accounts, " \
                "please enter the response URI that includes the authorization 'code' parameter: \n"
            )

        if authorization_data.authentication.state != CLIENT_STATE:
            raise Exception("The OAuth response state does not match the client request state.")

        # Request access and refresh tokens using the URI that you provided manually during program execution.
        authorization_data.authentication.request_oauth_tokens_by_response_uri(response_uri=response_uri)

    def get_refresh_token(self):
        ''' 
        Returns a refresh token if stored locally.
        '''
        file=None
        try:
            file=open(REFRESH_TOKEN_PATH)
            line=file.readline()
            file.close()
            return line if line else None
        except IOError:
            if file:
                file.close()
            return None

    def save_refresh_token(self,oauth_tokens):
        ''' 
        Stores a refresh token locally. Be sure to save your refresh token securely.
        '''
        with open(REFRESH_TOKEN_PATH,"w+") as file:
            file.write(oauth_tokens.refresh_token)
            file.close()
        return None

    async def insert_to_pg(self,data):
        # Create the db connection
        db = await asyncpg.connect('postgresql://clickfactory:click2018@clickfactoryprod.ccavhumaz3qp.us-west-1.rds.amazonaws.com/clickfactoryprod')

        # Submit and await the response
        # The copy functionality is the fastest way to bulk upload to a postgres db
        response = await db.copy_records_to_table(
            'answersite_bing_spend',
            schema_name='click_factory_db',
            records=data,
            columns=[
                'day_dt',
                'hour',
                'account_id',
                'campaign_nm',
                'campaign_id',
                'keyword',
                'keyword_id',
                'adgroup_nm',
                'adgroup_id',
                'device_type',
                'bid_match_type',
                'clicks',
                'impressions',
                'ctr',
                'ave_cpc',
                'spend',
                'quality_score',
                'keyword_status',
                'ave_position',
                'bid'
            ]
        )

        # close the connection upon completion
        await db.close()
        return response

    # Filter and convert the raw data to a csv file
    def csv_to_list(self):
        # Empty list to save the data
        keyword_list = []

        # Timezone variables for conversion
        utc = pytz.utc
        pacific = pytz.timezone('US/Pacific')

        # We set our target date/time to 1 hour prior to the current UTC time
        now = datetime.datetime.now(utc)
        target_date = now - datetime.timedelta(hours=1)

        # Open the file containing the raw bing spend data

        downloaded_file = FILE_DIRECTORY + "/" + DOWNLOAD_FILE_NAME
        
        #with open('download.csv', 'r') as f:
        with open(downloaded_file, mode='r', encoding='utf-8-sig') as f:

            # convert the data to a list
            reader = list(csv.reader(f, delimiter=',', quotechar='"'))

            # loop through list
            for row in reader:
                # When requesting the date column with hourly separation, bing provides a date string with an hour appended
                # to the end, ie: "11/12/2018 12:00:00 AM|1". Here we split on the irrelevant data to get the data and hour.

                time_period = row[0].replace('"','')
                time_period, tz = time_period.split(' 12:00:00 AM|')            

                # We split the date in order to create a new date object localized to PST and convert it to UTC
                date = time_period.split('/')

                converted_date = pacific.localize(datetime.datetime(int(date[2]), int(date[0]), int(date[1]), int(tz))).astimezone(utc)

                # We check if the date matches our target UTC day/time
                if converted_date.date() == target_date.date() and converted_date.hour == target_date.hour:
                    # Remove the date string and replace it with the UTC day/time
                    row.pop(0)
                    row.insert(0, int(converted_date.hour))
                    row.insert(0, converted_date)

                    # We convert the statical data to integers/floats to match the column requirements in the db
                    row[11] = int(row[11])
                    row[12] = int(row[12])
                    row[14] = self.convert_float(row[14])
                    row[15] = self.convert_float(row[15])
                    row[16] = self.convert_float(row[16])
                    row[18] = self.convert_float(row[18])

                    # add the processed row to the list
                    keyword_list.append(row)

        return keyword_list

    # Converts a string to a float
    def convert_float(self,str):
        if str == '':
            return 0.0
        else:
            return float(str)