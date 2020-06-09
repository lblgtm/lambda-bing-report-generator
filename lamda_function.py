import json
from bing_report import BingReport

def lambda_handler(event, context):
    # TODO implement
    start_date = event["queryStringParameters"]["startDate"]
    end_date = event["queryStringParameters"]["endDate"]
    report = BingReport()
    resp = report.generate(start_date,end_date)
    return resp
