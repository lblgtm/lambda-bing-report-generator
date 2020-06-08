import json
from bing_report import BingReport

def lambda_handler(event, context):
    # TODO implement
    report = BingReport()
    resp = report.generate('2020-06-01','2020-06-8')
    return event
