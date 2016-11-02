"""
s3lambda.py takes in a few arguments to a main function
then leverages this info to do the following:

1. Obtain a CSV file from S3
2. Parse the CSV file
3. Hand the parsed lines into a Kinesis stream

Additionally modifying the timeout is supported.

Your event should have the following items:

{
  "s3_bucket": "BUCKET"
  "s3_key": "KEY_TO_YOUR_S3_ASSET",
  "kinesis_stream": "PATH_TO_KINESIS_STREAM",
  "default_throttling": "1"
}

The default_throttling is a delay for inserting items into the 
stream in seconds.

Keep in mind lambda functions have a maximum lifespan of 5 minutes, so your delays should be small.

"""
__author__ = "Chris King"
__contact__ = "chrskn@amazon.com"		

# Global Imports
import csv
import time
from StringIO import StringIO
import boto3


# Actual Code
def get_file_from_S3(s3_bucket, s3_key):
	"""
	get_file_from_S3 will create an S3 client using boto first.
	Next it fetches your object from the specified bucket and converts the string
	into a StringBuffer ( to be treated like a file object.)
	The buffer is then returned.
	"""
	s3 = boto3.resource('s3')
	obj = s3.Object(s3_bucket, s3_key)
	data = obj.get()['Body'].read()
	buffer = StringIO(data)
	return buffer

def push_data_to_kinesis_stream(data, kinesis_stream, delay):
	"""
	push_data_to_kinesis_stream takes in a single line from a CSV, a kinesis stream, and the desire delay.
	Currently it just prints the data, then sleeps for the specified delay in seconds.
	#TODO add code to push to kinesis here.
	"""
	print data
	time.sleep(delay)

def parse_file(data_buffer, kinesis_stream, delay=1):
	"""
	parse_file takes in a data_buffer or StringBuffer of the file.
	It then parses the string as a full object into specific lines.
	The lines are handed over to push_data_to_kinesis_stream.
	"""
	reader = csv.reader(buffer)
	for line in reader:
	    push_data_to_kinesis_stream(data=line, kinesis_stream=kinesis_stream, delay=delay)

def s3_hanlder(event, context):
	"""
	s3_handler is the main function for this script.
	It takes the event items that you passed in and calls get_file_from_S3 first.
	The output from get_file_from_S3 is then handed to parse_file.

	parse_file will then hand the data over finally to push_data_to_kinesis_stream.
	Upon completion of the script everything goes home.

	"""
    live_data_buffer = get_file_from_S3(s3_bucket=event['s3_bucket'], s3_key=event['s3_key'])
    parse_file(data_buffer=live_data_buffer, kinesis_stream=event['kinesis_stream'], delay=int(event['default_throttling']))