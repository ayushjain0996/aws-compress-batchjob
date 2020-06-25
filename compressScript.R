#This is code for batch jobs to compress heavy TSV files to .fst files and store it in a separate bucket.

library('aws.s3')
library(aws.ec2metadata)
library('fst')


#IAM user credentials to give acces to AWS resources. 
#IAM user created in this case has List, Read and Write access to S3 resources
Sys.setenv(
	"AWS_ACCESS_KEY_ID" = "my-access-key",
	"AWS_SECRET_ACCESS_KEY" = "my-secret-access-key",
	"AWS_DEFAULT_REGION" = "us-west-2"

)

#Function to get compressed file name from the TSV file object key
getFSTfilename <- function(fileKey){
	splitFileKey = strsplit(fileKey, split = "/")
	tsvFileName = sapply(splitFileKey, tail, 1)
	outputFileName = substr(tsvFileName, 0, 14)
	fileExt = ".fst"
	compressedFileKey = paste0(outputFileName, fileExt)
	return (compressedFileKey)
}

#Function to get the path of the object from the object key and bucket name.
getInputObjectPath <- function(bucketName, fileKey){
	s3access <- "s3:/"
	objectPath <- paste(s3access, bucketName, fileKey, sep = "/")
}

compressAndStoreFile <- function(objectPath, fileKey){
	#Reading the TSV file to be compressed
	temporayTSVfile <- tempfile()
	save_object(object = objectPath, file = temporayTSVfile)
	data <- read.csv(temporayTSVfile, sep ="\t", header = TRUE)

	#TSV file compressed to FST file
	fstFile <- tempfile(fileext = ".fst")
	write_fst(data, fstFile)

	#Make Output Object key
	compressedFileKey <- getFSTfilename(fileKey)

	#Put Compressed file in Bucket "yusjainoutput"
	put_object(file = fstFile, object = compressedFileKey, bucket = "yusjainoutput")
	print("Compress Complete")
}

batchJob <- function(){
	#Bucket Name and Object Key to be compressed
	args <- commandArgs(TRUE)
	inputBucketName <- args[1]
	inputFileKey <- args[2]

	inputObjectPath <- getInputObjectPath(inputBucketName, inputFileKey)
	print("Taking file from:")
	print(inputObjectPath)

	compressAndStoreFile(inputObjectPath, inputFileKey)

	print("Job executed successfully")
}

batchJob()