#This is code for batch jobs to compress heavy TSV files to .fst files and store it in a separate bucket.

library('aws.s3')
library(aws.ec2metadata)
library('fst')


# Function to access IAM role to get access credentials
setAccessSecretKeys <- function(roleName, region = "us-west-2"){
	(role <- metadata$iam_info())
	print(paste0('IAM info is :', role))
	if(!is.null(role)){
		r = metadata$iam_role(roleName)
		print(r$AccessKeyId)
		print(r$SecretAccessKey)
		Sys.setenv(
			"AWS_ACCESS_KEY_ID" = r$AccessKeyId,
			"AWS_SECRET_ACCESS_KEY" = r$SecretAccessKey,
			"AWS_SESSION_TOKEN" = r$Token,
			"AWS_DEFAULT_REGION" = region
		)
	}
}

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

#Function to compress the TSV file to FST file
compressAndStoreFile <- function(inputObjectPath, fileKey){
	#Reading the TSV file to be compressed
	temporayTSVfile <- tempfile()
	save_object(object = inputObjectPath, file = temporayTSVfile)
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
	setAccessSecretKeys('IAM-RoleName')

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