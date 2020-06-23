#This is code for batch jobs to compress heavy TSV files to .fst files and store it in a separate bucket.

library('aws.s3')
library('fst')


#IAM user credentials to give acces to AWS resources. 
#IAM user created in this case has List, Read and Write access to S3 resources
Sys.setenv(
	"AWS_ACCESS_KEY_ID" = "AKIA35UNGTNGZW4FSBBP",
	"AWS_SECRET_ACCESS_KEY" = "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",
	"AWS_DEFAULT_REGION" = "us-west-2"

)

#Function to get compressed file name from the TSV file object key
getFSTfilename <- function(fileKey){
	length <- nchar(fileKey)
	start <- 16		#yusjain/output/ -> 16 characters
	end <- 29		#date format for filename -> 14 characters
	outputKey <- substr(fileKey, start, end)
	fileExt = ".fst"
	compressedFileKey = paste0(outputKey, fileExt)
	print("Output file key:")
	print(compressedFileKey)
	return (compressedFileKey)
}

#Function to get the path of the object from the object key and bucket name.
getInputObjectPath <- function(bucketName, fileKey){
	s3access <- "s3:/"
	objectPath <- paste(s3access, bucketName, fileKey, sep = "/")
}

compressAndStoreFile <- function(objectPath, fileKey){
	#Reading the TSV file to be compressed
	tempfile <- tempfile()
	save_object(object = objectPath, file = tempfile)
	data <- read.csv(tempfile, sep ="\t", header = TRUE)

	#TSV file compressed to FST file
	fst_file <- tempfile(fileext = ".fst")
	write_fst(data, fst_file)

	#Make Output Object key
	compressedFileKey <- getFSTfilename(fileKey)

	#Put Compressed file in Bucket "yusjainoutput"
	put_object(file = fst_file, object = compressedFileKey, bucket = "yusjainoutput", multipart = TRUE)
}

#Bucket Name and Object Key to be compressed
args <- commandArgs(TRUE)
inputBucketName <- args[1]
inputFileKey <- args[2]

inputObjectPath <- getInputObjectPath(inputBucketName, inputFileKey)
print("Taking file from:")
print(inputObjectPath)

compressAndStoreFile(inputObjectPath, inputFileKey)

print("Job executed successfully")
