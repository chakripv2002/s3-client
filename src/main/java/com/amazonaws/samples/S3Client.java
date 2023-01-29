package com.amazonaws.samples;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
 * before you try to run this sample.
 */
public class S3Client {

	private static final int BUFFER_SIZE = 4096;
	private static final String DESTINATION_ZIP_FILE_NAME = "/Users/chakri/Downloads/S3_Files.zip";

	public static void main(String[] args) throws IOException {
		/*
		 * Create your credentials file at ~/.aws/credentials
		 * (C:\Users\USER_NAME\.aws\credentials for Windows users) and save the
		 * following lines after replacing the underlined values with your own.
		 *
		 * [default] aws_access_key_id = YOUR_ACCESS_KEY_ID aws_secret_access_key =
		 * YOUR_SECRET_ACCESS_KEY
		 */

		AmazonS3 s3 = new AmazonS3Client();

		String bucketName = "chakrimyawsbucket";
		String key = "sampleKey";

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon S3");
		System.out.println("===========================================\n");

		try {

			/*
			 * Upload an object to your bucket - You can easily upload a file to S3, or
			 * upload directly an InputStream if you know the length of the data in the
			 * stream. You can also specify your own metadata when uploading to S3, which
			 * allows you set a variety of options like content-type and content-encoding,
			 * plus additional metadata specific to your applications.
			 */
			//System.out.println("Uploading a new object to S3 from a file\n");
			//s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

			/*
			 * Download an object - When you download an object, you get all of the object's
			 * metadata and a stream from which to read the contents. It's important to read
			 * the contents of the stream as quickly as possibly since the data is streamed
			 * directly from Amazon S3 and your network connection will remain open until
			 * you read all the data or close the input stream.
			 *
			 * GetObjectRequest also supports several other options, including conditional
			 * downloading of objects based on modification times, ETags, and selectively
			 * downloading a range of an object.
			 */
			//System.out.println("Downloading an object");
			//S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			//System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
			//displayTextInputStream(object.getObjectContent());

			/*
			 * List objects in your bucket by prefix - There are many options for listing
			 * the objects in your bucket. Keep in mind that buckets with many objects might
			 * truncate their results when listing their objects, so be sure to check if the
			 * returned object listing is truncated, and use the
			 * AmazonS3.listNextBatchOfObjects(...) operation to retrieve additional
			 * results.
			 */
			//System.out.println("Listing objects");
			//ObjectListing objectListing = s3
					//.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix("downloadDirectory"));
			//for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				//System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
			//}
			System.out.println();

			//downloadDirectory(bucketName, "downloadDirectory", "/Users/chakri/Downloads/s3-download-dir");
			
			zipS3Directory();
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	/**
	 * Creates a temporary file with text data to demonstrate uploading a file to
	 * Amazon S3
	 *
	 * @return A newly created temporary file with text data.
	 *
	 * @throws IOException
	 */
	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("aws-java-sdk-", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.write("01234567890112345678901234\n");
		writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
		writer.write("01234567890112345678901234\n");
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.close();

		return file;
	}

	/**
	 * Displays the contents of the specified input stream as text.
	 *
	 * @param input The input stream to display as text.
	 *
	 * @throws IOException
	 */
	private static void displayTextInputStream(InputStream input) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			System.out.println("    " + line);
		}
		System.out.println();
	}

	private static void downloadDirectory(String bucket_name, String key_prefix, String dir_path) {
		TransferManager xfer_mgr = TransferManagerBuilder.standard().build();

		try {
			MultipleFileDownload xfer = xfer_mgr.downloadDirectory(bucket_name, key_prefix, new File(dir_path));
			// loop with Transfer.isDone()
			XferMgrProgress.showTransferProgress(xfer);
			// or block with Transfer.waitForCompletion()
			XferMgrProgress.waitForCompletion(xfer);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
		xfer_mgr.shutdownNow();
	}

	private static void zipDirectory(File folder, String parentFolder, ZipOutputStream zos)
			throws FileNotFoundException, IOException {
		 
		for (File file : folder.listFiles()) {
			if (file.isDirectory()) {
				System.out.println("It is directory so recursively calling zipDirectory.");
				zipDirectory(file, parentFolder + "/" + file.getName(), zos);
				continue;
			}
			zos.putNextEntry(new ZipEntry(parentFolder + "/" + file.getName()));
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			long bytesRead = 0;
			byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;
			while ((read = bis.read(bytesIn)) != -1) {
				zos.write(bytesIn, 0, read);
				bytesRead += read;
			}
			zos.closeEntry();
		}
	}
	
public static void zipS3Directory() throws FileNotFoundException, IOException {
	   ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(DESTINATION_ZIP_FILE_NAME));
       
       zipDirectory(new File("/Users/chakri/Downloads/s3-download-dir/downloadDirectory"), "gen-letters", zos);
           
       zos.flush();
       zos.close();
}

}
