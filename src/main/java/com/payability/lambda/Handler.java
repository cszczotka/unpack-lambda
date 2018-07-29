package com.payability.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.zip.GZIPInputStream;

public class Handler implements RequestHandler<Object, String> {


    private static final Logger LOG = Logger.getLogger(Handler.class);

    @Override
    public String handleRequest(Object input, Context context) {
        LOG.debug("Received class name: " + input.getClass().getCanonicalName());
        LOG.debug("Received: " + input.toString());
        try {

            if (input instanceof LinkedHashMap) {
                processS3Event(S3EventNotification.parseJson(new Gson().toJson(input)));
           } else if(input instanceof S3Event) {
                processS3Event((S3Event)input);
            } else {
                LOG.error("Not supported input type " + input.getClass().getCanonicalName());
                return "ERROR";
            }
        } catch (IOException e) {
            LOG.error("Exception during handling request " + e.getMessage());
            return "ERROR";
        }
        return "SUCCESS";
    }


    private void processS3Event(S3EventNotification input) throws IOException {
        LOG.info("Processing S3Event ....");
        LOG.info("Number of s3Event records + " + input.getRecords().size());
        for (S3EventNotification.S3EventNotificationRecord record : input.getRecords()) {

            String srcBucket = record.getS3().getBucket().getName();
            LOG.info("S3Event for bucket: " + srcBucket);
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            AmazonS3 s3Client = new AmazonS3Client();

            LOG.info("Getting gz object from bucket: " + srcBucket);
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
            GZIPInputStream zis = new GZIPInputStream(new BufferedInputStream(s3Object.getObjectContent()));

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(zis, outputStream);
            LOG.info("Unpacking gz ");
            InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(outputStream.size());
            meta.setContentType("text/csv");
            LOG.info("Saving into bucket unpacking gz ");
            s3Client.putObject(srcBucket, FilenameUtils.getFullPath(srcKey) + FilenameUtils.getBaseName(srcKey) + ".csv", is, meta);
            is.close();
            outputStream.close();
            LOG.info("Processing S3Event is finished");
        }
    }

}
