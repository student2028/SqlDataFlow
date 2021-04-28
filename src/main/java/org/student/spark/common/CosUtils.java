package org.student.spark.common;


import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.Protocol;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.auth.BasicAWSCredentials;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder;
import com.ibm.cloud.objectstorage.regions.Regions;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3URI;
import com.ibm.cloud.objectstorage.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/*
 *
 * cos util
 * cos file upload ,download, list
 * with this you can create data for cloud_load_log if the table missing
 */
public class CosUtils {

    final static Logger logger = LoggerFactory.getLogger(CosUtils.class);

    private static AmazonS3 s3Client = null;

    public static void initialize(String accessKey, String secretKey, String endPoint) {
        if (null == s3Client) {
            createS3Client(accessKey, secretKey, endPoint);
        }
    }

    public static AmazonS3 getS3Client() {
        if (s3Client == null) {
            throw new DataFlowException("you should initialized the cosUtils before use it ");
        } else {
            return s3Client;
        }
    }

    public static AmazonS3 createS3Client(String accessKey, String secretKey, String endPoint) {
        return createS3Client(accessKey, secretKey, endPoint, false);
    }

    public static AmazonS3 createS3Client(String accessKey, String secretKey, String endPoint, boolean useHttp) {
        AWSCredentials credential = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(50000);
        clientConfig.setUseTcpKeepAlive(true);
        if (useHttp) {
            clientConfig.withProtocol(Protocol.HTTP);
        }

        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credential))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, Regions.DEFAULT_REGION.getName()))
                .withClientConfiguration(clientConfig).build();
        return s3Client;
    }

    public static void downloadFile(String bucketName, String fileObjKeyName, String path) {
        GetObjectRequest request = new GetObjectRequest(bucketName, fileObjKeyName);
        s3Client.getObject(request, new File(path));
    }

    public static void downloadFile(String bucketFile, String targetPath) {
        final String path = bucketFile.replaceAll("^cos://", "s3://");
        logger.info("download file {}", path);
        final AmazonS3URI uri = new AmazonS3URI(path);
        GetObjectRequest request = new GetObjectRequest(uri.getBucket(), uri.getKey());
        s3Client.getObject(request, new File(targetPath));
    }

    public static String getFileAsString(String filePath) {
        final String path = filePath.replaceAll("^(\\w+)://([^.]*)\\.?(\\w+)?/","s3://$2/");
        logger.info("s3 filepath is {}", path);
        final AmazonS3URI uri = new AmazonS3URI(path);
        logger.info("getCosFileAsString:" + path);
        String text;
        try (S3Object s3Object = s3Client.getObject(uri.getBucket(), uri.getKey());
             S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3ObjectInputStream, StandardCharsets.UTF_8))
        ) {
            text = bufferedReader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new DataFlowException(e);
        }
        return text;
    }

    public static void downloadStream(String bucketName, String fileObjKeyName, Consumer<InputStream> consumer) {
        S3Object returned = s3Client.getObject(bucketName, fileObjKeyName);

        try (S3ObjectInputStream s3Input = returned.getObjectContent()) {
            consumer.accept(s3Input);
        } catch (Exception ex) {
            logger.error("downloadStream", ex);
        }
    }


    public static void putObject(String bucketName, String fileObjKeyName, String fileName, String description) {
        logger.info("ready to put file {} to bucket {}", fileName, bucketName);
        PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
        ObjectMetadata metadata = new ObjectMetadata();
        //metadata.setContentType("plain/text");
        metadata.addUserMetadata("description", description);
        request.setMetadata(metadata);
        s3Client.putObject(request);
    }


    public static void listLatestTableVersion(String bucket) {

        String prefix = "";
        List<String> keys = listKeysInBucket(bucket, prefix);
        keys.forEach(
                key -> {
                    List<String> list = listKeysInBucket(bucket, key);
                    list.sort(Comparator.reverseOrder());
                    if (list.size() > 0) {
                        logger.info(list.get(0));
                    }
                });
    }

    public static List<String> listKeysInBucket(String bucketName, String prefix) {
        boolean isTopLevel = false;
        String delimiter = "/";
        if (prefix.isEmpty() || prefix.equals("/")) {
            isTopLevel = true;
        }
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }

        ListObjectsV2Request listObjectsRequest;
        if (isTopLevel) {
            listObjectsRequest =
                    new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withMaxKeys(500);
        } else {
            listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix)
                    .withDelimiter(delimiter);
        }
        ListObjectsV2Result objects = s3Client.listObjectsV2(listObjectsRequest);
        return objects.getCommonPrefixes();
    }


    //not used now
    public static java.sql.Timestamp getFileTimestamp(String remoteFilePath) {
        String regexBucket = "(?<=://)(.*?)(?=.service/)";
        String bucketName = CommonUtils.regexExtractFirst(remoteFilePath, regexBucket);
        String regexKey = "(?<=" + bucketName + ".service/)(.*)";
        String fileKey = CommonUtils.regexExtractFirst(remoteFilePath, regexKey);
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, fileKey));
        ObjectMetadata objectMetadata = fullObject.getObjectMetadata();
        Date lastModified = objectMetadata.getLastModified();
        return new java.sql.Timestamp(lastModified.getTime());
    }
}
