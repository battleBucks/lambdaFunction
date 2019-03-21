package main

import (
        "github.com/aws/aws-lambda-go/lambda"
        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/aws/session"
        "github.com/aws/aws-sdk-go/service/s3"
        "log"
        "fmt"
        "bytes"
        "net/http"
        "io/ioutil"
)

var s3Objects []*s3.Object
var svc *s3.S3
var bucket = "gn5456-easel-dev-batch-image-uploader"
//var bucketPrefix = "jb_source/"
var doneDir = "done/"
var errorDir = "error/"
var kafkaUrl = "http://qa2-usw2np-cp-kafka-rest.service.usw2-np.consul:8082/topics/easel-tmp"

func init() {
        sess := session.Must(session.NewSession())
        svc = s3.New(sess)
        result, _ := svc.ListObjectsV2(&s3.ListObjectsV2Input{
                Bucket: aws.String(bucket),
//                Prefix: aws.String(bucketPrefix),
//                StartAfter: aws.String(bucketPrefix),
        })
        s3Objects = result.Contents
}

func easelKafkaHandler() (error) {
        log.Print("Starting process...\n")
        for _, s3Object := range s3Objects {
                key := *s3Object.Key
                err := validateExtension(key)
                if err != nil {
                        log.Print(err)
                        //err := copyAndDeleteObject(key, errorDir)
                        continue
                }
                jsonString, err := getObject(key)
                if err != nil {
                        log.Print("unable to get object ", key, " from s3: ", err)
                        continue
                }
                err = postToKafka(jsonString)
                if err != nil {
                        log.Print(err)
                        _ = copyAndDeleteObject(key, errorDir)
                        continue
                }
                err = copyAndDeleteObject(key, doneDir)
                //err = copyObject(key, doneDir)
                if err != nil {
                        log.Print(err)
                        continue
                }
                /*
                err = deleteObject(key)
                if err != nil {
                        log.Print(err)
                        continue
                }
                */
        }
        return nil
}

func postToKafka(jsonObject []byte) error {
        log.Print("preparing to post ", string(jsonObject), " to: ",kafkaUrl, "\n")
        req, err := http.NewRequest("POST", kafkaUrl, bytes.NewBuffer(jsonObject))
        req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
        response, err := http.DefaultClient.Do(req)
        if err != nil {
                return fmt.Errorf("Unable to parse response. There may be a problem with the kafka topic. %v", err)
        }
        jsonResponse, _ := ioutil.ReadAll(response.Body)
        log.Print("postToKafka response: ", string(jsonResponse))
        return nil
}

func getObject(key string) ([]byte, error) {
        log.Print("Grabbing file: ",key)
        result, err := svc.GetObject(&s3.GetObjectInput{
                Bucket: aws.String(bucket),
                Key: aws.String(key),
        })
        if err != nil {
                return []byte{}, err
        }
        jsonString, _ := ioutil.ReadAll(result.Body)
        return jsonString, nil
}

func copyAndDeleteObject(key, destination string) error {
        err := copyObject(key, destination)
        if err != nil {
                return err
        }
        err = deleteObject(key)
        if err != nil {
                return err
        }
        return nil
}

func copyObject(key, destination string) error {
        log.Print("copying key: ", key, " to ", destination)
        _, err := svc.CopyObject(&s3.CopyObjectInput{
                Bucket: aws.String(bucket),
                CopySource: aws.String(bucket + "/" + key),
                Key: aws.String(destination + key),
        })
        if err != nil {
                return fmt.Errorf("Unable to copy object to %s bucket. %v", destination, err)
        }
        log.Print("Waiting for copy to complete")
        err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{
                Bucket: aws.String(bucket),
                Key: aws.String(destination + key),
        })
        if err != nil {
                return fmt.Errorf("Error occurred while waiting for item to be copied to %s bucket. %v", destination, err)
        }
        return nil
}

func deleteObject(key string) error {
        log.Print("deleting key: ", key)
        _, err := svc.DeleteObject(&s3.DeleteObjectInput{
                Bucket: aws.String(bucket),
                Key: aws.String(key),
        })
        if err != nil {
                return fmt.Errorf("Error occurred while deleting key from bucket. %v", err)
        }
        log.Print("Waiting for deletion to complete")
        err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
                Bucket: aws.String(bucket),
                Key:    aws.String(key),
        })
        if err != nil {
                return fmt.Errorf("Error occurred while waiting for item to be deleted to done bucket. %v", err)
        }
        return nil
}

func validateExtension(key string) error {
                if key[len(key)-5:] == ".json" {
                        return nil
                }
                return fmt.Errorf("S3 Object: %s is of improper format", key)
}

func main() {
        lambda.Start(easelKafkaHandler)
}
