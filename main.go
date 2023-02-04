package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var SourceBucketUrl = "https://ismail-staging-bucket.fra1.cdn.digitaloceanspaces.com"

func main() {
	sourceS3Client := SourceS3Client{
		Key:        "your source bucket key",
		Secret:     "your source bucket secret",
		Endpoint:   "https://fra1.digitaloceanspaces.com",
		Region:     "us-east-1",
		BucketName: "your source bucket name. Eg. ismail-staging-bucket",
	}
	targetS3Client := TargetS3Client{
		Key:        "your target bucket key",
		Secret:     "your target bucket secret",
		Endpoint:   "s3.eu-central-1.amazonaws.com",
		Region:     "eu-central-1",
		BucketName: "your target bucket name. Eg. staging-bucket",
	}

	existingKeysInTargetBucket, err := targetS3Client.GetKeys()
	if err != nil {
		log.Println(err.Error())
		return
	}

	startAfter := ""
	objectsToCopy, err := sourceS3Client.GetObjectsToCopy(startAfter)
	if err != nil {
		log.Println(err.Error())
		return
	}

	for {
		CopyObjects(objectsToCopy, existingKeysInTargetBucket, targetS3Client)

		if !*objectsToCopy.IsTruncated {
			fmt.Printf("\nDone copying objects\n\n")
			break
		}
		startAfter = *objectsToCopy.Contents[len(objectsToCopy.Contents)-1].Key
		objectsToCopy, err = sourceS3Client.GetObjectsToCopy(startAfter)
		if err != nil {
			log.Println(err.Error())
			return
		}
		fmt.Printf("\nthere are more objects than 1000, will get list of objects starting after: %s\n\n", startAfter)
	}
}

func CopyObjects(objectsToCopy *s3.ListObjectsV2Output, existingKeysInTargetBucket []string, targetS3Client TargetS3Client) {
	for _, o := range objectsToCopy.Contents {
		key := *o.Key
		if key[len(key)-1:] != "/" {
			if contains(existingKeysInTargetBucket, key) {
				fmt.Println("already copied skipping " + key)
				continue
			}
			fullUrl := SourceBucketUrl + "/" + *o.Key
			response, err := http.Get(fullUrl)
			if err != nil {
				log.Println(err.Error())
			}
			defer response.Body.Close()

			fileContent, err := ioutil.ReadAll(response.Body)
			s := string(fileContent)
			targetS3Client.UploadFile(*o.Key, s)
			fmt.Printf("copied %s\n", key)
		}
	}
}

type SourceS3Client struct {
	Key        string
	Secret     string
	Endpoint   string
	Region     string
	BucketName string
}

func (s SourceS3Client) GetObjectsToCopy(startAfter string) (*s3.ListObjectsV2Output, error) {
	objectsToCopy, err := s.getClient().ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:     aws.String(s.BucketName),
		StartAfter: aws.String(startAfter),
	})
	if err != nil {
		return nil, err
	}

	return objectsToCopy, nil
}

func (s SourceS3Client) getClient() *s3.S3 {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s.Key, s.Secret, ""),
		Endpoint:         aws.String(s.Endpoint),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(false), // Depending on your version, alternatively use o.UsePathStyle = false
	}

	newSession := session.New(s3Config)
	s3Client := s3.New(newSession)

	return s3Client
}

type TargetS3Client struct {
	Key        string
	Secret     string
	Endpoint   string
	Region     string
	BucketName string
}

func (t TargetS3Client) GetKeys() ([]string, error) {
	keys := make([]string, 0)

	alreadyCopiedObjects, err := t.getClient().ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(t.BucketName),
	})
	if err != nil {
		log.Println(err.Error())
		return keys, err
	}

	for {
		for _, o := range alreadyCopiedObjects.Contents {
			key := *o.Key
			if key[len(key)-1:] != "/" {
				keys = append(keys, key)
			}
		}
		if !*alreadyCopiedObjects.IsTruncated {
			break
		}

		startAfter := *alreadyCopiedObjects.Contents[len(alreadyCopiedObjects.Contents)-1].Key
		alreadyCopiedObjects, err = t.getClient().ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:     aws.String(t.BucketName),
			StartAfter: aws.String(startAfter),
		})
		if err != nil {
			log.Println(err.Error())
			return keys, err
		}
	}

	return keys, nil
}

func (t TargetS3Client) getClient() *s3.S3 {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(t.Key, t.Secret, ""),
		Endpoint:         aws.String(t.Endpoint),
		Region:           aws.String(t.Region),
		S3ForcePathStyle: aws.Bool(false), // Depending on your version, alternatively use o.UsePathStyle = false
	}

	newSession := session.New(s3Config)
	s3Client := s3.New(newSession)

	return s3Client
}

func (t TargetS3Client) UploadFile(path string, content string) {
	s3Client := t.getClient()

	object := s3.PutObjectInput{
		Bucket: aws.String(t.BucketName),
		Key:    aws.String(path),
		Body:   strings.NewReader(content),
		ACL:    aws.String("public-read"),
	}
	_, err := s3Client.PutObject(&object)
	if err != nil {
		log.Println(err.Error())
	}
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
