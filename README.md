# easelKafkaHandler
This function is triggered by an upload to an s3 bucket.
It scans the bucket and then sends json files to a kafka topic.
After POST to topic it copies the file to a done folder and deletes the original.

### How to build
run `docker run --rm -v "$PWD":/usr/src/easelKafkaHandler -v "$GOPATH":/go -w /usr/src/easelKafkaHandler -e GOOS=linux golang:latest go build -v`

### How to deploy
Zip up the compiled program
`zip handler.zip easelKafkaHandler`

upload from the cli (or the console)
`aws lambda update-function-code --function-name easelKafkaHandler --zip-file fileb://./handler.zip --profile <aws config profile>`

or if using vault
`aws-vault exec -- <aws-vault profile> aws lambda update-function-code --function-name easelKafkaHandler --zip-file fileb://./handler.zip`
