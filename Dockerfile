FROM golang:latest

#install packages
RUN go get github.com/aws/aws-lambda-go/lambda \
  github.com/aws/aws-sdk-go/aws \
  github.com/aws/aws-sdk-go/aws/session \
  github.com/aws/aws-sdk-go/service/s3 \

RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go build -o main .
CMD ["/app/main"]
