// PrivacyDAM 구현에 기본적으로 필요한 함수들이 정의된 패키지
package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	// AWS
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"github.com/tovdata/privacydam-go/core/model"

	// Util
	"github.com/tovdata/privacydam-go/core/db"
)

var (
	apis      = make(map[string]model.Api)
	sqsClient *sqs.Client
)

// AWS X-Ray를 사용하기 위해 설정하는 함수입니다.
//	# Parameters
//	address (string): ip address on AWS X-Ray daemon [format. <ip>:<port>]
func ConfigXray(address string) error {
	return xray.Configure(xray.Config{
		DaemonAddr:     address,
		ServiceVersion: "1.0.0",
	})
}

// AWS X-Ray를 이용한 추적에 대한 설정 함수입니다.
//	# Parameters
//	configPath (string): config file path
func ConfigTracking(configPath string) error {
	// Read data
	rawConfiguration, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	// Transform to map
	var config map[string]string
	if err := json.Unmarshal(rawConfiguration, &config); err != nil {
		return err
	}

	// Set config in environment various (processing tracking status)
	if value, ok := config["processing"]; ok {
		os.Setenv("TRACK_A_PROCESSING", value)
	} else {
		return errors.New("Configuration failed (not found processing tracking status)\r\n")
	}
	// Set config in environment various (sql tracking status)
	if value, ok := config["database"]; ok {
		os.Setenv("TRACK_A_DATABASE", value)
	} else {
		return errors.New("Configuration failed (not found databases tracking status)\r\n")
	}
	return nil
}

// 내부 또는 외부에서 사용할 데이터베이스의 초기화 작업을 수행하는 함수입니다. 설정 파일 또는 환경 변수에 저장된 설정 값을 이용하여 데이터베이스에 대한 초기화를 진행합니다.
//	# Parameters
//	configPath (interface{}): config file path or nil(load config data from process environment various)
func InitializeDatabase(ctx context.Context, configPath interface{}) error {
	if reflect.ValueOf(configPath).Kind() == reflect.String {
		// Load configuration and set environment various
		if err := loadDatabaseConfiguration(configPath.(string)); err != nil {
			return err
		}
	}

	// Initialize database
	return db.Initialization(ctx)
}

// 생성된 API의 정보들을 가져오기 위해 Polling을 설정하는 함수입니다. 지정한 시간마다 Polling을 수행합니다.
//	# Parameters
//	minute (int64): repeat period to polling
func InitializeApi(ctx context.Context, minute int64) {
	// Init
	UpdateApiList(ctx, Mutex)

	// Set time tick
	tick := time.Tick(time.Minute * time.Duration(minute))
	// Set repeat function
	go func() {
		for range tick {
			UpdateApiList(ctx, Mutex)
		}
	}()
}

// 생성된 API의 정보들을 가져오는 함수입니다. 생성된 API의 정보들을 가져와 메모리 상에 캐싱해두는 역할을 수행합니다.
//	# Parameters
//	mutex (*sync.Mutex): lock for sync
func UpdateApiList(ctx context.Context, mutex *sync.Mutex) {
	// Lock
	mutex.Lock()
	// Get a list of api
	list, err := db.In_getApiList(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	// Clear api
	apis = make(map[string]model.Api)
	// Transform to map
	for _, api := range list {
		apis[api.Alias] = api
	}

	// Unlock
	mutex.Unlock()
}

/*
 * Load database configuration (contain generate DSN)
 * <IN> configPath (string): database configuration file path
 * <OUT> (error): error object (contain nil)
 */
func loadDatabaseConfiguration(configPath string) error {
	// Load a database configuration
	rawConfiguration, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	// Transform to map
	var config map[string]string
	if err := json.Unmarshal(rawConfiguration, &config); err != nil {
		return err
	}

	// Generate DSN
	var dsn string
	switch config["name"] {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config["username"], config["password"], config["host"], config["port"], config["database"])
	case "hdb":
		dsn = fmt.Sprintf("hdb://%s:%s@%s:%s", config["username"], config["password"], config["host"], config["port"])
	}

	// Return DSN
	if dsn == "" {
		return errors.New("DSN creation failed.\r\n")
	} else {
		// Set environment various
		os.Setenv("DSN", dsn)
		return nil
	}
}

// AWS SQS Client를 생성하는 함수입니다. AWS SDK와 환경 변수에 저장된 AWS SQS URL를 이용하여 SQS를 사용할 수 있는 Client를 생성합니다.
//	# Parameters
//	region (string): aws region
func InitializeSQS(ctx context.Context, region string) error {
	// Create the AWS SQS client
	createSqsClient(ctx, region)

	// Set the parameters
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(os.Getenv("SQS")),
	}
	// Get the AWS SQS url
	output, err := sqsClient.GetQueueUrl(ctx, params)
	if err != nil {
		return err
	} else {
		fmt.Println(*output.QueueUrl)
	}

	// Set sqs url in environment various
	os.Setenv("AWS_SQS_URL", *output.QueueUrl)
	return nil
}

// 생성된 AWS SQS Client를 제공하는 함수입니다.
func GetSqsClient() (*sqs.Client, error) {
	return sqsClient, nil
}

func createSqsClient(ctx context.Context, region string) error {
	// Get AWS configuration
	configuration, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}

	sqsClient = sqs.NewFromConfig(configuration)
	return nil
}
