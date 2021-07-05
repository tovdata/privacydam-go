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

/*
 * Configurate AWS X-Ray
 * <IN> address (string): X-Ray Deamon address
 * <OUT> (error): error object (contain nil)
 */
func ConfigXray(address string) error {
	return xray.Configure(xray.Config{
		DaemonAddr:     address,
		ServiceVersion: "1.0.0",
	})
}

/*
 * Configurate tracking
 * <IN> configPath (string): configuration file path
 * <OUT> (error): error object (contain nil)
 */
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
		return errors.New("Configuration failed (not found processing tracking status)")
	}
	// Set config in environment various (sql tracking status)
	if value, ok := config["database"]; ok {
		os.Setenv("TRACK_A_DATABASE", value)
	} else {
		return errors.New("Configuration failed (not found databases tracking status)")
	}
	return nil
}

/*
 * Initialize database (create database connection pool)
 * <IN> ctx (context.Context): context
 * <IN> configPath (string): configuration file path
 * <OUT> (error): error object (contain nil)
 */
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

/*
 * Initialize api (set repeat function to get a list of api)
 * <IN> ctx (context.Context): context
 * <IN> minute (int64): repeat period
 */
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

/*
 * Update a list of api
 * <IN> ctx (context.Context): context
 * <IN> mutex (*sync.Mutex): mutex object (for sync)
 */
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
		return errors.New("DSN creation failed.")
	} else {
		// Set environment various
		os.Setenv("DSN", dsn)
		return nil
	}
}

// /*
//  * Add database connection pool
//  * <IN> ctx (context.Context): context
//  * <IN> tracking (bool): tracking with AWS X-Ray
//  * <IN> source (model.Source): source information object
//  * <OUT> (error): error object (contain nil)
//  */
// func AddDatabaseConnectionPool(ctx context.Context, tracking bool, source model.Source) error {
// 	return db.CreateConnectionPool(ctx, tracking, source, true)
// }

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
