// 로그 출력 및 작성을 처리하는 패키지
package logger

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	// AWS
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	// Privacydam package
	"github.com/tovdata/privacydam-go/core"
	"github.com/tovdata/privacydam-go/core/model"
)

// 로그 메시지를 출력하는 함수입니다.
//	# Parameters
//	logType (string): log type [debug|notice|warning|error]
//	message (string): log message
func PrintMessage(logType string, message string) {
	// Set buffer
	var buffer bytes.Buffer

	// Set log message prefix (by log type)
	switch logType {
	case "debug":
		buffer.WriteString("[DEBUG] ")
	case "notice":
		buffer.WriteString("[NOTICE] ")
	case "warning":
		buffer.WriteString("[WARNING] ")
	case "error":
		buffer.WriteString("[ERROR] ")
	default:
		buffer.WriteString("[DEBUG] ")
	}
	// Set log message
	buffer.WriteString(message)
	// Print log
	log.Println(buffer.String())
}

// API 처리 로그를 작성하는 함수입니다.
//	# Parameters
//	accessor (model.Accessor): accessor information object
//	api (model.Api): api information object
//	evaluation (model.Evaluation): k-anonymity evaluation result
//	result (string): processing result
func WriteProcessedResult(accessor model.Accessor, api model.Api, evaluation model.Evaluation, result string) {
	// Create processed format
	processed, err := CreateProcessedFormat(accessor, api, evaluation, result)
	if err != nil {
		PrintMessage("error", err.Error())
		return
	}

	// Send message
	if err := SendProcessedResult(processed); err != nil {
		PrintMessage("error", err.Error())
		return
	}
}

// API 처리 객체를 생성하는 함수입니다. API를 처리한 데이터를 이용하여 로그 작성을 위한 데이터 형식을 생성합니다.
//	# Parameters
//	accessor (model.Accessor): accessor information object
//	api (model.Api): api information object
//	evaluation (model.Evaluation): k-anonymity evaluation result
//	result (string): processing result
func CreateProcessedFormat(accessor model.Accessor, api model.Api, evaluation model.Evaluation, result string) (model.Processed, error) {
	// Extract current time
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// Get database object to load the source dsn by api uuid
	dbInfo, err := core.GetExternalDatabase(api.SourceId)
	if err != nil {
		return model.Processed{}, err
	}

	// Extract parameter values
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for i, value := range api.QueryContent.ParamsValue {
		buffer.WriteString(value.(string))
		if i < len(api.QueryContent.ParamsValue)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("]")

	// Set processed log format
	params := model.Processed{
		ApiAlias:  api.Alias,
		DateTime:  currentTime,
		Result:    result,
		RemoteIp:  accessor.Ip,
		UserAgent: accessor.UserAgent,
		Detail: model.ProcessedDetail{
			Dsn:       dbInfo.Dsn,
			KAnoPass:  evaluation.Result,
			KAnoValue: strconv.FormatInt(evaluation.Value, 10),
			Params:    buffer.String(),
			Syntax:    api.QueryContent.Syntax,
		},
	}

	// Return
	return params, nil
}

// API 처리 로그 메시지를 생성하고 SQS로 전송하는 함수입니다. createProcessedMessage()를 호출하여 SQS SendMessage를 생성하고, SendMessage()를 호출하여 메시지를 SQS로 전송합니다.
func SendProcessedResult(processed model.Processed) error {
	// Create message
	message, err := createProcessedMessage(processed)
	if err != nil {
		return err
	}
	// Send message
	return SendMessage(message)
}

/*
 * [Private function] Create processed message (fit sqs.SendMessageInput)
 * <IN> processed (model.Processed): processed data
 * <OUT> (*sqs.SendMessageInput): created message format
 * <OUT> (error): error object (contain nil)
 */
func createProcessedMessage(processed model.Processed) (*sqs.SendMessageInput, error) {
	// Get sqs url
	queueUrl := os.Getenv("AWS_SQS_URL")

	// Transformt detail object to string
	data, err := core.TransformToJSON(processed)
	if err != nil {
		return &sqs.SendMessageInput{}, err
	}

	// Set body
	var buffer bytes.Buffer
	buffer.WriteString("time: ")
	buffer.WriteString(processed.DateTime)
	buffer.WriteString(", processed result")

	// Set parameters
	params := &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueUrl),
		MessageGroupId: aws.String("privacydam_process"),
		MessageBody:    aws.String(buffer.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"apiAlias":  createSqsMessageAttributeValue(processed.ApiAlias),
			"datetime":  createSqsMessageAttributeValue(processed.DateTime),
			"result":    createSqsMessageAttributeValue(processed.Result),
			"remoteIp":  createSqsMessageAttributeValue(processed.RemoteIp),
			"userAgent": createSqsMessageAttributeValue(processed.UserAgent),
			"detail":    createSqsMessageAttributeValue(string(data)),
		},
	}

	// Return
	return params, err
}

/*
 * [Private function] Create sqs message attribute format
 * <IN> value (string): value
 * <OUT> (types.MessageAttributeValue): message attribute value (using sqs)
 */
func createSqsMessageAttributeValue(value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}

// AWS SQS로 생성한 메시지를 전달하는 함수입니다.
func SendMessage(params *sqs.SendMessageInput) error {
	// Get sqs client
	sqsClient, err := core.GetSqsClient()
	if err != nil {
		return err
	}
	// Send message
	if _, err := sqsClient.SendMessage(context.TODO(), params); err != nil {
		return err
	} else {
		return nil
	}
}

// AWS SQS로 생성한 메시지들을 전달하는 함수입니다. (Batch)
func SendMessages(params *sqs.SendMessageBatchInput) error {
	// Get sqs client
	sqsClient, err := core.GetSqsClient()
	if err != nil {
		return err
	}
	// Send messages
	if _, err := sqsClient.SendMessageBatch(context.TODO(), params); err != nil {
		return err
	} else {
		return nil
	}
}

// API 처리 성능 측정을 위한 구조체
type Measurement struct {
	Api     string
	Data    map[string]*MeasurementData
	GroupId string
}

// API 처리 성능 측정 데이터 구조체
type MeasurementData struct {
	duration  int64
	endTime   time.Time
	startTime time.Time
}

// API 성능 측정을 위한 객체를 생성하는 함수입니다.
//	# parameters
//	apiName (string): created API name
//	groupId (string): id to identify process
func MeasurementPerformance(apiName string, groupId string) *Measurement {
	return &Measurement{
		Api:     apiName,
		Data:    map[string]*MeasurementData{},
		GroupId: groupId,
	}
}

// 성능 측정을 위한 API 이름을 설정하는 함수입니다.
func (m *Measurement) SetApiName(apiName string) {
	m.Api = apiName
}

// 성능 측정을 위한 GroupId를 설정하는 함수입니다.
func (m *Measurement) SetGroupId(groupId string) {
	m.GroupId = groupId
}

// 성능 측정을 시작하는 함수입니다.
func (m *Measurement) Start(key string) {
	data := &MeasurementData{
		startTime: time.Now(),
	}
	// Append
	m.Data[key] = data
}

// 성능 측정을 종료하는 함수입니다. 종료 시간과 소요 시간을 계산하여 기록합니다.
func (m *Measurement) End(key string) {
	m.Data[key].endTime = time.Now()
	m.Data[key].duration = (m.Data[key].endTime.UnixNano() / int64(time.Microsecond)) - (m.Data[key].startTime.UnixNano() / int64(time.Microsecond))
}

// 측정 시작 시간을 반환하는 함수입니다.
//	# Response
//	(string): start time [format: YYYY-MM-DDTHH:mm:ss.nnnnnn]
func (m *MeasurementData) GetStartTime() string {
	return m.startTime.Format("2006-01-02T15:04:05.999999")
}

// 측정 종료 시간을 반환하는 함수입니다.
//	# Response
//	(string): end time [format: YYYY-MM-DDTHH:mm:ss.nnnnnn]
func (m *MeasurementData) GetEndTime() string {
	return m.endTime.Format("2006-01-02T15:04:05.999999")
}

// 측정에 소요된 시간을 반환하는 함수입니다.
//	# Response
//	(string): duration is microseconds [format: xxx.xxx]
func (m *MeasurementData) GetDuration() string {
	return strconv.FormatInt(int64(m.duration/1000), 10) + "." + strconv.FormatInt(int64(m.duration%1000), 10)
}

// 측정에 대한 기록을 AWS SQS로 전송하는 함수입니다. 성능에 대한 모든 측정이 끝났을 경우에 호출합니다.
func (m *Measurement) SendMeasurement(print bool) {
	// Create entries (using send message batch)
	entries := make([]types.SendMessageBatchRequestEntry, len(m.Data))

	// Set entries
	cnt := 0
	for key, data := range m.Data {
		// Set id
		id := m.GroupId + "_" + strconv.FormatInt(int64(cnt+1), 10)
		// Set body
		var buffer bytes.Buffer
		buffer.WriteString("time: ")
		buffer.WriteString(m.GroupId)
		buffer.WriteString("measurement: ")
		buffer.WriteString(key)

		// Create entry
		entry := types.SendMessageBatchRequestEntry{
			Id:             aws.String(id),
			MessageGroupId: aws.String("privacydam_measurement"),
			MessageBody:    aws.String(buffer.String()),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"api":      createSqsMessageAttributeValue(m.Api),
				"duration": createSqsMessageAttributeValue(data.GetDuration()),
				"end":      createSqsMessageAttributeValue(data.GetEndTime()),
				"groupId":  createSqsMessageAttributeValue(m.GroupId),
				"name":     createSqsMessageAttributeValue(key),
				"start":    createSqsMessageAttributeValue(data.GetStartTime()),
			},
		}

		if print {
			fmt.Println(key)
			fmt.Println(data.GetDuration())
			fmt.Println(data.GetEndTime())
			fmt.Println(data.GetStartTime())
			fmt.Println()
		}

		// Append
		entries[cnt] = entry
		cnt++
	}

	// Get sqs url
	queueUrl := os.Getenv("AWS_SQS_URL")
	// Create message
	messages := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueUrl),
		Entries:  entries,
	}

	// Send
	SendMessages(messages)
}
