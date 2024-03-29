// PrivacyDAM에 생성된 API를 실질적으로 처리하기 위한 패키지
package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	// ORM
	"github.com/jmoiron/sqlx"

	// AWS
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"github.com/tovdata/privacydam-go/core/model"
	// Core (database pool)
	coreDB "github.com/tovdata/privacydam-go/core/db"
	// Util
	util "github.com/tovdata/privacydam-go/core/util"
	"github.com/tovdata/privacydam-go/process/util/did"
	"github.com/tovdata/privacydam-go/process/util/kAno"
)

// 외부 데이터베이스와의 Connection을 테스트하는 함수입니다.
//	# Parameters
//	driverName (string): database driver name (ex. mysql, hdb ...)
//	dsn (string): database source name
func Ex_testConnection(ctx context.Context, driverName string, dsn string) error {
	// Create database object
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// Test connection
	return db.Ping()
}

// 데이터 수정(Insert, Update, Delete)에 대한 처리를 수행하는 함수입니다.
//	# Parameters
//	sourceId (string): source uuid by generated database
//	params ([]interface): API parameter values
//	isTest (bool): test or not
//
//	# Response
//	(int64): affected row count by query
func Ex_changeData(ctx context.Context, sourceId string, querySyntax string, params []interface{}, isTest bool) (int64, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] Set the subsegment
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Process change")
		defer subSegment.Close(nil)
	}

	// Set default various
	var affected int64 = 0

	// Get database object
	dbInfo, err := coreDB.GetDatabase("external", sourceId)
	if err != nil {
		return affected, err
	}

	// Processing by test or not
	if isTest {
		// Begin transaction
		tx, err := dbInfo.Instance.Begin()
		if err != nil {
			return affected, err
		}
		defer tx.Rollback()
		// Execute query
		var result sql.Result
		if dbInfo.Tracking {
			result, err = tx.ExecContext(subCtx, querySyntax, params...)
		} else {
			result, err = tx.Exec(querySyntax, params...)
		}
		// Catch error
		if err != nil {
			return affected, err
		} else {
			return result.RowsAffected()
		}
	} else {
		// Execute query
		var result sql.Result
		if dbInfo.Tracking {
			result, err = dbInfo.Instance.ExecContext(subCtx, querySyntax, params...)
		} else {
			result, err = dbInfo.Instance.Exec(querySyntax, params...)
		}
		// Catch error
		if err != nil {
			return affected, err
		} else {
			return result.RowsAffected()
		}
	}
}

// 데이터 반출 처리를 수행하는 함수입니다. (For echo framework)
//	# Parameters
//	res (http.ResponseWriter): writer for response
//	routineCount (int): go-routine count
//	apiName (string): API alias
//	sourceId (string): source uuid by generated database
//	querySyntax (string): syntax to query
//	params ([]interface): API parameter values
//	didOptions (map[string]model.AnoParamOption): de-identification option by column
//
//	# Response
//	(model.Evaluation): K-anonymity evaluation result
func Ex_exportData(ctx context.Context, res http.ResponseWriter, routineCount int64, apiName string, sourceId string, querySyntax string, params []interface{}, didOptions map[string]model.AnoParamOption) (model.Evaluation, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// Set default evaluation structure
	evaluation := model.Evaluation{}
	// Get database object
	dbInfo, err := coreDB.GetDatabase("external", sourceId)
	if err != nil {
		return evaluation, err
	}

	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	// [For debug] Set the subsegment
	if tracking {
		_, subSegment = xray.BeginSubsegment(ctx, "Prepare export")
	}
	/* Prepare part */
	// Get queue size from environment various (default: 10,000)
	queueSize, err := strconv.ParseInt(os.Getenv("QUEUE_SIZE"), 10, 64)
	if err != nil {
		queueSize = 50000
	}

	// Set process count for go-routine
	nTransProc := uint64(routineCount)
	nAnonyProc := uint64(routineCount)
	// Create channel(data queue) for go-routine
	iDataQueue := make(chan map[string]interface{}, queueSize)
	tDataQueue := make(chan []string, queueSize)
	aDataQueue := make(chan []string, queueSize)
	// Create channel(process queue) for go-routine
	quitQuery := make(chan bool)
	quitTrans := make(chan bool, nTransProc)
	quitAnony := make(chan bool, nAnonyProc)
	quitProce := make(chan model.Evaluation)
	if tracking {
		subSegment.Close(nil)
	}

	// cc := runtime.GOMAXPROCS(1)
	// fmt.Println("Core count: " + strconv.FormatInt(int64(cc), 10))

	// [For debug] Set the subsegment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Process export")
	}
	/* Processing part */
	// Execute query
	var rows *sqlx.Rows
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(subCtx, querySyntax, params...)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax, params...)
	}
	// Catch error
	if err != nil {
		return evaluation, err
	}

	// Extract column types and column names
	// columnTypes, err := rows.ColumnTypes()
	// if err != nil {
	// 	return evaluation, err
	// }
	columns, err := rows.Columns()
	if err != nil {
		return evaluation, err
	}

	// Extract query result
	go executeExportQuery(subCtx, tracking, rows, iDataQueue, quitQuery)
	// Transform query result to string
	for i := uint64(0); i < nTransProc; i++ {
		go transformQueryResult(subCtx, tracking, columns, iDataQueue, tDataQueue, quitTrans)
	}
	// Process de-identification
	for i := uint64(0); i < nAnonyProc; i++ {
		go processDeIdentification(subCtx, tracking, didOptions, columns, tDataQueue, aDataQueue, quitAnony)
	}

	// Check K-Ano evaluation condition
	isEval := checkAnoEvaluationCondition(didOptions)
	// Write data
	go writeExportedData(subCtx, tracking, res, apiName, columns, isEval, aDataQueue, quitProce)

	// Exit logic
	completedTrans := uint64(0)
	completedAnony := uint64(0)
	for {
		select {
		case result := <-quitQuery:
			// Release database connection
			rows.Close()
			// Close channel
			close(iDataQueue)
			if !result {
				// Close channel
				close(tDataQueue)
				close(aDataQueue)
				if tracking {
					subSegment.Close(nil)
				}
				return evaluation, errors.New("Query error\r\n")
			}
		case <-quitTrans:
			completedTrans++
			if completedTrans >= nTransProc {
				// Close channel
				close(tDataQueue)
			}
		case <-quitAnony:
			completedAnony++
			if completedAnony >= nAnonyProc {
				// Close channel
				close(aDataQueue)
			}
		case evaluation := <-quitProce:
			if tracking {
				subSegment.Close(nil)
			}
			return evaluation, nil
		}
	}
}

// 데이터 반출 처리를 수행하는 함수입니다. (For aws lambda)
//	# Parameters
//	res (http.ResponseWriter): writer for response (AWS API Gateway proxy response)
//	routineCount (int): go-routine count
//	apiName (string): API alias
//	sourceId (string): source uuid by generated database
//	querySyntax (string): syntax to query
//	params ([]interface): API parameter values
//	didOptions (map[string]model.AnoParamOption): de-identification option by column
//
//	# Response
//	(model.Evaluation): K-anonymity evaluation result
func Ex_exportDataOnLambda(ctx context.Context, res *events.APIGatewayProxyResponse, routineCount int64, apiName string, sourceId string, querySyntax string, params []interface{}, didOptions map[string]model.AnoParamOption) (model.Evaluation, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// Set default evaluation structure
	evaluation := model.Evaluation{}
	// Get database object
	dbInfo, err := coreDB.GetDatabase("external", sourceId)
	if err != nil {
		return evaluation, err
	}

	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	// [For debug] Set the subsegment
	if tracking {
		_, subSegment = xray.BeginSubsegment(ctx, "Process export")
	}
	/* Prepare part */
	// Get queue size from environment various (default: 10,000)
	queueSize, err := strconv.ParseInt(os.Getenv("QUEUE_SIZE"), 10, 64)
	if err != nil {
		queueSize = 10000
	}

	// Set process count for go-routine
	nTransProc := uint64(routineCount)
	nAnonyProc := uint64(routineCount)
	// Create channel(data queue) for go-routine
	iDataQueue := make(chan map[string]interface{}, queueSize)
	tDataQueue := make(chan []string, queueSize)
	aDataQueue := make(chan []string, queueSize)
	// Create channel(process queue) for go-routine
	quitQuery := make(chan bool)
	quitTrans := make(chan bool, nTransProc)
	quitAnony := make(chan bool, nAnonyProc)
	quitProce := make(chan model.Evaluation)
	if tracking {
		subSegment.Close(nil)
	}

	// [For debug] Set the subsegment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Process export")
	}
	/* Processing part */
	// Execute query
	var rows *sqlx.Rows
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(subCtx, querySyntax, params...)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax, params...)
	}
	// Catch error
	if err != nil {
		return evaluation, err
	}

	// Extract column types and column names
	// columnTypes, err := rows.ColumnTypes()
	// if err != nil {
	// 	return evaluation, err
	// }
	columns, err := rows.Columns()
	if err != nil {
		return evaluation, err
	}

	// Extract query result
	go executeExportQuery(subCtx, tracking, rows, iDataQueue, quitQuery)
	// Transform query result to string
	for i := uint64(0); i < nTransProc; i++ {
		go transformQueryResult(subCtx, tracking, columns, iDataQueue, tDataQueue, quitTrans)
	}
	// Process de-identification
	for i := uint64(0); i < nAnonyProc; i++ {
		go processDeIdentification(subCtx, tracking, didOptions, columns, tDataQueue, aDataQueue, quitAnony)
	}

	// Check K-Ano evaluation condition
	isEval := checkAnoEvaluationCondition(didOptions)
	// Write data
	go writeExportedDataOnLambda(subCtx, tracking, res, apiName, columns, isEval, aDataQueue, quitProce)

	// Exit logic
	completedTrans := uint64(0)
	completedAnony := uint64(0)
	for {
		select {
		case result := <-quitQuery:
			// Release database connection
			rows.Close()
			// Close channel
			close(iDataQueue)
			if !result {
				// Close channel
				close(tDataQueue)
				close(aDataQueue)
				if tracking {
					subSegment.Close(nil)
				}
				return evaluation, errors.New("Query error\r\n")
			}
		case <-quitTrans:
			completedTrans++
			if completedTrans >= nTransProc {
				// Close channel
				close(tDataQueue)
			}
		case <-quitAnony:
			completedAnony++
			if completedAnony >= nAnonyProc {
				// Close channel
				close(aDataQueue)
			}
		case evaluation := <-quitProce:
			if tracking {
				subSegment.Close(nil)
			}
			return evaluation, nil
		}
	}
}

func checkAnoEvaluationCondition(didOptions map[string]model.AnoParamOption) bool {
	// Get options key count
	total := len(didOptions)

	if total == 0 {
		return false
	} else {
		count := 0
		for _, option := range didOptions {
			if option.Level > 0 {
				break
			}
			count++
		}

		if total == count {
			return false
		} else {
			return true
		}
	}
}

func executeExportQuery(ctx context.Context, tracking bool, rows *sqlx.Rows, iDataQueue chan<- map[string]interface{}, quitQuery chan<- bool) {
	// [For debug] Set the subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Export data")
		defer subSegment.Close(nil)
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		// allocated := allocateMemoryByScanType(columnTypes)
		// // Scan and store
		// rows.Scan(allocated...)

		allocated := make(map[string]interface{})
		rows.MapScan(allocated)

		iDataQueue <- allocated
	}
	// Catch error
	if err := rows.Err(); err != nil {
		log.Println(err.Error())
		quitQuery <- false
	} else {
		quitQuery <- true
	}
}

func transformQueryResult(ctx context.Context, tracking bool, columns []string, iDataQueue <-chan map[string]interface{}, tDataQueue chan<- []string, procQueue chan<- bool) {
	// [For debug] Set the subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Process transformation")
		defer subSegment.Close(nil)
	}

	for v, ok := <-iDataQueue; ok; v, ok = <-iDataQueue {
		converted := make([]string, len(columns))
		// for i, column := range v {
		// 	if columnTypes[i].ScanType() == nil {
		// 		converted[i] = transformToString("string", column)
		// 	} else {
		// 		converted[i] = transformToString(columnTypes[i].ScanType().String(), column)
		// 	}
		// }
		for i, key := range columns {
			converted[i] = transformToString(reflect.ValueOf(v[key]).Kind().String(), v[key])
		}

		tDataQueue <- converted
	}
	procQueue <- true
}

func processDeIdentification(ctx context.Context, tracking bool, options map[string]model.AnoParamOption, columns []string, tDataQueue <-chan []string, aDataQueue chan<- []string, quitAnony chan<- bool) {
	// [For debug] Set the subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Process de-identification")
		defer subSegment.Close(nil)
	}

	// build processing functions
	funcList := make([](func(string) string), len(columns))
	passAsIs := func(inString string) string {
		return inString
	}
	dropAll := func(inString string) string {
		return ""
	}

	for i, key := range columns {
		if option, exists := options[key]; exists == true {
			switch option.Method {
			case "encryption":
				funcList[i] = did.BuildEncryptingFunc(option.Options)
			case "rounding":
				funcList[i] = did.BuildRoundingFunc(option.Options)
			case "data_range":
				funcList[i] = did.BuildRangingFunc(option.Options)
			case "blank_impute":
				funcList[i] = did.BuildMaskingFunc(option.Options)
			case "pii_reduction":
				funcList[i] = did.BuildMaskingFunc(option.Options)
			case "non":
				funcList[i] = passAsIs
			default:
				funcList[i] = dropAll
			}
		} else {
			funcList[i] = passAsIs
		}
	}

	cnt := 0
	for v, ok := <-tDataQueue; ok; v, ok = <-tDataQueue {
		output := make([]string, len(columns))
		for i, value := range v {
			output[i] = funcList[i](value)
		}
		aDataQueue <- output
		cnt++
	}

	funcList = nil
	quitAnony <- true
}

func writeExportedData(ctx context.Context, tracking bool, res http.ResponseWriter, name string, header []string, isEval bool, aDataQueue <-chan []string, quitProce chan<- model.Evaluation) {
	// Set the subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Write data in response body")
		defer subSegment.Close(nil)
	}

	// Set a file name
	filename := name + "_export.csv"
	// Set response header
	res.Header().Set("Connection", "Keep-Alive")
	res.Header().Set("Transfer-Encoding", "chunked")
	res.Header().Set("X-Content-Type-Options", "nosniff")
	// Set stream file in response header
	res.Header().Set("Content-Disposition", "attachment;filename="+filename)
	res.Header().Set("Content-Type", "application/octet-stream")

	// Create k-anonymity tester
	var evaluater *kAno.AnoTester
	if isEval {
		evaluater = new(kAno.AnoTester)
		evaluater.New(len(header), 2)
	}

	// Transform header data to csv format
	buffer := transformToCsvFormat(header)
	res.Write(buffer.Bytes())
	// Export process
	for row, ok := <-aDataQueue; ok; row, ok = <-aDataQueue {
		// Add data to evaluate k-anonymity
		if isEval {
			evaluater.AddStrings(row)
		}
		// Transform exported data and write data
		buffer.Reset()
		buffer = transformToCsvFormat(row)
		res.Write(buffer.Bytes())
	}

	// Evaluate k-anonymity
	evaluation := model.Evaluation{
		ApiName: name,
		Result:  "none",
		Value:   int64(0),
	}
	if isEval {
		evalResult, actValue := evaluater.Eval()
		evaluation.Result = strconv.FormatBool(evalResult)
		evaluation.Value = int64(actValue)
	}

	// Exit
	quitProce <- evaluation
	evaluater = nil
}

func writeExportedDataOnLambda(ctx context.Context, tracking bool, res *events.APIGatewayProxyResponse, name string, header []string, isEval bool, aDataQueue <-chan []string, quitProce chan<- model.Evaluation) {
	// Set the subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Write data in response body")
		defer subSegment.Close(nil)
	}

	// Create k-anonymity tester
	var evaluater *kAno.AnoTester
	if isEval {
		evaluater = new(kAno.AnoTester)
		evaluater.New(len(header), 2)
	}

	// Set body
	var body bytes.Buffer

	// Transform header data to csv format
	lineCount := int64(0)
	buffer := transformToCsvFormat(header)
	body.Write(buffer.Bytes())
	// Export process
	for row, ok := <-aDataQueue; ok; row, ok = <-aDataQueue {
		// Add data to evaluate k-anonymity
		if isEval {
			evaluater.AddStrings(row)
		}
		// Transform exported data and write data
		buffer.Reset()
		buffer = transformToCsvFormat(row)
		body.Write(buffer.Bytes())
		lineCount++
	}
	// Write response body
	res.Body = body.String()
	body.Reset()

	// Evaluate k-anonymity
	evaluation := model.Evaluation{
		ApiName: name,
		Result:  "none",
		Value:   int64(0),
	}
	if isEval {
		evalResult, actValue := evaluater.Eval()
		evaluation.Result = strconv.FormatBool(evalResult)
		evaluation.Value = int64(actValue)
	}

	// Exit
	quitProce <- evaluation
	evaluater = nil
}

// func allocateMemoryByScanType(columns []*sql.ColumnType) []interface{} {
// 	allocated := make([]interface{}, len(columns))
// 	for i, column := range columns {
// 		if column.ScanType() == nil {
// 			allocated[i] = new(string)
// 		} else {
// 			switch column.ScanType().String() {
// 			case "int", "int8", "int16", "int32", "int64":
// 				allocated[i] = new(int64)
// 			case "uint", "uint8", "uint16", "uint32", "uint64":
// 				allocated[i] = new(uint64)
// 			case "float32", "float64":
// 				allocated[i] = new(float64)
// 			case "bool":
// 				allocated[i] = new(bool)
// 			case "string":
// 				allocated[i] = new(string)
// 			case "time.time":
// 				allocated[i] = new(time.Time)
// 			case "sql.RawBytes", "slice":
// 				allocated[i] = new([]byte)
// 			// case "driver.Decimal":
// 			//  temp[i] = new(driver.Decimal)
// 			default:
// 				//  log.Print("New type: ", column.ScanType().String())
// 				allocated[i] = new(interface{})
// 			}
// 		}
// 	}
// 	return allocated
// }

func transformToString(scanType string, elem interface{}) string {
	var converted string
	switch scanType {
	case "int", "int8", "int16", "int32", "int64":
		converted = strconv.FormatInt(elem.(int64), 10)
	case "uint", "uint8", "uint16", "uint32", "uint64", "uintptr":
		converted = strconv.FormatUint(elem.(uint64), 10)
	case "float32", "float64":
		converted = strconv.FormatFloat(elem.(float64), 'f', -6, 64)
	// case "complex32", "complex64", "complex128":
	// 	converted = strconv.FormatComplex(elem.(complex128), [])
	case "bool":
		converted = strconv.FormatBool(elem.(bool))
	case "string":
		converted = (elem.(string))
	case "time.time":
		converted = (elem.(time.Time).Format("2006-01-02T15:04:05"))
	case "sql.RawBytes", "slice":
		converted = string(elem.([]byte))
	// case "driver.Decimal":
	//  converted[i] = big.NewFloat(0).SetRat((*big.Rat)(elem.(*driver.Decimal))).String()
	default:
		converted = "-/-"
	}
	return converted
}
func transformToCsvFormat(data []string) bytes.Buffer {
	var buffer bytes.Buffer
	for index, value := range data {
		if strings.ContainsAny(value, ",") {
			buffer.WriteString("\"")
			buffer.WriteString(value)
			buffer.WriteString("\"")
		} else {
			buffer.WriteString(value)
		}
		// If not the last elem data, add a comma
		if index < len(data)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("\r\n")
	return buffer
}
