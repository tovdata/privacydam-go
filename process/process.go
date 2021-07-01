package process

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"

	// AWS
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"privacydam-go/core/model"

	// PrivacyDAM package
	"privacydam-go/core"
	"privacydam-go/core/util"
	"privacydam-go/process/util/auth"
	"privacydam-go/process/util/db"
)

// func ProcessTestInEcho(ctx echo.Context) error {
// 	// Set context
// 	cCtx := ctx.Request().Context()

// 	// Get API information
// 	api, err := GetApiInformation(cCtx, "a_marketing_01")
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	// Verify API expires
// 	if err := VerifyExpires(cCtx, api.ExpDate, api.Status); err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	// Get query parameters and verify parameters
// 	// params := make([]interface{}, 0)
// 	params := []interface{}{"10"}

// 	didOptions, err := GetDeIdentificationOptions(cCtx, api.Uuid)
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	}

// 	_, err = db.Ex_exportData(cCtx, ctx.Response(), api, params, didOptions)
// 	if err != nil {
// 		return res.SendMessage(ctx, "error", err.Error())
// 	} else {
// 		return nil
// 	}
// }

/*
 * Test connection
 * <IN> ctx (context.Context): context
 * <IN> source (mode.Source): source object
 * <OUT> (error): error object (contain nil)
 */
func TestConnection(ctx context.Context, source model.Source) error {
	return db.Ex_testConnection(ctx, source.Type, source.RealDsn)
}

/*
 * Extract API alias from request path and verify alias format (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> key (string): key to found api alias from request path
 * <OUT> (string): extracted value (= API alias)
 * <OUT> (error): error object (contain nil)
 */
func ExtractApiAliasOnEcho(ctx echo.Context, key string) (string, error) {
	// Extract
	value := ctx.Param(key)
	// Verify API alias format
	if value != "" {
		err := VerifyApiAliasFormat(value)
		return value, err
	} else {
		return value, errors.New("Not found request path parameter")
	}
}

/*
 * Extract API alias from request path and verify alias format (on AWS lambda)
 * <IN> ctx (context.Context): context
 * <IN> key (string): key to found api alias from request path
 * <OUT> (string): extracted value (= API alias)
 * <OUT> (error): error object (contain nil)
 */
func ExtractApiAliasOnLambda(ctx context.Context, request events.APIGatewayProxyRequest, key string) (string, error) {
	// Extract
	if value, ok := request.PathParameters[key]; ok {
		// Verify
		err := VerifyApiAliasFormat(value)
		return value, err
	} else {
		return value, errors.New("Not found request path parameter")
	}
}

/*
 * Verify API alias format
 * <IN> alias (string): alias value to verify
 * <OUT> (error): error object (contain nil)
 */
func VerifyApiAliasFormat(alias string) error {
	// Verify
	match, err := regexp.MatchString("^a_+", alias)
	if err != nil {
		return err
	} else if !match {
		return errors.New("Invalid API alias format")
	} else {
		return nil
	}
}

/*
 * Get API information
 * <IN> ctx (context.Context): context
 * <IN> param (string): condition to find API (api_alias)
 * <OUT> (model.Api): api information format
 * <OUT> (error): error object (contain nil)
 */
func GetApiInformation(ctx context.Context, param string) (model.Api, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Find API information")
		defer subSegment.Close(nil)
	}

	// Find API using param
	return db.In_findApi(subCtx, param)
}

/*
 * Verify API expires
 * <IN> ctx (context.Context): context
 * <IN> date (string): api expiration date (mysql datatime format)
 * <IN> status (string): api activation status ('active' or 'disabled')
 * <OUT> (error): error object (contain nil)
 */
func VerifyExpires(ctx context.Context, date string, status string) error {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Verify API Expires")
		defer subSegment.Close(nil)
	}

	// Verify API expires
	expDate, err := time.Parse("2006-01-02 15:04:05", date)
	if expDate.Before(time.Now()) {
		err = errors.New("This API has expired")
	} else if status == "disabled" {
		err = errors.New("This API is not avaliable")
	}
	return err
}

/*
 * Verify API parameters (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> keys ([]string): a list of parameter key
 * <OUT> ([]interface{}): a list of parameter value extracted from the request
 * <OUT> (error): error object (contain nil)
 */
func VerifyParametersOnEcho(ctx echo.Context, keys []string) ([]interface{}, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx.Request().Context(), "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Get parameters from request body
	params := make([]interface{}, 0)
	for _, key := range keys {
		value := ctx.QueryParam(key)
		if value == "" {
			return params, errors.New("Invalid parameters")
		} else {
			params = append(params, value)
		}
	}
	// Verify parameters
	err := verifyParameters(params, keys)
	return params, err
}

/*
 * Verify API parameters (on AWS lambda)
 * <IN> ctx (context.Context): context
 * <IN> req (events.APIGatewayProxyRequest): request object (for AWS APIGateway proxy, lambda)
 * <IN> keys ([]string): a list of parameter key
 * <OUT> ([]interface{}): a list of parameter value extracted from the request
 * <OUT> (error): error object (contain nil)
 */
func VerifyParametersOnLambda(ctx context.Context, req events.APIGatewayProxyRequest, keys []string) ([]interface{}, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	if tracking {
		_, subSegment := xray.BeginSubsegment(ctx, "Verify API parameters")
		defer subSegment.Close(nil)
	}

	// Get parameters from request body
	params := make([]interface{}, 0)
	for _, key := range keys {
		if value, ok := req.QueryStringParameters[key]; ok {
			params = append(params, value)
		} else {
			return params, errors.New("Invalid parameters")
		}
	}
	// Verify parameters
	err := verifyParameters(params, keys)
	return params, err
}

func verifyParameters(standard []interface{}, target []string) error {
	if len(standard) != len(target) {
		return errors.New("Invalid paramters")
	} else {
		return nil
	}
}

/*
 * Get a de-identification options
 * <IN> ctx (context.Context): context
 * <IN> id (string): API id by generated database
 * <OUT> (map[string]model.AnoParamOption): de-identification options
 * <OUT> (error): error object (contain nil)
 */
func GetDeIdentificationOptions(ctx context.Context, id string) (map[string]model.AnoParamOption, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Load de-identification options")
		defer subSegment.Close(nil)
	}

	// Set default de-identification options
	var didOptions map[string]model.AnoParamOption

	// Get de-identification options
	rawOptions, err := db.In_getDeIdentificationOptions(subCtx, id)
	if err != nil {
		return didOptions, err
	}
	// Transform to structure
	if rawOptions != "" {
		err = json.Unmarshal([]byte(rawOptions), &didOptions)
		return didOptions, err
	} else {
		return nil, err
	}
}

/*
 * Authenticate access on server (on echo framework)
 * <IN> ctx (echo.Context): context
 * <IN> server (string): OPA server host (contain protocal, host, port)
 * <OUT> (error): authentication result (nil is a successful authentication)
 */
func AuthenticateAccessOnEcho(ctx echo.Context, server string) error {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subCtx context.Context = ctx.Request().Context()
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx.Request().Context(), "Authentication access")
		defer subSegment.Close(nil)
	}

	// Extract access token
	token, err := auth.ExtractAccessTokenOnEcho(ctx)
	if err != nil {
		return err
	}
	// Authenticate access token (using another OPA)
	return auth.AuthenticateAccess(subCtx, tracking, server, token)
}

/*
 * Authenticate access on http (on aws lambda)
 * <IN> ctx (echo.Context): context
 * <IN> req (events.APIGatewayProxyRequest): apigateway proxy request
 * <IN> server (string): OPA server host (contain protocal, host, port)
 * <OUT> (error): authentication result (nil is a successful authentication)
 */
func AuthenticateAccessOnLambda(ctx context.Context, req events.APIGatewayProxyRequest, server string) error {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subCtx context.Context = ctx
	var subSegment *xray.Segment
	if tracking {
		subCtx, subSegment = xray.BeginSubsegment(ctx, "Authentication access")
		defer subSegment.Close(nil)
	}

	// Extract access token
	token, err := auth.ExtractAccessTokenOnLambda(ctx, req)
	if err != nil {
		return err
	}
	// Authenticate access token (using another OPA)
	return auth.AuthenticateAccess(subCtx, tracking, server, token)
}

/*
 * Create API name (timestamp)
 * <IN> isTemp (bool): is temparary name?
 * <OUT> (string): api name
 */
func CreateApiName(isTemp bool) string {
	// Create Api name
	name := strconv.FormatInt(time.Now().Unix(), 10)
	// Return by temp
	if isTemp {
		return "temp_" + name
	} else {
		return name
	}
}

/*
 * Export data (process for export API)
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> res (http.ResponseWriter): responseWriter object
 * <IN> apiName (string): api name
 * <IN> sourceId (string): api source id by generated database
 * <IN> querySyntax (string) syntax to query
 * <IN> params ([]interface{}): parameters to query
 * <IN> didOptions (map[string]model.AnoParamOption): de-identification options
 * <OUT> (model.Evaluation): k-anonymity evaluation result
 * <OUT> (error): error object (contain nil)
 */
func ExportDataOnServer(ctx context.Context, res http.ResponseWriter, api model.Api) (model.Evaluation, error) {
	// Check api name
	name := api.Name
	if api.Name == "" {
		name = CreateApiName(true)
	}
	// Processing
	return db.Ex_exportData(ctx, res, name, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, api.QueryContent.DidOptions)
}

/*
 * Export data (process for export API)
 * <IN> ctx (context.Context): context
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> res (http.ResponseWriter): responseWriter object
 * <IN> apiName (string): api name
 * <IN> sourceId (string): api source id by generated database
 * <IN> querySyntax (string) syntax to query
 * <IN> params ([]interface{}): parameters to query
 * <IN> didOptions (map[string]model.AnoParamOption): de-identification options
 * <OUT> (model.Evaluation): k-anonymity evaluation result
 * <OUT> (error): error object (contain nil)
 */
func ExportDataOnLambda(ctx context.Context, res *events.APIGatewayProxyResponse, api model.Api) (model.Evaluation, error) {
	// Check api name
	name := api.Name
	if api.Name == "" {
		name = CreateApiName(true)
	}
	// Processing
	return db.Ex_exportDataOnLambda(ctx, res, name, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, api.QueryContent.DidOptions)
}

/*
 * Change data (process for control API)
 * <IN> ctx (context.Context): context
 * <IN> sourceId (string): api source id by generated database
 * <IN> querySyntax (string): syntax to query
 * <IN> params ([]interface{}): parameters to query
 * <IN> isTest (bool): test or not
 * <OUT> (int64): affected row count by query
 * <OUT> (error): error object (contain nil)
 */
func ChangeData(ctx context.Context, api model.Api, isTest bool) (int64, error) {
	return db.Ex_changeData(ctx, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, isTest)
}

// func WriteProcessLogInDB(ctx context.Context, accessor model.Accessor, apiId string, apiType string, evaluation model.Evaluation, finalResult string) error {
// 	return db.In_writeProcessLog(ctx, accessor, apiId, apiType, evaluation, finalResult)
// }

func WriteProcessLog(ctx context.Context, accessor model.Accessor, api model.Api, evaluation model.Evaluation, finalResult string) error {
	// Get sqs url
	queueUrl := os.Getenv("AWS_SQS_URL")

	// Extract current timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// Get database information
	dbInfo, err := core.GetExternalDatabase(api.SourceId)
	if err != nil {
		return err
	}

	// Extract parameter values
	var queryParams string
	for i, value := range api.QueryContent.ParamsValue {
		queryParams += value.(string)
		if i < len(api.QueryContent.ParamsValue)-1 {
			queryParams += ","
		}
	}

	// Set the parameters
	params := &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueUrl),
		MessageGroupId: aws.String("privacydam_process"),
		MessageBody:    aws.String("processtime: " + timestamp),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"timestamp":          createSqsMessageAttributeValue(timestamp),
			"remote_ip":          createSqsMessageAttributeValue(accessor.Ip),
			"user_agent":         createSqsMessageAttributeValue(accessor.UserAgent),
			"dsn":                createSqsMessageAttributeValue(dbInfo.Dsn),
			"syntax":             createSqsMessageAttributeValue(api.QueryContent.Syntax),
			"params":             createSqsMessageAttributeValue(queryParams),
			"k_ano_result_pass":  createSqsMessageAttributeValue(evaluation.Result),
			"k_ano_result_value": createSqsMessageAttributeValue(strconv.FormatInt(evaluation.Value, 10)),
			"final_result":       createSqsMessageAttributeValue(finalResult),
		},
	}

	// Get sqs client
	sqsClient, err := core.GetSqsClient()
	if err != nil {
		return nil
	}
	// Receive message
	if _, err := sqsClient.SendMessage(ctx, params); err != nil {
		return err
	} else {
		return nil
	}
}

func createSqsMessageAttributeValue(value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}

func GetAccessorOnServer(ctx echo.Context) model.Accessor {
	// Define accessor struct
	var accessor model.Accessor
	// Extract user agent
	accessor.UserAgent = ctx.Request().UserAgent()
	// Extract ip address (Proxy server)
	for _, header := range []string{"X-Forward-For", "X-Real-Ip"} {
		addresses := strings.Split(ctx.Request().Header.Get(header), ",")
		// March from right to left until we get a public address
		// that will be the address right before our proxy.
		for index := len(addresses) - 1; index >= 0; index-- {
			ip := strings.TrimSpace(addresses[index])
			// Header can contain spaces too, strip those out.
			realIP := net.ParseIP(ip)
			if !realIP.IsGlobalUnicast() {
				continue
			} else {
				accessor.Ip = ip
				return accessor
			}
		}
	}
	// Extract ip address (Direct)
	accessor.Ip = ctx.Request().RemoteAddr
	return accessor
}
