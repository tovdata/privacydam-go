// PrivacyDAM에 의해 API 생성 과정에서 필요한 함수들 및 생성된 API를 처리하는 함수들이 정의된 패키지
package process

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	// AWS
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-xray-sdk-go/xray"

	// Echo framework
	"github.com/labstack/echo/v4"

	// Model
	"github.com/tovdata/privacydam-go/core/model"

	// PrivacyDAM package
	"github.com/tovdata/privacydam-go/core"
	"github.com/tovdata/privacydam-go/core/util"
	"github.com/tovdata/privacydam-go/process/util/auth"
	"github.com/tovdata/privacydam-go/process/util/db"
)

// Source(외부 데이터베이스)를 등록하기 전에 연결에 대한 테스트를 수행하는 함수입니다.
func TestConnection(ctx context.Context, source model.Source) error {
	return db.Ex_testConnection(ctx, source.Type, source.RealDsn)
}

// HTTP 요청 URL로부터 API 별칭을 추출하는 함수입니다. (For echo framework)
//	# Parameters
//	key (string): URL key name (/:<key>)
//
//	# Response
//	(string): extracted value (= API alias)
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

// HTTP 요청 URL로부터 API 별칭을 추출하는 함수입니다. (For aws lambda)
//	# Parameters
//	request (events.APIGatewayProxyRequest): AWS API Gateway proxy request
//	key (string): URL key name (/:<key>)
//
//	# Response
//	(string): extracted value (= API alias)
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

// API 별칭의 형식을 검증하는 함수입니다.
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

// 내부 데이터베이스로부터 API의 정보를 가져오는 함수입니다.
//	# Parameters
//	param (string): condition to find API (ex. API alias)
func GetApiInformationFromDB(ctx context.Context, param string) (model.Api, error) {
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
	return db.In_findApiFromDB(subCtx, param)
}

// 캐싱된 데이터로부터 API의 정보를 가져오는 함수입니다.
//	# Parameters
//	param (string): condition to find API (ex. API alias)
func GetApiInformation(ctx context.Context, param string) (model.Api, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subSegment *xray.Segment
	if tracking {
		_, subSegment = xray.BeginSubsegment(ctx, "Find API information")
		defer subSegment.Close(nil)
	}

	// Find API using param
	return db.In_findApi(param)
}

// API의 만료일을 검증하는 함수입니다.
//	# Parameters
//	data (string): API expiration date [format: YYYY-MM-DD HH:mm:ss]
//	status (string): API activation status ['active' or 'disabled']
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

// API의 파라미터 값을 검증하는 함수입니다. API의 파라미터는 Key:Value 형식으로 이루어져 있으며, HTTP 요청에 포함된 파라미터 데이터의 Key값과 API의 파라미터의 Key 값을 비교하여 데이터를 검증하고 추출된 파라미터들을 반환합니다. (For echo framework)
//	# Parameters
//	key ([]string): a list of API parameter key
//
//	# Response
//	([]interface{}): a list of parameter value extracted from HTTP request
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

// API의 파라미터 값을 검증하는 함수입니다. API의 파라미터는 Key:Value 형식으로 이루어져 있으며, HTTP 요청에 포함된 파라미터 데이터의 Key값과 API의 파라미터의 Key 값을 비교하여 데이터를 검증하고 추출된 파라미터들을 반환합니다. (For aws lambda)
//	# Parameters
//	req (events.APIGatewayProxyRequest): AWS API Gateway proxy request
//	key ([]string): a list of API parameter key
//
//	# Response
//	([]interface{}): a list of parameter value extracted from HTTP request
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

// 내부 데이터베이스로부터 API의 비식별 옵션을 가져오는 함수입니다.
//	# Parameters
//	id (string): API uuid by generated database
func GetDeIdentificationOptionsFromDB(ctx context.Context, id string) (map[string]model.AnoParamOption, error) {
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
	rawOptions, err := db.In_getDeIdentificationOptionsFromDB(subCtx, id)
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
 * Transform to de-identification options
 * <IN> ctx (context.Context): context
 * <IN> rawDidOptions (string): raw de-identification options (string type)
 * <OUT> (map[string]model.AnoParamOption): de-identification options
 * <OUT> (error): error object (contain nil)
 */

// AnoParamOption 형태(JSON)의 문자열 데이터를 map 형태로 변환하는 함수입니다. 문자열로 저장되어 있는 비식별 옵션 데이터를 map으로 변환하여 사용하기 위해서 호출됩니다.
//	# Parameters
//	rawDidOptions (string): string of JSON format
func TransformDeIdentificationOptions(ctx context.Context, rawDidOptions string) (map[string]model.AnoParamOption, error) {
	// Get tracking status
	tracking := util.GetTrackingStatus("processing")

	// [For debug] set subsegment
	var subSegment *xray.Segment
	if tracking {
		_, subSegment = xray.BeginSubsegment(ctx, "Load de-identification options")
		defer subSegment.Close(nil)
	}

	// Transformation
	return core.TransformToDidOptions(rawDidOptions)
}

// API 접근에 대한 인증을 하는 함수입니다. HTTP 요청 Header 내에 Token 값을 OPA 서버로 전달하고 인증에 대한 응답을 받아서 처리합니다. (For echo framework)
//	# Parameters
//	opaUrl (string): OPA URL [format: <host>:<port>/<path>]
func AuthenticateAccessOnEcho(ctx echo.Context, opaUrl string) error {
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
	return auth.AuthenticateAccess(subCtx, tracking, opaUrl, token)
}

// API 접근에 대한 인증을 하는 함수입니다. HTTP 요청 Header 내에 Token 값을 OPA 서버로 전달하고 인증에 대한 응답을 받아서 처리합니다. (For aws lambda)
//	# Parameters
//	req (events.APIGatewayProxyRequest): AWS API Gateway proxy request
//	opaUrl (string): OPA URL [format: <host>:<port>/<path>]
func AuthenticateAccessOnLambda(ctx context.Context, req events.APIGatewayProxyRequest, opaUrl string) error {
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
	return auth.AuthenticateAccess(subCtx, tracking, opaUrl, token)
}

// API Name를 생성하는 함수로써 Timestamp를 이용하여 API의 고유한 이름을 생성합니다.
//	# Parameters
//	isTemp (bool): temparary status
//
//	# Response
//	(string): created API name
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

// 데이터 반출 처리를 수행하는 함수입니다. (For echo framework)
//	# Parameters
//	res (http.ResponseWriter): writer for reponse
//	api (model.Api): API information object for generation
//
//	# Response
//	(model.Evaluation): K-anonymity evaluation result
func ExportDataOnServer(ctx context.Context, res http.ResponseWriter, api model.Api) (model.Evaluation, error) {
	// Get routinCount
	routineCount := core.GetRoutineCount()
	if routineCount == 0 {
		return model.Evaluation{}, errors.New("Invaild routine count\r\n")
	}

	// Check api name
	name := api.Name
	if api.Name == "" {
		name = CreateApiName(true)
	}
	// Processing
	return db.Ex_exportData(ctx, res, routineCount, name, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, api.QueryContent.DidOptions)
}

// 데이터 반출 처리를 수행하는 함수입니다. (For aws lambda)
//	# Parameters
//	res (*events.APIGatewayProxyResponse): writer for reponse (AWS API Gateway proxy response)
//	api (model.Api): API information object for generation
//
//	# Response
//	(model.Evaluation): K-anonymity evaluation result
func ExportDataOnLambda(ctx context.Context, res *events.APIGatewayProxyResponse, api model.Api) (model.Evaluation, error) {
	// Get routinCount
	routineCount := core.GetRoutineCount()
	if routineCount == 0 {
		return model.Evaluation{}, errors.New("Invaild routine count\r\n")
	}

	// Check api name
	name := api.Name
	if api.Name == "" {
		name = CreateApiName(true)
	}
	// Processing
	return db.Ex_exportDataOnLambda(ctx, res, routineCount, name, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, api.QueryContent.DidOptions)
}

// 데이터 수정(Insert, Update, Delete)에 대한 처리를 수행하는 함수입니다.
//	# Parameters
//	api (model.Api): API information object for generation
//	isTest (bool): test or not
//
//	# Response
//	(int64): affected row count by query
func ChangeData(ctx context.Context, api model.Api, isTest bool) (int64, error) {
	return db.Ex_changeData(ctx, api.SourceId, api.QueryContent.Syntax, api.QueryContent.ParamsValue, isTest)
}

// API에 접근한 사용자의 정보를 추출하는 함수입니다. 접속 IP, UserAgent를 추출합니다.
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
