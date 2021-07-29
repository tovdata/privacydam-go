// 인증 관련 패키지
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	// AWS
	"github.com/aws/aws-lambda-go/events"

	// Echo framwork
	echo "github.com/labstack/echo/v4"
)

// HTTP 요청 내의 Header로부터 Access Token를 추출하는 함수입니다. (For echo framework)
//	# Response
//	(string): extracted access token from HTTP request
func ExtractAccessTokenOnEcho(ctx echo.Context) (string, error) {
	// Select request header attribute
	headerData := ctx.Request().Header.Get("Authorization")
	if headerData != "" {
		strArr := strings.Split(headerData, " ")
		if len(strArr) == 2 {
			return strArr[1], nil
		}
	} else {
		// Select cookie
		cookie, err := ctx.Cookie("access-token")
		if err == nil && cookie != nil && cookie.Value != " " {
			return cookie.Value, nil
		}
	}
	return "", errors.New("Authentication failed (access token not found)\r\n")
}

// HTTP 요청 내의 Header로부터 Access Token를 추출하는 함수입니다. (For aws lambda)
//	# Parameters
//	req (events.APIGatewayProxyRequest): AWS API Gateway proxy request
//
//	# Response
//	(string): extracted access token from HTTP request
func ExtractAccessTokenOnLambda(ctx context.Context, req events.APIGatewayProxyRequest) (string, error) {
	// Select request header attribute
	if value, ok := req.Headers["Authorization"]; ok {
		strArr := strings.Split(value, " ")
		if len(strArr) == 2 {
			return strArr[1], nil
		}
	}
	return "", errors.New("Authentication failed (access token not found)\r\n")
}

// API에 대한 접근을 인증하는 함수로써 추출된 Access Token를 OPA server로 전달하고 응답을 받아 API에 대한 접근을 제어합니다.
//	# Parameters
//	traking (bool): process tracking status (using AWS X-Ray / need AWS X-Ray configuration)
//	opaUrl (string): OPA URL [format: <host>:<port>/<path>]
//	token (string): access token
func AuthenticateAccess(ctx context.Context, tracking bool, opaUrl string, token string) error {
	var request *http.Request
	var err error
	// Create request object (to OPA server)
	if tracking {
		request, err = http.NewRequestWithContext(ctx, "GET", opaUrl, nil)
	} else {
		request, err = http.NewRequest("GET", opaUrl, nil)
	}
	// Set connection close
	request.Header.Add("Connection", "close")
	// Catch error
	if err != nil {
		return err
	}

	// Create authorization attribute value
	var buffer bytes.Buffer
	buffer.WriteString("bearer ")
	buffer.WriteString(token)
	// Add data in request header
	request.Header.Add("authorization", buffer.String())

	// Create client for execute request
	client := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	// Execute request
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	defer response.Body.Close()

	// Read body data
	result, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	// Transform to map
	var data map[string]string
	if err := json.Unmarshal(result, &data); err != nil {
		return err
	} else if value, ok := data["allow"]; ok {
		// Verify authentication
		if value == "true" {
			return nil
		} else {
			return errors.New("Unauthentication\r\n")
		}
	}
	return errors.New("Authentication process error\r\n")
}
