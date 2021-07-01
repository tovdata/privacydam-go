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
	"github.com/labstack/echo"
)

/*
 * Extract access token in request header (attribute or cookie) (for echo framework)
 * <IN> ctx (echo.Context): echo context object
 * <OUT> (string): token value
 * <OUT> (error): error object (contain nil)
 */
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
	return "", errors.New("Authentication failed (access token not fount)")
}

/*
 * Extract access token in request header (attribute or cookie) (for aws lambda)
 * <IN> ctx (echo.Context): echo context object
 * <IN> req (events.APIGatewayProxyRequest): apigateway proxy request
 * <OUT> (string): token value
 * <OUT> (error): error object (contain nil)
 */
func ExtractAccessTokenOnLambda(ctx context.Context, req events.APIGatewayProxyRequest) (string, error) {
	// Select request header attribute
	if value, ok := req.Headers["Authorization"]; ok {
		strArr := strings.Split(value, " ")
		if len(strArr) == 2 {
			return strArr[1], nil
		}
	}
	return "", errors.New("Authentication failed (access token not found)")
}

/*
 * Verify access to generated API
 * <IN> ctx (context.Context): context object
 * <IN> tracking (bool): tracking with AWS X-Ray
 * <IN> server (string): OPA server (contain protocal, host, port)
 * <IN> token (string): token value
 * <OUT> (error): authentication result (nil is a successful authentication)
 */
func AuthenticateAccess(ctx context.Context, tracking bool, server string, token string) error {
	var request *http.Request
	var err error
	// Create request object (to OPA server)
	if tracking {
		request, err = http.NewRequestWithContext(ctx, "GET", server, nil)
	} else {
		request, err = http.NewRequest("GET", server, nil)
	}
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
		Timeout: time.Second * 30,
	}
	// Execute request
	response, err := client.Do(request)
	if err != nil {
		return err
	}
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
			return errors.New("Unauthentication")
		}
	}
	return errors.New("Authentication process error")
}
