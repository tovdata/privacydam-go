package core

import (
	"bytes"
	"encoding/json"
	"runtime"
	"sync"

	// Model
	"github.com/tovdata/privacydam-go/core/model"
	// DB
	"github.com/tovdata/privacydam-go/core/db"
)

var (
	Mutex        = &sync.Mutex{}
	RoutineCount int64
)

// 빈 Evaluation 객체를 반환하는 함수입니다.
func EmptyEvaluation() model.Evaluation {
	return model.Evaluation{}
}

// 빈 Api 객체를 반환하는 함수입니다.
func EmptyApi() model.Api {
	return model.Api{}
}

// 빈 Source 객체를 반환하는 함수입니다.
func EmptySource() model.Source {
	return model.Source{}
}

// Interface{}의 RawData를 Api 객체로 변환하는 함수입니다.
func TransformToApi(rawData interface{}) *model.Api {
	return rawData.(*model.Api)
}

// Interface{}의 RawData를 Evaluation 객체로 변환하는 함수입니다.
func TransformToEvaluation(rawData interface{}) *model.Evaluation {
	return rawData.(*model.Evaluation)
}

// AnoParamOption 형태(JSON)의 문자열 데이터를 map 형태로 변환하는 함수입니다. 문자열로 저장되어 있는 비식별 옵션 데이터를 map으로 변환하여 사용하기 위해서 호출됩니다.
func TransformToDidOptions(rawOptions string) (map[string]model.AnoParamOption, error) {
	// Set default de-identification options
	var didOptions map[string]model.AnoParamOption
	// Transform to structure
	if rawOptions == "" {
		return didOptions, nil
	}
	if err := json.Unmarshal([]byte(rawOptions), &didOptions); err != nil {
		return didOptions, err
	} else {
		return didOptions, nil
	}
}

// 내부 데이터베이스에 대한 정보(Connection 포함)를 제공하는 함수입니다.
func GetInternalDatabase() (model.ConnInfo, error) {
	return db.GetDatabase("internal", nil)
}

// 외부 데이터베이스에 대한 정보(Connection 포함)를 제공하는 함수입니다.
//	# Parameters
//	key (interface{}): value to identify (= Source id)
func GetExternalDatabase(key interface{}) (model.ConnInfo, error) {
	return db.GetDatabase("external", key)
}

// API의 정보에 대한 목록을 제공하는 함수입니다.
func GetApiList() map[string]model.Api {
	return apis
}

// Go-routine이 동작할 Core 개수를 설정하는 함수입니다.
func SetRoutineCount(count int64) {
	// Get CPU core count
	cpuCore := runtime.NumCPU()
	// Set max process
	runtime.GOMAXPROCS(cpuCore)

	if count > 0 {
		RoutineCount = count
	} else {
		RoutineCount = 4
	}
}

// Go-routine이 동작할 Core 개수를 반환하는 함수입니다.
func GetRoutineCount() int64 {
	return RoutineCount
}

// Interface{}의 데이터(JSON 형태)를 []byte 데이터로 변환하는 함수입니다. unicode와 같은 데이터를 포함한 데이터를 변환합니다.
func TransformToJSON(obj interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	// Set option
	encoder.SetEscapeHTML(false)
	// Encode
	err := encoder.Encode(obj)
	// Return
	return buffer.Bytes(), err
}
