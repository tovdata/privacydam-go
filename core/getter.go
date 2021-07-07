package core

import (
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
	RoutineCount = 2
)

func EmptyEvaluation() model.Evaluation {
	return model.Evaluation{}
}

func EmptyApi() model.Api {
	return model.Api{}
}

func EmptySource() model.Source {
	return model.Source{}
}

func TransformToApi(rawData interface{}) *model.Api {
	return rawData.(*model.Api)
}

func TransformToEvaluation(rawData interface{}) *model.Evaluation {
	return rawData.(*model.Evaluation)
}

func TransformToDidOptions(rawOptions string) (map[string]model.AnoParamOption, error) {
	// Set default de-identification options
	var didOptions map[string]model.AnoParamOption
	// Transform to structure
	if err := json.Unmarshal([]byte(rawOptions), &didOptions); err != nil {
		return didOptions, err
	} else {
		return didOptions, nil
	}
}

func GetInternalDatabase() (model.ConnInfo, error) {
	return db.GetDatabase("internal", nil)
}

func GetExternalDatabase(key interface{}) (model.ConnInfo, error) {
	return db.GetDatabase("external", key)
}

func GetApiList() map[string]model.Api {
	return apis
}

func SetRoutineCount() {
	// Set default go-routine count (min count: 4)
	RoutineCount = runtime.NumCPU()
	if RoutineCount < 4 {
		RoutineCount = 4
	}
	runtime.GOMAXPROCS(RoutineCount * 2)
}

func GetRoutineCount() int {
	return RoutineCount
}
