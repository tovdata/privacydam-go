package db

import (
	"context"
	"database/sql"
	"errors"

	// ORM
	"github.com/jmoiron/sqlx"

	// Model
	"github.com/tovdata/privacydam-go/core/model"

	// PrivacyDAM package
	core "github.com/tovdata/privacydam-go/core"

	// Core (database pool)
	coreDB "github.com/tovdata/privacydam-go/core/db"
)

// 내부 데이터베이스로부터 API의 정보를 가져오는 함수입니다.
//	# Parameters
//	param (string): value to find API (= API alias)
func In_findApiFromDB(ctx context.Context, param string) (model.Api, error) {
	// Create api structure
	info := model.Api{
		QueryContent: model.QueryContent{},
	}

	// Get database object
	dbInfo, err := coreDB.GetDatabase("internal", nil)
	if err != nil {
		return info, err
	}

	// Execute query (get a api information)
	var rows *sqlx.Rows
	querySyntax := `SELECT api_id, source_id, api_name, api_alias, api_type, syntax "queryContent.syntax", reg_date, exp_date, status FROM api WHERE api_alias=?`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(ctx, querySyntax, param)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax, param)
	}
	// Catch error
	if err != nil {
		return info, err
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		if err := rows.StructScan(&info); err != nil {
			return info, err
		}
	}
	// Catch error
	if err := rows.Err(); err != nil {
		return info, err
	} else if info.Uuid == "" {
		return info, errors.New("Not found API (Please check if the API alias is correct)\r\n")
	}

	// Allocate memory to store parameters
	info.QueryContent.ParamsKey = make([]string, 0)
	// Execute query (get a list of parameters)
	querySyntax = `SELECT p.parameter_key FROM api AS a INNER JOIN parameter AS p ON a.api_id=p.api_id WHERE a.api_id=?`
	if dbInfo.Tracking {
		err = dbInfo.Instance.SelectContext(ctx, &info.QueryContent.ParamsKey, querySyntax, info.Uuid)
	} else {
		err = dbInfo.Instance.Select(&info.QueryContent.ParamsKey, querySyntax, info.Uuid)
	}
	return info, err
}

// 내부 데이터베이스로부터 API의 비식별 옵션(Raw data)을 가져오는 함수입니다.
//	# Parameters
//	id (string): API uuid by generated database
func In_getDeIdentificationOptionsFromDB(ctx context.Context, id string) (string, error) {
	// Set default return value
	var options string

	// Get database object
	dbInfo, err := coreDB.GetDatabase("internal", nil)
	if err != nil {
		return options, err
	}

	// Execute query (get a de-identificaion options)
	var rows *sql.Rows
	querySyntax := `SELECT options FROM did_option WHERE api_id=?`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryContext(ctx, querySyntax, id)
	} else {
		rows, err = dbInfo.Instance.Query(querySyntax, id)
	}
	// Catch error
	if err != nil {
		return options, err
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		if err := rows.Scan(&options); err != nil {
			return options, err
		}
	}

	// Return
	return options, rows.Err()
}

// 캐싱된 API 목록으로부터 API의 정보를 가져오는 함수입니다.
//	# Parameters
//	param (string): value to find API (= API alias)
func In_findApi(param string) (model.Api, error) {
	// Lock
	core.Mutex.Lock()
	// Get a list of api
	apis := core.GetApiList()
	// Unlcok
	core.Mutex.Unlock()

	// Find api
	if data, ok := apis[param]; ok {
		return data, nil
	} else {
		return model.Api{}, errors.New("Not found API (Please check if the API alias is correct)\r\n")
	}
}

// func In_writeProcessLog(ctx context.Context, accessor model.Accessor, apiId string, apiType string, evaluation model.Evaluation, finalResult string) error {
// 	// Get database object
// 	dbInfo, err := coreDB.GetDatabase("internal", nil)
// 	if err != nil {
// 		return err
// 	}

// 	// Execute query (write process log)
// 	if apiType == "export" {
// 		querySyntax := `INSERT INTO process_log (api_id, remote_ip, user_agent, k_ano_result_pass, k_ano_result_value, final_result) VALUE (?, ?, ?, ?, ?, ?)`
// 		if dbInfo.Tracking {
// 			if _, err := dbInfo.Instance.ExecContext(ctx, querySyntax, apiId, accessor.Ip, accessor.UserAgent, evaluation.Result, evaluation.Value, finalResult); err != nil {
// 				return err
// 			}
// 		} else {
// 			if _, err := dbInfo.Instance.Exec(querySyntax, apiId, accessor.Ip, accessor.UserAgent, evaluation.Result, evaluation.Value, finalResult); err != nil {
// 				return err
// 			}
// 		}
// 	} else if apiType == "control" {
// 		querySyntax := `INSERT INTO process_log (api_id, remote_ip, user_agent, final_result) VALUE (?, ?, ?, ?)`
// 		if dbInfo.Tracking {
// 			if _, err := dbInfo.Instance.ExecContext(ctx, querySyntax, apiId, accessor.Ip, accessor.UserAgent, finalResult); err != nil {
// 				return err
// 			}
// 		} else {
// 			if _, err := dbInfo.Instance.Exec(querySyntax, apiId, accessor.Ip, accessor.UserAgent, finalResult); err != nil {
// 				return err
// 			}
// 		}
// 	} else {
// 		return errors.New("Invalid API type")
// 	}
// 	return nil
// }
