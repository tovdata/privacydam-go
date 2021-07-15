package db

import (
	"context"

	// ORM
	"github.com/jmoiron/sqlx"

	// Model
	"github.com/tovdata/privacydam-go/core/model"
)

// Source(외부 데이터베이스)의 정보에 대한 목록을 제공하는 함수로써 PrivacyDAM Management server에 등록된 Source만을 가져옵니다.
func In_getDatabaseConnectionList(ctx context.Context) ([]model.Source, error) {
	// Set array
	result := make([]model.Source, 0)

	// Get database object
	dbInfo, err := GetDatabase("internal", nil)
	if err != nil {
		return result, err
	}

	// Execute query
	var rows *sqlx.Rows
	querySyntax := `SELECT source_id, source_category, source_type, source_name, real_dsn, fake_dsn FROM source`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(ctx, querySyntax)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax)
	}
	defer rows.Close()
	// Catch error
	if err != nil {
		return result, err
	}

	// Extarct query result
	for rows.Next() {
		var source model.Source
		if err := rows.StructScan(&source); err != nil {
			return result, err
		}
		// Append
		result = append(result, source)
	}
	// Return
	return result, rows.Err()
}

// PrivacyDAM에 의해 생성된 API의 정보에 대한 목록을 제공하는 함수입니다.
func In_getApiList(ctx context.Context) ([]model.Api, error) {
	// Set array
	result := make([]model.Api, 0)

	// Get database object
	dbInfo, err := GetDatabase("internal", nil)
	if err != nil {
		return result, err
	}

	// Execute query (get a api information)
	var rows *sqlx.Rows
	querySyntax := `SELECT a.api_id, a.source_id, a.api_name, a.api_alias, a.api_type, a.syntax "queryContent.syntax", a.reg_date, a.exp_date, a.status, d.options "queryContent.rawDidOptions" FROM api AS a LEFT JOIN did_option AS d ON a.api_id=d.api_id`
	if dbInfo.Tracking {
		rows, err = dbInfo.Instance.QueryxContext(ctx, querySyntax)
	} else {
		rows, err = dbInfo.Instance.Queryx(querySyntax)
	}
	// Catch error
	if err != nil {
		return result, err
	}
	defer rows.Close()

	// Extract query result
	for rows.Next() {
		api := model.Api{}
		if err := rows.StructScan(&api); err != nil {
			return result, err
		}

		// Allocate memory to store parameters
		api.QueryContent.ParamsKey = make([]string, 0)
		// Execute query (get a list of parameters)
		querySyntax = `SELECT p.parameter_key FROM api AS a INNER JOIN parameter AS p ON a.api_id=p.api_id WHERE a.api_id=?`
		if dbInfo.Tracking {
			err = dbInfo.Instance.SelectContext(ctx, &api.QueryContent.ParamsKey, querySyntax, api.Uuid)
		} else {
			err = dbInfo.Instance.Select(&api.QueryContent.ParamsKey, querySyntax, api.Uuid)
		}

		// Append
		result = append(result, api)
	}
	// Return
	return result, rows.Err()
}
