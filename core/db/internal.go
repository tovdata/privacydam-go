package db

import (
	"context"

	"github.com/jmoiron/sqlx"

	// Model
	"privacydam-go/core/model"
)

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

	// Execute query result
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
