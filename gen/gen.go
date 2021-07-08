// PrivacyDAM에서 사용할 API와 Source를 생성하는 패키지
package gen

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	// AWS

	// Model
	"github.com/tovdata/privacydam-go/core/model"
	// Util
	"github.com/tovdata/privacydam-go/core/db"
)

// Api를 생성하는 함수입니다.
func GenerateApi(ctx context.Context, api model.Api) error {
	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	// Begin transaction
	tx, err := dbInfo.Instance.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute query (insert API information)
	var result sql.Result
	querySyntax := `INSERT INTO api (source_id, api_name, api_alias, api_type, syntax, exp_date) VALUE (?, ?, ?, ?, ?, ?)`
	if dbInfo.Tracking {
		result, err = tx.ExecContext(ctx, querySyntax, api.SourceId, api.Name, api.Alias, api.Type, api.QueryContent.Syntax, api.ExpDate)
	} else {
		result, err = tx.Exec(querySyntax, api.SourceId, api.Name, api.Alias, api.Type, api.QueryContent.Syntax, api.ExpDate)
	}
	// Catch error
	if err != nil {
		return err
	}
	// Extract inserted id
	insertedId, err := result.LastInsertId()
	if err != nil {
		return err
	}

	if len(api.QueryContent.ParamsKey) > 0 {
		// Prepare query (insert API parameters)
		var stmt *sql.Stmt
		querySyntax = `INSERT INTO parameter (api_id, parameter_key) VALUE (?, ?)`
		if dbInfo.Tracking {
			stmt, err = tx.PrepareContext(ctx, querySyntax)
		} else {
			stmt, err = tx.Prepare(querySyntax)
		}
		// Catch error
		if err != nil {
			return err
		}

		// Execute query (insert API parameters)
		for _, param := range api.QueryContent.ParamsKey {
			var err error
			if dbInfo.Tracking {
				_, err = stmt.ExecContext(ctx, insertedId, param)
			} else {
				_, err = stmt.Exec(insertedId, param)
			}
			// Catch error
			if err != nil {
				return err
			}
		}
	}

	if api.QueryContent.RawDidOptions.Valid && api.QueryContent.RawDidOptions.String != "" {
		// Execute query (insert de-identification options)
		var err error
		querySyntax := `INSERT INTO did_option (api_id, options) VALUE (?, ?)`
		if dbInfo.Tracking {
			_, err = tx.ExecContext(ctx, querySyntax, insertedId, api.QueryContent.RawDidOptions)
		} else {
			_, err = tx.Exec(querySyntax, insertedId, api.QueryContent.RawDidOptions)
		}
		// Catch error
		if err != nil {
			return err
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	} else {
		return nil
	}
}

// Api 별칭에 대한 중복을 확인하는 함수입니다.
func DuplicateCheckForAlias(ctx context.Context, alias string) error {
	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	// Execute query
	var result string
	querySyntax := `SELECT COUNT(*) FROM api WHERE api_alias=?`
	if dbInfo.Tracking {
		err = dbInfo.Instance.QueryRowContext(ctx, querySyntax, alias).Scan(&result)
	} else {
		err = dbInfo.Instance.QueryRow(querySyntax, alias).Scan(&result)
	}
	// Catch error
	if err != nil {
		return err
	}

	// Verify
	count, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		return err
	} else if count > int64(0) {
		return errors.New("Alias that already exist")
	} else {
		return nil
	}
}

// Source(외부 데이터베이스)를 생성하는 함수입니다.
func GenerateSource(ctx context.Context, source model.Source) error {
	// Get database object
	dbInfo, err := db.GetDatabase("internal", nil)
	if err != nil {
		return err
	}

	var result sql.Result
	// Execute query (insert source)
	querySyntax := `INSERT INTO source (source_category, source_type, source_name, real_dsn, fake_dsn) VALUE (:source_category, :source_type, :source_name, :real_dsn, :fake_dsn)`
	if dbInfo.Tracking {
		result, err = dbInfo.Instance.NamedExecContext(ctx, querySyntax, source)
	} else {
		result, err = dbInfo.Instance.NamedExec(querySyntax, source)
	}
	// Catch error
	if err != nil {
		return err
	} else {
		// Get inserted id
		insertId, err := result.LastInsertId()
		if err != nil {
			return err
		}
		// Set source uuid
		source.Uuid = strconv.FormatInt(insertId, 10)
		// Add database connection pool
		return db.CreateConnectionPool(ctx, source, true)
	}
}
