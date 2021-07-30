// Database connection pool의 생성 및 connection 관리를 위한 패키지
package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"runtime"
	"strconv"
	"time"

	// ORM
	"github.com/jmoiron/sqlx"

	// AWS
	"github.com/aws/aws-xray-sdk-go/xray"

	// Model
	"github.com/tovdata/privacydam-go/core/model"

	// Util
	"github.com/tovdata/privacydam-go/core/logger"
	"github.com/tovdata/privacydam-go/core/util"

	// Driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	INTERNAL_CONN_TIMEOUT = time.Second * 30
	EXTERNAL_CONN_TIMEOUT = time.Second * 30

	INTERNAL_CONN_LIMIT = 4
	EXTERNAL_CONN_LIMIT = 10
)

var (
	gExDB map[string]model.ConnInfo
	gInDB model.ConnInfo

	coreCount int
)

// 기본적인 초기화를 진행하는 함수로써 Go-routine을 사용할 CPU core의 개수를 설정하고, 내부 또는 외부에서 사용할 데이터베이스의 초기화 작업을 수행합니다.
func Initialization(ctx context.Context) error {
	// Set go-routine count
	coreCount = runtime.NumCPU()
	if coreCount < 4 {
		coreCount = 4
	}

	// Create internal database connection pool
	if err := createInternalConnectionPool(ctx); err != nil {
		return err
	} else {
		logger.PrintMessage("notice", "Successful connection with internal database")
	}

	// Create exteranl database connection pool
	gExDB = make(map[string]model.ConnInfo)
	if err := createExternalConnectionPool(ctx); err != nil {
		return err
	} else {
		logger.PrintMessage("notice", "Successful connection with external databases")
	}
	return nil
}

/*
 * [Private function] Create internal connection pool
 * <IN> ctx (context.Context): context
 * <OUT> (error): error object (contain nil)
 */
func createInternalConnectionPool(ctx context.Context) error {
	// Create source object
	source := model.Source{
		Category: "sql",
		Type:     "mysql",
		Name:     "main_database",
		RealDsn:  os.Getenv("DSN"),
	}
	// Create connection pool
	return CreateConnectionPool(ctx, source, false)
}

/*
 * [Private function] Create external connection pools
 * <IN> ctx (context.Context): context
 * <OUT> (error): error object (contain nil)
 */
func createExternalConnectionPool(ctx context.Context) error {
	// Get a list of connection info
	list, err := In_getDatabaseConnectionList(ctx)
	if err != nil {
		return err
	}
	// Create connection pool
	for _, source := range list {
		if err := CreateConnectionPool(ctx, source, true); err != nil {
			return err
		}
	}
	return nil
}

// Database Connection Pool를 생성하는 함수입니다.
// 생성된 Connection Pool들은 메모리 상에서 관리되며, 데이터베이스와의 데이터 일치성을 위해서 외부 데이터베이스에 대한 Connection 정보는 Polling을 통해 주기적으로 업데이트해주어야 합니다.
//	# Parameters Description
//	ctx (context.Context): context
//	source (model.Source): source information object
//	isEx (bool): external databse or not
func CreateConnectionPool(ctx context.Context, source model.Source, isEx bool) error {
	var db *sql.DB
	var err error

	// Get a status to track a database
	trackDB := util.GetTrackingStatus("database")
	// Set segment and sub context various
	var segment *xray.Segment
	var subCtx context.Context

	// Create database object for internal database
	if trackDB {
		// Set segment
		subCtx, segment = xray.BeginSegment(ctx, "Initialize Database")
		defer segment.Close(nil)
		// Store context
		db, err = xray.SQLContext(source.Type, source.RealDsn)
	} else {
		db, err = sql.Open(source.Type, source.RealDsn)
	}
	// Catch error
	if err != nil {
		return err
	}

	// Wapping for sqlx by connection type (internal, external)
	wappingDB := sqlx.NewDb(db, source.Type)
	// Set database options for connection pool
	SetConnectionPoolOptions(wappingDB, isEx)

	// Test ping
	if trackDB {
		err = wappingDB.PingContext(subCtx)
	} else {
		err = wappingDB.Ping()
	}
	// Catch error
	if err != nil {
		return err
	} else {
		// Create connection object
		conn := model.ConnInfo{
			Category: source.Category,
			Dsn:      source.FakeDsn,
			Type:     source.Type,
			Name:     source.Name,
			Tracking: trackDB,
			Instance: wappingDB,
		}
		// Store connection pool
		if isEx {
			gExDB[source.Uuid] = conn
		} else {
			gInDB = conn
		}
	}
	return nil
}

// Connected 데이터베이스를 대상으로 Connection pool을 생성하기 위한 옵션을 설정하는 함수입니다.
//	# Parameters Description
//	db (*sqlx.DB): connected database object
//	isEx (bool): external database or not
func SetConnectionPoolOptions(db *sqlx.DB, isEx bool) {
	// Create various
	var limitConn int
	var timeout time.Duration
	// Set various by type (internal, external)
	if isEx {
		conn := os.Getenv("DATABASE_CONNECTION_LIMIT")
		if conn == "" {
			conn = strconv.FormatInt(EXTERNAL_CONN_LIMIT, 10)
		}
		// Transform
		transformed, err := strconv.ParseInt(conn, 10, 64)
		if err != nil {
			limitConn = EXTERNAL_CONN_LIMIT
		} else if limitConn <= 0 {
			limitConn = EXTERNAL_CONN_LIMIT
		} else {
			limitConn = int(transformed)
		}

		timeout = EXTERNAL_CONN_TIMEOUT
	} else {
		limitConn = INTERNAL_CONN_LIMIT
		timeout = INTERNAL_CONN_TIMEOUT
	}
	// Set database options for connection pool
	db.SetConnMaxIdleTime(timeout)
	db.SetMaxOpenConns(limitConn)
	db.SetMaxIdleConns(limitConn)
}

// 생성된 Connection Pool 내에서 Connection을 제공하는 함수입니다. 내부 또는 외부 데이터베이스 여부를 확인하고, 외부 데이터베이스 인 경우 Database의 Key(Source ID)에 맞는 Connection을 제공합니다.
//	# Parameters Description
//	connType (string): database type ("internal" or "external")
//	key (interface{}): external database key
func GetDatabase(connType string, key interface{}) (model.ConnInfo, error) {
	// Set default value
	info := model.ConnInfo{}
	// Return object by type
	if connType == "internal" {
		if gInDB.Instance == nil {
			return gInDB, errors.New("No initialization was made for the database")
		} else {
			return gInDB, nil
		}
	} else if connType == "external" {
		if value, ok := gExDB[key.(string)]; ok {
			return value, nil
		} else {
			return model.ConnInfo{}, errors.New("Invalid database key\r\n")
		}
	} else {
		return info, errors.New("Invalid conn type\r\n")
	}
}
