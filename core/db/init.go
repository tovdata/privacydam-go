package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"runtime"
	"time"

	"github.com/jmoiron/sqlx"

	// AWS
	"github.com/aws/aws-xray-sdk-go/xray"
	// Model
	"privacydam-go/core/model"
	// Util
	"privacydam-go/core/log"
	"privacydam-go/core/util"

	// Driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	INTERNAL_CONN_TIMEOUT = time.Second * 30
	EXTERNAL_CONN_TIMEOUT = time.Second * 30
	INTERNAL_CONN_LIMIT   = 8
	EXTERNAL_CONN_LIMIT   = 8
)

var (
	gExDB map[string]model.ConnInfo
	gInDB model.ConnInfo

	coreCount int
)

/*
 * Initialization (create internal and external database connection pool)
 * <IN> ctx (context.Context): context
 * <OUT> (error): error object (contain nil)
 */
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
		log.PrintMessage("notice", "Successful connection with internal database")
	}

	// Create exteranl database connection pool
	gExDB = make(map[string]model.ConnInfo)
	if err := createExternalConnectionPool(ctx); err != nil {
		return err
	} else {
		log.PrintMessage("notice", "Successful connection with external databases")
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

/*
 * Create connection pool
 * <IN> ctx (context.Context): context
 * <IN> source (model.Source): source information object
 * <IN> isEx (bool): external databse or not
 * <OUT> (error): error object (contain nil)
 */
func CreateConnectionPool(ctx context.Context, source model.Source, isEx bool) error {
	var db *sql.DB
	var err error

	// Get a status to track a database
	trackDB := util.GetTrackingStatus("database")

	// Create database object for internal database
	if trackDB {
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
	setConnectionPoolOptions(wappingDB, isEx)

	// Test ping
	if trackDB {
		err = wappingDB.PingContext(ctx)
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

/*
 * Set connection pool options
 * <IN> db (*sqlx.DB): connected database object
 * <IN> isEx (bool): external database or not
 */
func setConnectionPoolOptions(db *sqlx.DB, isEx bool) {
	// Create various
	var limitConn int
	var timeout time.Duration
	// Set various by type (internal, external)
	if isEx {
		limitConn = EXTERNAL_CONN_LIMIT
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

/*
 * Get internal database object
 * <IN> connType (string): database type ("internal" or "external")
 * <IN> key (interface{}): external database key
 * <OUT> (model.ConnInfo): database connection information
 * <OUT> (error): error object (contain nil)
 */
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
			return model.ConnInfo{}, errors.New("Invalid database key")
		}
	} else {
		return info, errors.New("Invalid conn type")
	}
}
