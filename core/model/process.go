package model

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Accessor information format to write access log
type Accessor struct {
	Ip        string `json:"ip"`
	UserAgent string `json:"agent"`
}

// External database simple information and connection object for connection pool
type ConnInfo struct {
	Category string   `json:"category"`
	Dsn      string   `json:"dsn"`
	Type     string   `json:"type"`
	Name     string   `json:"name"`
	Tracking bool     `json:"tracking"`
	Instance *sqlx.DB `json:"instance"`
}

// API information format
type Api struct {
	Uuid         string       `json:"uuid,omitempty" db:"api_id"`
	Name         string       `json:"name,omitempty" db:"api_name"`
	Alias        string       `json:"alias" db:"api_alias"`
	Type         string       `json:"type" db:"api_type"`
	RegDate      string       `json:"regDate,omitempty" db:"reg_date"`
	ExpDate      string       `json:"expDate" db:"exp_date"`
	Status       string       `json:"status,omitempty"`
	SourceId     string       `json:"source" db:"source_id"`
	QueryContent QueryContent `json:"queryContent" db:"queryContent"`
}

// Database information (= source) format to load from internal databse
type Source struct {
	Uuid     string `json:"uuid,omitempty" db:"source_id"`
	Category string `json:"category" db:"source_category"`
	Type     string `json:"type" db:"source_type"`
	Name     string `json:"name" db:"source_name"`
	RealDsn  string `json:"realDsn" db:"real_dsn"`
	FakeDsn  string `json:"fakeDsn" db:"fake_dsn"`
}

// information format to query
type QueryContent struct {
	Syntax        string                    `json:"syntax" db:"syntax"`
	ParamsKey     []string                  `json:"paramsKey,omitempty"`
	ParamsValue   []interface{}             `json:"paramsValue,omitempty"`
	RawDidOptions sql.NullString            `json:"rawDidOptions,omitempty" db:"rawDidOptions"`
	DidOptions    map[string]AnoParamOption `json:"didOptions,omitempty"`
}

// evaluation result format for k-anonymity
type Evaluation struct {
	ApiName string `json:"apiName"`
	Result  string `json:"result"`
	Value   int64  `json:"value"`
}

// AnoOption defines the specific anonymization option parameter format
type AnoOption struct {
	Fore       string `json:"fore,omitempty"`
	Aft        string `json:"aft,omitempty"`
	MaskChar   string `json:"maskChar,omitempty"`
	KeepLength string `json:"keepLength,omitempty"`
	Algorithm  string `json:"algorithm,omitempty"`
	Position   int    `json:"position,omitempty"`
	Unit       string `json:"unit,omitempty"`
	Key        string `json:"key,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Lower      string `json:"lower,omitempty"`
	Upper      string `json:"upper,omitempty"`
	Bin        string `json:"bin,omitempty"`
	Linear     string `json:"linear,omitempty"`
}

// Option defines the field anonymization method parameter format
type AnoParamOption struct {
	Method      string    `json:"method"`
	Options     AnoOption `json:"options"`
	Level       int       `json:"level"`
	Description string    `json:"description"`
}

// Processed log format
type Processed struct {
	ApiAlias  string          `json:"apiName"`
	DateTime  string          `json:"datetime"`
	Message   string          `json:"message"`
	Result    string          `json:"result"`
	RemoteIp  string          `json:"remoteIp"`
	UserAgent string          `json:"userAgent"`
	Detail    ProcessedDetail `json:"processedInfo"`
}

// Processed detail format
type ProcessedDetail struct {
	Dsn       string `json:"dsn"`
	KAnoPass  string `json:"kAnoPass"`
	KAnoValue string `json:"kAnoValue"` // string of int format
	Params    string `json:"params"`    // string of array format
	Syntax    string `json:"syntax"`
}
