// 로그 출력을 위한 패키지 (Using core part)
package logger

import (
	"bytes"
	"log"
)

// 로그 메시지를 출력하는 함수입니다.
//	# Parameters
//	logType (string): log type [debug|notice|warning|error]
//	message (string): log message
func PrintMessage(logType string, message string) {
	// Set buffer
	var buffer bytes.Buffer

	// Set log message prefix (by log type)
	switch logType {
	case "debug":
		buffer.WriteString("[DEBUG] ")
	case "notice":
		buffer.WriteString("[NOTICE] ")
	case "warning":
		buffer.WriteString("[WARNING] ")
	case "error":
		buffer.WriteString("[ERROR] ")
	default:
		buffer.WriteString("[DEBUG] ")
	}
	// Set log message
	buffer.WriteString(message)
	// Print log
	log.Println(buffer.String())
}
