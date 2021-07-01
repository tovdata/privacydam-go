package log

import (
	"bytes"
	"log"
)

/*
 * Print log message
 * <IN> logType (string): log type [debug|notice|warning|error]
 * <IN> message (string): log message
 */
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
