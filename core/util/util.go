package util

import (
	"log"
	"os"
	"strconv"
)

func GetTrackingStatus(srcType string) bool {
	var statusText string
	// Get a status by source type
	switch srcType {
	case "processing":
		statusText = os.Getenv("TRACK_A_PROCESSING")
	case "database":
		statusText = os.Getenv("TRACK_A_DATABASE")
	}

	// Transform to boolean
	if status, err := strconv.ParseBool(statusText); err != nil {
		log.Fatal("Initialization failed (not found tracking configuration)")
		return false
	} else {
		return status
	}
}
