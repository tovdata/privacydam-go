package did

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	// Model
	model "github.com/tovdata/privacydam-go/core/model"
)

// define some error code
const (
	ErrorParameter = 2
	// execution format error
	ErrorOptionFile = 3
	// option file read error
	ErrorOptionFormat = 4  //  option file format error
	ErrorInputFormat  = 5  // input file format error
	ErrorOutput       = 6  // file write error
	ErrorInternal     = 10 // mapping function execution error
)

func BuildEncryptingFunc(options model.AnoOption) func(string) string {
	switch options.Algorithm {
	case "hmac":
		switch options.Digest {
		case "sha256":
			mac := hmac.New(sha256.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		case "md5":
			mac := hmac.New(md5.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		default:
			mac := hmac.New(sha256.New, []byte(options.Key))
			return func(inString string) string {
				mac.Write([]byte(inString))
				defer mac.Reset()
				return hex.EncodeToString(mac.Sum(nil))
			}
		}
	case "hash(sha256)":
		mac := sha256.New()
		return func(inString string) string {
			mac.Write([]byte(inString))
			defer mac.Reset()
			return hex.EncodeToString(mac.Sum(nil))
		}
	case "hash(md5)":
		mac := md5.New()
		return func(inString string) string {
			mac.Write([]byte(inString))
			defer mac.Reset()
			return hex.EncodeToString(mac.Sum(nil))
		}
	default:
		return func(inString string) string {
			return "unknown Encrypting algorithm"
		}
	}
}

func BuildRoundingFunc(options model.AnoOption) func(string) string {
	/*position, err := strconv.ParseInt(options.Position, 10, 0)
	if err != nil {
		return func (inString string) string {
			return "position parameter error"
		}
	}*/
	position := int(options.Position)
	posPower := math.Pow(10, math.Abs(float64(position)))
	switch options.Algorithm {
	case "round":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Round(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Round(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString
		}
	case "ceil":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Ceil(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Ceil(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString

		}
	case "floor":
		return func(inString string) string {
			if value, err := strconv.ParseFloat(inString, 64); err == nil {
				if position > 0 {
					return strconv.FormatFloat(math.Floor(value*posPower)/posPower, 'f', position, 64)
				}
				return strconv.FormatFloat(math.Floor(value/posPower)*posPower, 'f', 0, 64)
			}
			return "parseFloat error:" + inString
		}
	default:
		return func(inString string) string {
			return "unknown Rounding algorithm"
		}
	}
}

func BuildRangingFunc(options model.AnoOption) func(string) string {
	lowBound, err := strconv.ParseFloat(options.Lower, 64)
	if err != nil {
		return func(inString string) string {
			return "lower parameter error"
		}
	}
	upBound, err2 := strconv.ParseFloat(options.Upper, 64)
	if err2 != nil {
		return func(inString string) string {
			return "upper parameter error"
		}
	}
	binNumP, err3 := strconv.ParseInt(options.Bin, 10, 0)
	if err3 != nil {
		return func(inString string) string {
			return "bin parameter error"
		}
	}
	binNum := int(binNumP)
	//boundary := []float64{}
	boundary := make([]float64, binNum+1, binNum+1)
	for i := 0; i < binNum; i++ {
		//boundary = append(boundary, lowBound+((upBound-lowBound)/float64(binNum))*float64(i))
		boundary[i] = lowBound + ((upBound-lowBound)/float64(binNum))*float64(i)
	}
	//boundary = append(boundary, upBound)
	boundary[binNum] = upBound

	return func(inString string) string {
		if value, err := strconv.ParseFloat(inString, 64); err == nil {
			before := ""
			last := ""
			for _, bound := range boundary {
				if bound > value {
					return fmt.Sprint(before, " ~ ", bound)
				}
				before = fmt.Sprintf("%v", bound) //bound
				last = fmt.Sprintf("%v", bound)
			}
			return fmt.Sprint(last, " ~ ")
		}
		return "parseFloat error:" + inString
	}
}

func BuildMaskingFunc(options model.AnoOption) func(string) string {
	//maskPattern = '(^.{{{startlen}}})(.*)(.{{{endlen}}}$)'
	fore, err := strconv.ParseInt(options.Fore, 10, 0)
	if err != nil {
		return func(inString string) string {
			return "fore parameter error"
		}
	}
	aft, err1 := strconv.ParseInt(options.Aft, 10, 0)
	if err1 != nil {
		return func(inString string) string {
			return "aft parameter error"
		}
	}
	maskChar := options.MaskChar
	keepLength, err2 := strconv.ParseBool(options.KeepLength)
	if err2 != nil {
		return func(inString string) string {
			return "keepLength parameter error"
		}
	}
	//reString := fmt.Sprintf("(^.{%v})(.*)(.{%v}$)", fore, aft)
	//re := regexp.MustCompile(reString)

	mask := strings.Repeat(maskChar, int(256)) // assume the Maximum Length of field is less than 256

	//reObject = re.compile(maskPattern.format(startlen=fore, endlen=aft))
	return func(inString string) string {
		if inString == "" {
			return ""
		}
		if len(inString) >= int(fore+aft) {
			//resIndex := re.FindStringSubmatchIndex(inString)
			if keepLength {
				//maskLen := resIndex[5] - resIndex[4]
				maskLen := len(inString) - int(fore) - int(aft)
				//repeatNum := math.Ceil(float64(maskLen / len(maskChar)))
				//mask := strings.Repeat(maskChar, int(repeatNum))
				//return inString[resIndex[2]:resIndex[3]] + mask[0:maskLen] + inString[resIndex[6]:resIndex[7]]
				return inString[0:fore] + mask[0:maskLen] + inString[len(inString)-int(aft):]
			}
			//return inString[resIndex[2]:resIndex[3]] + maskChar + inString[resIndex[6]:resIndex[7]]
			return inString[0:fore] + maskChar + inString[len(inString)-int(aft):]
		}
		return ""
	}
}
