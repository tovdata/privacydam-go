package kAno

import (
	"fmt"
)

type anoEncoder struct {
	encDict, freqDict map[string]int
	initialized       bool
}

func (p *anoEncoder) init() {
	p.encDict = make(map[string]int)
	p.freqDict = make(map[string]int)
	p.initialized = true
}
func (p *anoEncoder) add(str string) int {
	if str == "" || p.initialized != true {
		return 0
	}
	if freq, ok := p.freqDict[str]; ok {
		p.freqDict[str] = freq + 1
	} else {
		p.encDict[str] = len(p.encDict) + 1
		p.freqDict[str] = 1
	}
	return p.encDict[str]
}
func (p *anoEncoder) getMinFreq() int {
	if p.initialized != true {
		return 0
	}
	minFreq := 65535
	for _, value := range p.freqDict {
		if value < minFreq {
			minFreq = value
		}
	}
	return minFreq
}
func (p *anoEncoder) getMaxFreq() int {
	maxFreq := 0
	for _, value := range p.freqDict {
		if value > maxFreq {
			maxFreq = value
		}
	}
	return maxFreq
}
func (p *anoEncoder) encode(str string) int {
	if p.initialized != true {
		return 0
	}
	return p.encDict[str]
}
func (p *anoEncoder) decode(target int) string {
	for key, value := range p.encDict {
		if value == target {
			return key
		}
	}
	return ""
}

type AnoTester struct {
	// disable field-level encoding for performance
	//encoderList  map[int]*anoEncoder
	finalEncoder anoEncoder
	targetKValue int
	fieldLen	int
	evalFields   []bool
}

func (t *AnoTester) New(length int, kValue int) {
	t.fieldLen = length
	// disable field-level encoding for performance
	//t.encoderList = make(map[int]*anoEncoder, length)
	t.evalFields = make([]bool, length)
	for i := 0; i < length; i++ {
		// disable field-level encoding for performance
		//v := new(anoEncoder)
		//v.init()
		//t.encoderList[i] = v
		t.evalFields[i] = true
	}
	t.finalEncoder.init()
	t.targetKValue = kValue
}
func (t *AnoTester) SetEvalFields(fields []bool) {
	for i, v := range fields {
		if i < len(t.evalFields) {
			t.evalFields[i] = v
		}
	}
}
func (t *AnoTester) AddStrings(strList []string) int {
	// disable field-level encoding for performance
	//encoded := make([]int, 0)
	filtered := make([]string, t.fieldLen)
	for i, v := range strList {
		if t.evalFields[i] {
			// disable field-level encoding for performance
			//encoder := t.encoderList[i]
			//encoded = append(encoded, encoder.add(v))
			filtered[i] = v
		}
	}
	//fmt.Printf("%v\n", encoded)
	//return t.finalEncoder.add(fmt.Sprintf("%v", encoded))
	return t.finalEncoder.add(fmt.Sprintf("%q", filtered))
}
func (t *AnoTester) Eval() (bool, int) {
	actValue := t.finalEncoder.getMinFreq()
	if actValue < t.targetKValue {
		//fmt.Println("Failed! (Target: ", t.targetKValue, ", actual: ", actValue, ")")
		return false, actValue
	} else {
		//	fmt.Println("Passed")
		return true, actValue
	}
}
