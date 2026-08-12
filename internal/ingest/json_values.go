package ingest

import (
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type jsonValueMismatch struct {
	path             string
	generated        any
	profile          any
	generatedPresent bool
	profilePresent   bool
}

// firstJSONValueMismatch compares values according to their JSON shape rather
// than their concrete Go containers. Profile responses use map[string]any and
// []any, while generated metadata can contain named maps and typed slices;
// reflection avoids a lossy marshal-and-decode round trip between them.
func firstJSONValueMismatch(path string, generated any, generatedPresent bool, profile any, profilePresent bool) *jsonValueMismatch {
	if !generatedPresent || !profilePresent {
		if generatedPresent == profilePresent {
			return nil
		}
		return &jsonValueMismatch{
			path: path, generated: generated, profile: profile,
			generatedPresent: generatedPresent, profilePresent: profilePresent,
		}
	}

	generatedNull := isJSONNull(generated)
	profileNull := isJSONNull(profile)
	if generatedNull || profileNull {
		if generatedNull && profileNull {
			return nil
		}
		return presentJSONMismatch(path, generated, profile)
	}

	generatedNumber, generatedNumeric := canonicalJSONNumber(generated)
	profileNumber, profileNumeric := canonicalJSONNumber(profile)
	if generatedNumeric || profileNumeric {
		bothValidNumbers := generatedNumeric && profileNumeric && generatedNumber != nil && profileNumber != nil
		if bothValidNumbers && generatedNumber.Cmp(profileNumber) == 0 {
			return nil
		}
		return presentJSONMismatch(path, generated, profile)
	}

	generatedValue := reflect.ValueOf(generated)
	profileValue := reflect.ValueOf(profile)
	if isJSONObject(generatedValue) || isJSONObject(profileValue) {
		if !isJSONObject(generatedValue) || !isJSONObject(profileValue) {
			return presentJSONMismatch(path, generated, profile)
		}
		return firstJSONObjectMismatch(path, generatedValue, profileValue)
	}

	if isJSONArray(generatedValue) || isJSONArray(profileValue) {
		if !isJSONArray(generatedValue) || !isJSONArray(profileValue) {
			return presentJSONMismatch(path, generated, profile)
		}
		return firstJSONArrayMismatch(path, generatedValue, profileValue)
	}

	if generatedValue.Kind() == reflect.String && profileValue.Kind() == reflect.String && generatedValue.String() == profileValue.String() {
		return nil
	}
	if generatedValue.Kind() == reflect.Bool && profileValue.Kind() == reflect.Bool && generatedValue.Bool() == profileValue.Bool() {
		return nil
	}
	return presentJSONMismatch(path, generated, profile)
}

func firstJSONObjectMismatch(path string, generated, profile reflect.Value) *jsonValueMismatch {
	keys := make(map[string]struct{}, generated.Len()+profile.Len())
	for _, key := range generated.MapKeys() {
		keys[key.String()] = struct{}{}
	}
	for _, key := range profile.MapKeys() {
		keys[key.String()] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		generatedMember := generated.MapIndex(reflect.ValueOf(key).Convert(generated.Type().Key()))
		profileMember := profile.MapIndex(reflect.ValueOf(key).Convert(profile.Type().Key()))
		var generatedValue, profileValue any
		if generatedMember.IsValid() {
			generatedValue = generatedMember.Interface()
		}
		if profileMember.IsValid() {
			profileValue = profileMember.Interface()
		}
		if mismatch := firstJSONValueMismatch(appendJSONPointer(path, key), generatedValue, generatedMember.IsValid(), profileValue, profileMember.IsValid()); mismatch != nil {
			return mismatch
		}
	}
	return nil
}

func firstJSONArrayMismatch(path string, generated, profile reflect.Value) *jsonValueMismatch {
	common := min(generated.Len(), profile.Len())
	for index := range common {
		if mismatch := firstJSONValueMismatch(appendJSONPointer(path, strconv.Itoa(index)), generated.Index(index).Interface(), true, profile.Index(index).Interface(), true); mismatch != nil {
			return mismatch
		}
	}
	if generated.Len() == profile.Len() {
		return nil
	}

	index := common
	var generatedValue, profileValue any
	generatedPresent := index < generated.Len()
	profilePresent := index < profile.Len()
	if generatedPresent {
		generatedValue = generated.Index(index).Interface()
	}
	if profilePresent {
		profileValue = profile.Index(index).Interface()
	}
	return firstJSONValueMismatch(appendJSONPointer(path, strconv.Itoa(index)), generatedValue, generatedPresent, profileValue, profilePresent)
}

func equalJSONValues(left, right any) bool {
	return firstJSONValueMismatch("", left, true, right, true) == nil
}

func presentJSONMismatch(path string, generated, profile any) *jsonValueMismatch {
	return &jsonValueMismatch{
		path: path, generated: generated, profile: profile,
		generatedPresent: true, profilePresent: true,
	}
}

func isJSONNull(value any) bool {
	if value == nil {
		return true
	}
	// Nil containers are JSON null, not empty containers. TAMS Profile contracts
	// require null, omitted, empty object, and empty array to remain distinct.
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Map, reflect.Slice, reflect.Interface, reflect.Pointer:
		return reflected.IsNil()
	default:
		return false
	}
}

func isJSONObject(value reflect.Value) bool {
	return value.IsValid() && value.Kind() == reflect.Map && value.Type().Key().Kind() == reflect.String
}

func isJSONArray(value reflect.Value) bool {
	return value.IsValid() && (value.Kind() == reflect.Array || value.Kind() == reflect.Slice)
}

// canonicalJSONNumber returns the exact rational value and whether value has a
// Go numeric type. A nil rational with true denotes a malformed json.Number or
// a non-finite float, neither of which is a valid JSON number or an equal value.
func canonicalJSONNumber(value any) (*big.Rat, bool) {
	if number, ok := value.(json.Number); ok {
		text := number.String()
		if !json.Valid([]byte(text)) {
			return nil, true
		}
		rational, valid := new(big.Rat).SetString(text)
		if !valid {
			return nil, true
		}
		return rational, true
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return nil, false
	}
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(big.Rat).SetInt64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		integer := new(big.Int).SetUint64(reflected.Uint())
		return new(big.Rat).SetInt(integer), true
	case reflect.Float32, reflect.Float64:
		floating := reflected.Float()
		if math.IsInf(floating, 0) || math.IsNaN(floating) {
			return nil, true
		}
		text := strconv.FormatFloat(floating, 'g', -1, reflected.Type().Bits())
		rational, valid := new(big.Rat).SetString(text)
		if !valid {
			return nil, true
		}
		return rational, true
	default:
		return nil, false
	}
}

func appendJSONPointer(path, token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")
	return path + "/" + token
}

func formatJSONMismatchValue(value any, present bool) string {
	if !present {
		return "<missing>"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<invalid JSON value>"
	}
	const maxRunes = 160
	characters := []rune(string(encoded))
	if len(characters) > maxRunes {
		characters = append(characters[:maxRunes-3], '.', '.', '.')
	}
	return string(characters)
}
