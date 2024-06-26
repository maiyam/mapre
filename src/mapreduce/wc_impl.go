package mapreduce

import (
	"log"
	"strconv"
	s "strings"
	u "unicode"
)

// WCMap our simplified version of MapReduce does not supply a key to the
// Map function, as in Google's MapReduce paper; only a value,
// which is a portion of the input file's content
func WCMap(value string) []KeyValue {
	f := func(c rune) bool {
		return !u.IsLetter(c)
	}

	wordsSlice := s.FieldsFunc(value, f)
	var wordsWithCount = make([]KeyValue, len(wordsSlice))
	for i, word := range wordsSlice {
		wordsWithCount[i] = KeyValue{word, "1"}
	}
	return wordsWithCount
}

// WCReduce called once for each key generated by Map, with a list of that
// key's values. should return a single output value for that key.
func WCReduce(key string, values []string) string {
	var res uint64
	for _, countStr := range values {
		tmp, err := strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			log.Fatal("Got error while running parseUnint err: ", err)
		}
		res += tmp

	}
	return strconv.FormatUint(res, 10)

}
