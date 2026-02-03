package main

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	mr "github.com/LakshyaMittal3301/mapreduce/mapreduce"
)

// Map: for each word occurrence, emit (term, "doc:1")
func Map(filename string, contents string) []mr.KeyValue {
	// Split on non-letters
	isSep := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, isSep)

	doc := filepath.Base(filename)

	kva := make([]mr.KeyValue, 0, len(words))
	for _, w := range words {
		w = strings.ToLower(w)
		if w == "" {
			continue
		}
		kva = append(kva, mr.KeyValue{Key: w, Value: doc + ":1"})
	}
	return kva
}

// Reduce: values are like ["docA:1", "docA:1", "docB:1", ...]
// Output: "docA:tf,docB:tf,..." (sorted by doc for stable output)
func Reduce(term string, values []string) string {
	tfByDoc := map[string]int{}

	for _, v := range values {
		parts := strings.SplitN(v, ":", 2)
		if len(parts) != 2 {
			continue
		}
		doc := parts[0]
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		tfByDoc[doc] += n
	}

	// Stable ordering makes demos nicer
	docs := make([]string, 0, len(tfByDoc))
	for d := range tfByDoc {
		docs = append(docs, d)
	}
	sort.Strings(docs)

	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d+":"+strconv.Itoa(tfByDoc[d]))
	}

	return strings.Join(out, ",")
}
