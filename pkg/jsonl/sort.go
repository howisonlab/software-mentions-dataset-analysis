package jsonl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"iter"
	"sort"
)

const keyField = "file"

type uint128 struct {
	_1 uint64
	_2 uint64
}

func Sort(seq iter.Seq2[*map[string]any, error]) (iter.Seq[map[string]any], error) {
	var keys []uint128
	valMap := make(map[uint128]map[string]any)

	for v, err := range seq {
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		keyAny, exists := (*v)[keyField]
		if !exists {
			return nil, fmt.Errorf("entry missing key field %q", keyField)
		}

		keyString, isString := keyAny.(string)
		if !isString {
			return nil, fmt.Errorf("entry key field %q is %T, not a string", keyAny, keyField)
		}

		id, err := uuid.Parse(keyString[:36])
		if err != nil {
			return nil, fmt.Errorf("entry key field %q is not a UUID: %w", keyString, err)
		}
		idBytes, err := id.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("unable to format bytes for entry field %q: %w", keyString, err)
		}

		left := binary.BigEndian.Uint64(idBytes[:8])
		right := binary.BigEndian.Uint64(idBytes[8:])
		key := uint128{_1: left, _2: right}

		keys = append(keys, key)
		valMap[key] = *v
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i]._1 != keys[j]._1 {
			return keys[i]._1 < keys[j]._1
		}
		return keys[i]._2 < keys[j]._2
	})

	return func(yield func(map[string]any) bool) {
		for _, key := range keys {
			if !yield(valMap[key]) {
				return
			}
		}
	}, nil
}
