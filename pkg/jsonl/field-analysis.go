package jsonl

import (
	"fmt"
	"math"
	"strings"
)

// MaxEnum is the largest number of unique values to track before not trying to
// interpret the field as an enum.
const MaxEnum = 20

type Field interface {
	Add(obj any) (Field, error)
	String() string
}

// NullField represents a field which is never filled in.
// Adding any object to a NullField returns a non-NullField.
type NullField struct{}

func (nf *NullField) Add(obj any) (Field, error) {
	switch o := obj.(type) {
	case bool:
		var f Field = &BoolField{}
		f, err := f.Add(o)
		if err != nil {
			return nil, err
		}
		return f, nil
	case float64:
		var f Field = &NumberField{
			Seen: make(map[float64]int),
		}
		f, err := f.Add(o)
		if err != nil {
			return nil, err
		}
		return f, nil
	case string:
		var f Field = &StringField{
			Seen: make(map[string]int),
		}
		f, err := f.Add(o)
		if err != nil {
			return nil, err
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown type %T added to %T", o, nf)
	}
}

func (nf *NullField) String() string {
	return "null"
}

type BoolField struct {
	True  int
	False int
}

func (f *BoolField) Add(obj any) (Field, error) {
	switch o := obj.(type) {
	case bool:
		if o {
			f.True++
		} else {
			f.False++
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown type %T added to %T", o, f)
	}
}

func (f *BoolField) String() string {
	return fmt.Sprintf("true:%d;false:%d", f.True, f.False)
}

type NumberType string

type NumberField struct {
	Integral bool
	Float32  bool

	Min, Max float64
	Seen     map[float64]int
}

func (f *NumberField) Add(obj any) (Field, error) {
	switch o := obj.(type) {
	case float64:
		if len(f.Seen) > 0 {
			f.Integral = f.Integral && isIntegral(o)
			f.Float32 = f.Float32 && isFloat32(o)

			if o < f.Min {
				f.Min = o
			} else if o > f.Max {
				f.Max = o
			}
		} else {
			f.Integral = isIntegral(o)
			f.Float32 = isFloat32(o)

			f.Min = o
			f.Max = o
		}

		if len(f.Seen) <= MaxEnum {
			f.Seen[o]++
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown type %T added to %T", o, f)
	}
}

func isIntegral(f float64) bool {
	return math.Round(f) == f
}

const (
	Float64FractionLength = 52
	Float32FractionLength = 23
	Float64Mask           = (1 << (Float64FractionLength - Float32FractionLength)) - 1

	// DropRight is the number of decimal places at the end to discard when testing for
	// if the value is a float32.
	DropRight = 3
)

func isFloat32(f float64) bool {
	n := math.Float64bits(f)
	n &= Float64Mask

	// The number can be represented as a float32 without loss of precision as
	// it uses none of the float64-specific fraction bits.
	// Does not handle exponents out of the range of float32.
	return n == 0

	//if n == 0 {
	//	return true
	//}
	//formatted := strconv.FormatFloat(f, 'f', -1, 64)
	//decimal := strings.Index(formatted, ".")
	//if decimal == -1 {
	//	// The integer is too large to be represented exactly as a float32.
	//	return false
	//}
	//
	//formatted = formatted[decimal+1:]
	//if len(formatted) <= DropRight {
	//	return true
	//}
	//
	//formatted = formatted[:len(formatted)-DropRight]
	//
	//// Handle cases where the float is very-precisely recreating  essentially
	//// a float32.
	//rightDigit := formatted[len(formatted)-1:]
	//if rightDigit == "0" || rightDigit == "9" {
	//	formatted = strings.TrimRight(formatted, rightDigit)
	//}
	//
	//if len(formatted) > 7 {
	//	fmt.Printf("%s, %b, %b\n", strconv.FormatFloat(f, 'f', -1, 64), math.Float64bits(f), n)
	//}
	//
	//return len(formatted) <= 7
}

func (f *NumberField) String() string {
	result := strings.Builder{}
	if f.Integral {
		if f.Min < 0 {
			if f.Max <= math.MaxInt8 {
				result.WriteString("int8")
			} else if f.Max <= math.MaxInt16 {
				result.WriteString("int16")
			} else if f.Max <= math.MaxInt32 {
				result.WriteString("int32")
			} else {
				result.WriteString("int64")
			}
		} else {
			if f.Max <= math.MaxUint8 {
				result.WriteString("uint8")
			} else if f.Max <= math.MaxUint16 {
				result.WriteString("uint16")
			} else if f.Max <= math.MaxUint32 {
				result.WriteString("uint32")
			} else {
				result.WriteString("uint64")
			}
		}
	} else {
		if f.Float32 {
			result.WriteString("float32")
		} else {
			result.WriteString("float64")
		}
	}
	result.WriteString(";")
	if f.Integral {
		result.WriteString(fmt.Sprintf("%d;%d", int(f.Min), int(f.Max)))
	} else {
		result.WriteString(fmt.Sprintf("%f;%f", f.Min, f.Max))
	}

	if len(f.Seen) <= MaxEnum {
		for k, v := range f.Seen {
			if f.Integral {
				result.WriteString(fmt.Sprintf("%d:%d;", int(k), v))
			} else {
				result.WriteString(fmt.Sprintf("%f:%d;", k, v))
			}
		}
	}

	return result.String()
}

type StringField struct {
	Seen map[string]int
}

func (f *StringField) Add(obj any) (Field, error) {
	switch o := obj.(type) {
	case string:
		if len(f.Seen) <= MaxEnum {
			f.Seen[o]++
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown type %T added to %T", o, f)
	}
}

func (f *StringField) String() string {
	result := strings.Builder{}
	if len(f.Seen) <= MaxEnum {
		result.WriteString(fmt.Sprintf("enum;%d;", len(f.Seen)))
		for k, v := range f.Seen {
			result.WriteString(fmt.Sprintf("%s:%d;", k, v))
		}
	} else {
		result.WriteString("string;")
	}

	return result.String()
}
