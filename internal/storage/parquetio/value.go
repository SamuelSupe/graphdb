package parquetio

import (
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	parquetValueKindNull   = "null"
	ParquetValueKindString = "string"
	parquetValueKindBool   = "bool"
	parquetValueKindNumber = "number"
	parquetValueKindRaw    = "raw"
)

type Value struct {
	Kind        string
	StringValue string
	BoolValue   bool
	FloatValue  float64
}

func ValueFromAny(value any) (Value, error) {
	switch typed := value.(type) {
	case nil:
		return Value{Kind: parquetValueKindNull}, nil
	case string:
		return Value{Kind: ParquetValueKindString, StringValue: typed}, nil
	case bool:
		return Value{Kind: parquetValueKindBool, BoolValue: typed}, nil
	case float64:
		return Value{Kind: parquetValueKindNumber, FloatValue: typed}, nil
	case float32:
		return Value{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case int:
		return Value{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case int64:
		return Value{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case json.Number:
		parsed, err := strconv.ParseFloat(string(typed), 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: parquetValueKindNumber, FloatValue: parsed}, nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: parquetValueKindRaw, StringValue: string(data)}, nil
	}
}

func AnyFromValue(value Value) (any, error) {
	switch value.Kind {
	case parquetValueKindNull:
		return nil, nil
	case ParquetValueKindString:
		return value.StringValue, nil
	case parquetValueKindBool:
		return value.BoolValue, nil
	case parquetValueKindNumber:
		return value.FloatValue, nil
	case parquetValueKindRaw:
		var out any
		if err := json.Unmarshal([]byte(value.StringValue), &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown parquet value kind %q", value.Kind)
	}
}
