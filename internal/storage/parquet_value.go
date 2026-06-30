package storage

import (
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	parquetValueKindNull   = "null"
	parquetValueKindString = "string"
	parquetValueKindBool   = "bool"
	parquetValueKindNumber = "number"
	parquetValueKindRaw    = "raw"
)

type parquetValue struct {
	Kind        string
	StringValue string
	BoolValue   bool
	FloatValue  float64
}

func parquetValueFromAny(value any) (parquetValue, error) {
	switch typed := value.(type) {
	case nil:
		return parquetValue{Kind: parquetValueKindNull}, nil
	case string:
		return parquetValue{Kind: parquetValueKindString, StringValue: typed}, nil
	case bool:
		return parquetValue{Kind: parquetValueKindBool, BoolValue: typed}, nil
	case float64:
		return parquetValue{Kind: parquetValueKindNumber, FloatValue: typed}, nil
	case float32:
		return parquetValue{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case int:
		return parquetValue{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case int64:
		return parquetValue{Kind: parquetValueKindNumber, FloatValue: float64(typed)}, nil
	case json.Number:
		parsed, err := strconv.ParseFloat(string(typed), 64)
		if err != nil {
			return parquetValue{}, err
		}
		return parquetValue{Kind: parquetValueKindNumber, FloatValue: parsed}, nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return parquetValue{}, err
		}
		return parquetValue{Kind: parquetValueKindRaw, StringValue: string(data)}, nil
	}
}

func anyFromParquetValue(value parquetValue) (any, error) {
	switch value.Kind {
	case parquetValueKindNull:
		return nil, nil
	case parquetValueKindString:
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
