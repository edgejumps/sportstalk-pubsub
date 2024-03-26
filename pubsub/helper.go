package pubsub

import (
	"strconv"
	"strings"
)

func NormalizeActionPath(action string) string {
	if action == "" {
		return "/"
	} else if strings.HasPrefix(action, "/") {
		return action
	} else {
		return "/" + action
	}
}

func NormalizeTTL(value interface{}) int {
	if value == nil {
		return MinimumTTL
	}

	parsed := 0

	switch v := value.(type) {
	case int:
		parsed = v
	case int64:
		parsed = int(v)
	case float64:
		parsed = int(v)
	case string:
		converted, err := strconv.Atoi(v)

		if err == nil {
			parsed = converted
		}
	}

	if parsed < MinimumTTL {
		return MinimumTTL
	}

	return parsed
}

func ParseTimestamp(value interface{}) int64 {
	if value == nil {
		return 0
	}

	parsed := int64(0)

	switch v := value.(type) {
	case int:
		parsed = int64(v)
	case int64:
		parsed = v
	case float64:
		parsed = int64(v)
	case string:
		converted, err := strconv.ParseInt(v, 10, 64)

		if err == nil {
			parsed = converted
		}
	default:
		parsed = 0
	}

	return parsed
}
