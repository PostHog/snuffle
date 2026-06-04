package snuffle

import (
	"fmt"
	"math"

	promvalue "github.com/prometheus/prometheus/model/value"
)

func isRemoteWriteSampleDroppable(value float64) bool {
	return math.IsNaN(value) && !promvalue.IsStaleNaN(value)
}

func isStaleSampleValue(value float64) bool {
	return promvalue.IsStaleNaN(value)
}

func staleSampleSQL(valueExpr string) string {
	return fmt.Sprintf("reinterpretAsUInt64(%s) = %d", valueExpr, uint64(promvalue.StaleNaN))
}

func nonStaleSampleSQL(valueExpr string) string {
	return fmt.Sprintf("reinterpretAsUInt64(%s) != %d", valueExpr, uint64(promvalue.StaleNaN))
}

func nonStaleNullableSampleSQL(valueExpr string) string {
	return "isNotNull(" + valueExpr + ") AND " + nonStaleSampleSQL("assumeNotNull("+valueExpr+")")
}

func staleAwareNullableGridSQL(gridExpr string) string {
	return fmt.Sprintf(
		"arrayMap(x -> if(isNull(x) OR %s, NULL, x), %s)",
		staleSampleSQL("assumeNotNull(x)"),
		gridExpr,
	)
}
