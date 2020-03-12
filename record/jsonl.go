package record

import (
	"strings"
)

func FormatJsonl(data []byte) ([]string, error) {
	return strings.Split(string(data), "\n"), nil
}
