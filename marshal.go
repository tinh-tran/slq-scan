package sqlscan

import (
	"unicode"
)

func NewEncoder() Encoder { return Encoder{} }

type Encoder struct {
	unsafe bool
	mapper *Mapper
}

// missing fields on unmarshalling.
func (e Encoder) Unsafe() Encoder {
	e.unsafe = true
	return e
}

func (e Encoder) WithMapper(m *Mapper) Encoder {
	e.mapper = m
	return e
}

var DefaultMapper = NewMapperFunc("db", Underscore)

func Underscore(s string) string {
	in := []rune(s)
	isLower := func(idx int) bool {
		return idx >= 0 && idx < len(in) && unicode.IsLower(in[idx])
	}
	out := make([]rune, 0, len(in)+len(in)/2)
	for i, r := range in {
		if unicode.IsUpper(r) {
			r = unicode.ToLower(r)
			if i > 0 && in[i-1] != '_' && (isLower(i-1) || isLower(i+1)) {
				out = append(out, '_')
			}
		}
		out = append(out, r)
	}
	return string(out)
}
