package expr

import (
	"github.com/pingcap/tidb/xprotocol/util"
	"strconv"
)

type queryBuilder struct {
	str          string
	inQuoted     bool
	inIdentifier bool
}

func (qb *queryBuilder) Bquote() *queryBuilder {
	qb.str += "'"
	qb.inQuoted = true
	return qb
}

func (qb *queryBuilder) Equote() *queryBuilder {
	qb.str += "'"
	qb.inQuoted = true
	return qb
}

func (qb *queryBuilder) Bident() *queryBuilder {
	qb.str += "`"
	qb.inIdentifier = true
	return qb
}

func (qb *queryBuilder) Eident() *queryBuilder {
	qb.str += "`"
	qb.inIdentifier = true
	return qb
}

func (qb *queryBuilder) dot() *queryBuilder {
	return qb.put(".")
}

func (qb *queryBuilder) put(i interface{}) *queryBuilder {
	switch i.(type) {
	case int64:
		qb.str += strconv.FormatInt(i.(int64), 10)
	case uint64:
		qb.str += strconv.FormatUint(i.(uint64), 10)
	case uint32:
		qb.str += strconv.FormatUint(uint64(i.(uint32)), 10)
	case float64:
		qb.str += strconv.FormatFloat(i.(float64), 'g', -1, 64)
	case float32:
		qb.str += strconv.FormatFloat(float64(i.(float32)), 'g', -1, 64)
	case string:
		qb.str += i.(string)
	case []byte:
		if qb.inQuoted {

		} else if qb.inIdentifier {

		} else {

		}
	}

	return qb
}

func (qb *queryBuilder) QuoteString(str string) *queryBuilder {
	return qb.put(util.QuoteString(str))
}
