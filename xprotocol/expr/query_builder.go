package expr

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/util"
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

func addUnquoteExpr(e interface{}, isRelation bool) (*string, error) {
	gen, err := AddExpr(e, isRelation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str := "JSON_UNQUOTE(" + *gen + ")"
	return &str, nil
}

func addForEach(es []interface{}, f func(e interface{}, isRelation bool) (*string, error), offset int) (*string, error) {
	if len(es) == 0 {
		return nil, nil
	}
	var str string
	for _, e := range es[offset : len(es)-1] {
		gen, err := f(e, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		str += *gen + ","
	}
	gen, err := f(es[len(es)], false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str += *gen
	return &str, nil
}
