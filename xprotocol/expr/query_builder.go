package expr

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
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
	switch v := i.(type) {
	case int64:
		qb.str += strconv.FormatInt(v, 10)
	case uint64:
		qb.str += strconv.FormatUint(v, 10)
	case uint32:
		qb.str += strconv.FormatUint(uint64(v), 10)
	case float64:
		qb.str += strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		qb.str += strconv.FormatFloat(float64(v), 'g', -1, 64)
	case string:
		qb.str += v
	case []byte:
		if qb.inQuoted {

		} else if qb.inIdentifier {

		} else {

		}
	default:
		panic("can not put this value")
	}
	return qb
}

func (qb *queryBuilder) QuoteString(str string) *queryBuilder {
	return qb.put(util.QuoteString(str))
}

// ConcatExpr contains expressions which needed to be concat together.
type ConcatExpr struct {
	expr                 interface{}
	isRelationOrFunction bool
	defaultSchema        *string
	args                 []*Mysqlx_Datatypes.Scalar
}

// NewConcatExpr returns a new ConcatExpr pointer.
func NewConcatExpr(expr interface{}, isRelationOrFunction bool, defaultSchema *string, args []*Mysqlx_Datatypes.Scalar) *ConcatExpr {
	return &ConcatExpr{
		expr:                 expr,
		isRelationOrFunction: isRelationOrFunction,
		defaultSchema:        defaultSchema,
		args:                 args,
	}
}

// AddExpr executes add operation.
func AddExpr(c *ConcatExpr) (*string, error) {
	var g generator

	switch v := c.expr.(type) {
	case *Mysqlx_Expr.Expr:
		g = &expr{v, c.args, c.isRelationOrFunction}
	case *Mysqlx_Expr.Identifier:
		g = &ident{v, c.isRelationOrFunction, *c.defaultSchema}
	case []*Mysqlx_Expr.DocumentPathItem:
		g = &docPathArray{v}
	case *Mysqlx_Expr.Object_ObjectField:
		g = &objectField{v}
	case *Mysqlx_Datatypes.Any:
		g = &any{v}
	case *Mysqlx_Datatypes.Scalar:
		g = &scalar{v}
	case *Mysqlx_Datatypes.Scalar_Octets:
		g = &scalarOctets{v}
	default:
		return nil, util.ErrXBadMessage
	}

	qb, err := g.generate(&queryBuilder{"", false, false})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &qb.str, nil
}

func addUnquoteExpr(c *ConcatExpr) (*string, error) {
	gen, err := AddExpr(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str := "JSON_UNQUOTE(" + *gen + ")"
	return &str, nil
}

// AddForEach concats each expression.
func AddForEach(cs []*ConcatExpr, f func(c *ConcatExpr) (*string, error), offset int) (*string, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	var str string
	for _, c := range cs[offset : len(cs)-1] {
		gen, err := f(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		str += *gen + ","
	}
	gen, err := f(cs[len(cs)-1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	str += *gen
	return &str, nil
}
