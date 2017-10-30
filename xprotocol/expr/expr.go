package expr

import (
	"strconv"

	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

type generator interface {
	generate(*queryBuilder) (*queryBuilder, error)
}

type expr struct {
	isRelation bool
	expr       *Mysqlx_Expr.Expr
}

func (e *expr) generate(qb *queryBuilder) (*queryBuilder, error) {
	var g generator

	expr := e.expr
	switch expr.GetType() {
	case Mysqlx_Expr.Expr_IDENT:
		g = &ident{e.isRelation, expr.GetIdentifier()}
	case Mysqlx_Expr.Expr_LITERAL:
		g = &scalar{expr.GetLiteral()}
	case Mysqlx_Expr.Expr_VARIABLE:
		g = &variable{expr.GetVariable()}
	case Mysqlx_Expr.Expr_FUNC_CALL:
		g = &funcCall{expr.GetFunctionCall()}
	case Mysqlx_Expr.Expr_OPERATOR:
		g = &operator{expr.GetOperator()}
	case Mysqlx_Expr.Expr_PLACEHOLDER:
		g = &placeHolder{expr.GetPosition()}
	case Mysqlx_Expr.Expr_OBJECT:
		g = &object{expr.GetObject()}
	case Mysqlx_Expr.Expr_ARRAY:
		g = &array{expr.GetArray()}
	default:
		return nil, util.ErrXBadMessage
	}
	return g.generate(qb)
}

type ident struct {
	isRelation bool
	identifier *Mysqlx_Expr.ColumnIdentifier
}

func (i *ident) generate(qb *queryBuilder) (*queryBuilder, error) {
	schemaName := i.identifier.GetSchemaName()
	tableName := i.identifier.GetTableName()

	if schemaName != "" && tableName == "" {
		return nil, util.ErrorMessage(util.CodeErrXExprMissingArg,
			"Table name is required if schema name is specified in ColumnIdentifier.")
	}

	docPath := i.identifier.GetDocumentPath()
	name := i.identifier.GetName()
	if tableName == "" && name == "" && i.isRelation && (len(docPath) > 0) {
		return nil, util.ErrorMessage(util.CodeErrXExprMissingArg,
			"Column name is required if table name is specified in ColumnIdentifier.")
	}

	if len(docPath) > 0 {
		qb.put("JSON_EXTRACT(")
	}

	if schemaName != "" {
		qb.put(util.QuoteIdentifier(schemaName)).dot()
	}

	if tableName != "" {
		qb.put(util.QuoteIdentifier(tableName)).dot()
	}

	if name != "" {
		qb.put(util.QuoteIdentifier(name))
	}

	if len(docPath) > 0 {
		if name == "" {
			qb = qb.put("doc")
		}

		qb.put(",")
		generatedQuery, err := AddExpr(docPath, i.isRelation)
		if err != nil {
			return nil, err
		}
		qb.put(*generatedQuery)
		qb.put(")")
	}
	return qb, nil
}

type scalar struct {
	scalar *Mysqlx_Datatypes.Scalar
}

func (l *scalar) generate(qb *queryBuilder) (*queryBuilder, error) {
	literal := l.scalar
	switch literal.GetType() {
	case Mysqlx_Datatypes.Scalar_V_UINT:
		return qb.put(literal.GetVUnsignedInt()), nil
	case Mysqlx_Datatypes.Scalar_V_SINT:
		return qb.put(literal.GetVSignedInt()), nil
	case Mysqlx_Datatypes.Scalar_V_NULL:
		return qb.put("NULL"), nil
	case Mysqlx_Datatypes.Scalar_V_OCTETS:
		generatedQuery, err := AddExpr(literal.GetVOctets(), false)
		if err != nil {
			return nil, err
		}
		return qb.put(*generatedQuery), nil
	case Mysqlx_Datatypes.Scalar_V_STRING:
		if literal.GetVString().GetCollation() != 0 {
			//TODO: see line No. 231 in expr_generator.cc if the mysql's codes
		}
		return qb.QuoteString(string(literal.GetVString().GetValue())), nil
	case Mysqlx_Datatypes.Scalar_V_DOUBLE:
		return qb.put(literal.GetVDouble()), nil
	case Mysqlx_Datatypes.Scalar_V_FLOAT:
		return qb.put(literal.GetVFloat()), nil
	case Mysqlx_Datatypes.Scalar_V_BOOL:
		if literal.GetVBool() {
			return qb.put("TRUE"), nil
		}
		return qb.put("FALSE"), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid value for Mysqlx::Datatypes::Scalar::Type "+literal.GetType().String())
	}
}

type variable struct {
	variable string
}

func (v *variable) generate(qb *queryBuilder) (*queryBuilder, error) {
	return nil, nil
}

type funcCall struct {
	functionCall *Mysqlx_Expr.FunctionCall
}

func (fc *funcCall) generate(qb *queryBuilder) (*queryBuilder, error) {
	return nil, nil
}

type placeHolder struct {
	position uint32
}

func (ph *placeHolder) generate(qb *queryBuilder) (*queryBuilder, error) {
	return nil, nil
}

type object struct {
	object *Mysqlx_Expr.Object
}

func (ob *object) generate(qb *queryBuilder) (*queryBuilder, error) {
	return nil, nil
}

type array struct {
	array *Mysqlx_Expr.Array
}

func (a *array) generate(qb *queryBuilder) (*queryBuilder, error) {
	return nil, nil
}

type docPathArray struct {
	docPath []*Mysqlx_Expr.DocumentPathItem
}

func (d *docPathArray) generate(qb *queryBuilder) (*queryBuilder, error) {
	docPath := d.docPath
	if len(docPath) == 1 &&
		docPath[0].GetType() == Mysqlx_Expr.DocumentPathItem_MEMBER &&
		docPath[0].GetValue() == "" {
		qb.put(util.QuoteIdentifier("$"))
		return qb, nil
	}

	qb.Bquote().put("$")
	for _, item := range docPath {
		switch item.GetType() {
		case Mysqlx_Expr.DocumentPathItem_MEMBER:
			if item.GetValue() == "" {
				return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
					"Invalid empty value for Mysqlx::Expr::DocumentPathItem::MEMBER")
			}
			qb.put(util.QuoteIdentifierIfNeeded(item.GetValue()))
		case Mysqlx_Expr.DocumentPathItem_MEMBER_ASTERISK:
			qb.put(".*")
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX:
			qb.put("[").put(item.GetIndex()).put("]")
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX_ASTERISK:
			qb.put("[*]")
		case Mysqlx_Expr.DocumentPathItem_DOUBLE_ASTERISK:
			qb.put("**")
		default:
			return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
				"Invalid value for Mysqlx::Expr::DocumentPathItem::Type ")
		}
	}

	qb.Equote()
	return qb, nil
}

const (
	ctPlain    = 0x0000 //   default value; general use of octets
	ctGeometry = 0x0001 //   BYTES  0x0001 GEOMETRY (WKB encoding)
	ctJSON     = 0x0002 //   BYTES  0x0002 JSON (text encoding)
	ctXML      = 0x0003 //   BYTES  0x0003 XML (text encoding)
)

type scalarOctets struct {
	scalarOctets *Mysqlx_Datatypes.Scalar_Octets
}

func (so *scalarOctets) generate(qb *queryBuilder) (*queryBuilder, error) {
	scalarOctets := so.scalarOctets
	content := string(scalarOctets.GetValue())
	switch scalarOctets.GetContentType() {
	case ctPlain:
		return qb.QuoteString(content), nil
	case ctGeometry:
		return qb.put("ST_GEOMETRYFROMWKB(").QuoteString(content).put(")"), nil
	case ctJSON:
		return qb.put("CAST(").QuoteString(content).put(" AS JSON)"), nil
	case ctXML:
		return qb.QuoteString(content), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid content type for Mysqlx::Datatypes::Scalar::Octets "+
				strconv.FormatUint(uint64(scalarOctets.GetContentType()), 10))
	}
}

type any struct {
	any *Mysqlx_Datatypes.Any
}

func (a *any) generate(qb *queryBuilder) (*queryBuilder, error) {
	any := a.any
	switch any.GetType() {
	case Mysqlx_Datatypes.Any_SCALAR:
		generatedQuery, err := AddExpr(any.GetScalar(), false)
		if err != nil {
			return nil, err
		}
		return qb.put(*generatedQuery), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid value for Mysqlx::Datatypes::Any::Type "+
				strconv.Itoa(int(any.GetType())))
	}
}

// AddExpr executes add operation.
func AddExpr(e interface{}, isRelation bool) (*string, error) {
	var g generator

	switch e.(type) {
	case *Mysqlx_Expr.Expr:
		g = &expr{isRelation, e.(*Mysqlx_Expr.Expr)}
	case []*Mysqlx_Expr.DocumentPathItem:
		g = &docPathArray{e.([]*Mysqlx_Expr.DocumentPathItem)}
	case *Mysqlx_Datatypes.Any:
		g = &any{e.(*Mysqlx_Datatypes.Any)}
	case *Mysqlx_Datatypes.Scalar:
		g = &scalar{e.(*Mysqlx_Datatypes.Scalar)}
	case *Mysqlx_Datatypes.Scalar_Octets:
		g = &scalarOctets{e.(*Mysqlx_Datatypes.Scalar_Octets)}
	default:
		return nil, util.ErrXBadMessage
	}

	qb, err := g.generate(&queryBuilder{"", false, false})
	return &qb.str, err
}
