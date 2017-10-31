package expr

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

type generator interface {
	generate(*queryBuilder) (*queryBuilder, error)
}

type expr struct {
	expr       *Mysqlx_Expr.Expr
	args       []*Mysqlx_Datatypes.Scalar
	isRelation bool
}

func (e *expr) generate(qb *queryBuilder) (*queryBuilder, error) {
	var g generator

	expr := e.expr
	switch expr.GetType() {
	case Mysqlx_Expr.Expr_IDENT:
		g = &columnIdent{expr.GetIdentifier(), e.isRelation}
	case Mysqlx_Expr.Expr_LITERAL:
		g = &scalar{expr.GetLiteral()}
	case Mysqlx_Expr.Expr_VARIABLE:
		g = &variable{expr.GetVariable()}
	case Mysqlx_Expr.Expr_FUNC_CALL:
		g = &funcCall{expr.GetFunctionCall()}
	case Mysqlx_Expr.Expr_OPERATOR:
		g = &operator{expr.GetOperator()}
	case Mysqlx_Expr.Expr_PLACEHOLDER:
		g = &placeHolder{expr.GetPosition(), e.args}
	case Mysqlx_Expr.Expr_OBJECT:
		g = &object{expr.GetObject()}
	case Mysqlx_Expr.Expr_ARRAY:
		g = &array{expr.GetArray()}
	default:
		return nil, util.ErrXBadMessage
	}
	return g.generate(qb)
}

type columnIdent struct {
	identifier *Mysqlx_Expr.ColumnIdentifier
	isRelation bool
}

func (i *columnIdent) generate(qb *queryBuilder) (*queryBuilder, error) {
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
		generatedQuery, err := AddExpr(&ConcatExpr{docPath, i.isRelation, nil, nil})
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
		generatedQuery, err := AddExpr(&ConcatExpr{literal.GetVOctets(), false, nil, nil})
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
	return qb.put(v.variable), nil
}

type ident struct {
	ident         *Mysqlx_Expr.Identifier
	isFunction    bool
	defaultSchema string
}

func (i *ident) generate(qb *queryBuilder) (*queryBuilder, error) {
	ident := i.ident
	if i.defaultSchema != "" && ident.GetSchemaName() == "" &&
		(!i.isFunction || expression.IsBuiltInFunc(ident.GetName())) {
		qb.put(util.QuoteIdentifierIfNeeded(i.defaultSchema)).dot()
	}

	if ident.GetSchemaName() != "" {
		qb.put(util.QuoteIdentifier(ident.GetSchemaName())).dot()
	}

	qb.put(util.QuoteIdentifierIfNeeded(ident.GetName()))

	return qb, nil
}

type funcCall struct {
	functionCall *Mysqlx_Expr.FunctionCall
}

func (fc *funcCall) generate(qb *queryBuilder) (*queryBuilder, error) {
	functionCall := fc.functionCall

	generatedQuery, err := AddExpr(&ConcatExpr{functionCall.GetName(), true, nil, nil})
	if err != nil {
		return nil, err
	}
	qb.put(*generatedQuery)
	qb.put("(")

	for _, expr := range functionCall.GetParam() {
		generatedQuery, err := AddExpr(&ConcatExpr{expr, true, nil, nil})
		if err != nil {
			return nil, err
		}

		if expr.GetType() == Mysqlx_Expr.Expr_IDENT && len(expr.Identifier.GetDocumentPath()) > 0 {
			qb.put("JSON_UNQUOTE(").put(*generatedQuery).put(")")
		} else {
			qb.put(*generatedQuery).put(",")
		}
	}

	qb.put(")")
	return qb, nil
}

type placeHolder struct {
	position uint32
	msg      []*Mysqlx_Datatypes.Scalar
}

func (ph *placeHolder) generate(qb *queryBuilder) (*queryBuilder, error) {
	position := ph.position
	msg := ph.msg
	if position < uint32(len(msg)) {
		generatedQuery, err := AddExpr(&ConcatExpr{msg[position], true, nil, nil})
		if err != nil {
			return nil, err
		}
		return qb.put(generatedQuery), nil
	}
	return nil, util.ErrXExprBadValue.GenByArgs("Invalid value of placeholder")
}

type objectField struct {
	objectField *Mysqlx_Expr.Object_ObjectField
}

func (ob *objectField) generate(qb *queryBuilder) (*queryBuilder, error) {
	objectField := ob.objectField
	if objectField.GetKey() == "" {
		return nil, util.ErrXExprBadValue.GenByArgs("Invalid key for Mysqlx::Expr::Object")
	}

	if objectField.GetValue() == nil {
		return nil, util.ErrXExprBadValue.GenByArgs("Invalid value for Mysqlx::Expr::Object on key '" + objectField.GetKey() + "'")
	}
	qb.QuoteString(objectField.GetKey()).put(",")

	generatedQuery, err := AddExpr(&ConcatExpr{objectField.GetValue(), false, nil, nil})
	if err != nil {
		return nil, err
	}
	qb.put(*generatedQuery)

	return qb, nil
}

type object struct {
	object *Mysqlx_Expr.Object
}

func (ob *object) generate(qb *queryBuilder) (*queryBuilder, error) {
	qb.put("JSON_OBJECT(")
	fields := ob.object.GetFld()
	cs := make([]*ConcatExpr, len(fields))
	for i, d := range fields {
		cs[i] = &ConcatExpr{d, false, nil, nil}
	}
	gen, err := AddForEach(cs, AddExpr, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

type array struct {
	array *Mysqlx_Expr.Array
}

func (a *array) generate(qb *queryBuilder) (*queryBuilder, error) {
	qb.put("JSON_ARRAY(")
	values := a.array.GetValue()
	cs := make([]*ConcatExpr, len(values))
	for i, d := range values {
		cs[i] = &ConcatExpr{d, false, nil, nil}
	}
	gen, err := AddForEach(cs, AddExpr, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
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
			qb.put(".")
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
		generatedQuery, err := AddExpr(&ConcatExpr{any.GetScalar(), false, nil, nil})
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
