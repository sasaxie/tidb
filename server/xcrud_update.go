package server

import (
	"github.com/pingcap/tidb/xprotocol/expr"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Crud"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

type updateBuilder struct {
	baseBuilder
}

func (ub *updateBuilder) build(payload []byte) (*string, error) {
	var msg Mysqlx_Crud.Update
	var isRelation bool

	if err := msg.Unmarshal(payload); err != nil {
		return nil, util.ErrXBadMessage
	}

	if msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE {
		isRelation = true
	}

	sqlQuery := "UPDATE "
	sqlQuery += *ub.addCollection(msg.GetCollection())
	generatedField, err := ub.addOpetration(msg.GetOperation(), isRelation)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	generatedField, err = ub.addFilter(msg.GetCriteria())
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	generatedField, err = ub.addOrder(msg.GetOrder())
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	generatedField, err = ub.addLimit(msg.GetLimit(), true)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	return &sqlQuery, nil
}

func (ub *updateBuilder) addOpetration(operations []*Mysqlx_Crud.UpdateOperation,
	tableDataMode bool) (*string, error) {
	if len(operations) == 0 {
		return nil, util.ErrXBadUpdateData.GenByArgs("Invalid update expression list")
	}

	target := " SET "

	var generatedField *string
	var err error
	if tableDataMode {
		generatedField, err = ub.addTableOperation(operations)
	} else {
		generatedField, err = ub.addDocumentOperation(operations)
	}

	if err != nil {
		return nil, err
	}
	target += *generatedField

	return &target, nil
}

func (ub *updateBuilder) addTableOperation(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	begin := 0
	end := findIfNotEqual(operations)
	generatedField, err := ub.addTableOperationItems(operations[0:end])
	if err != nil {
		return nil, err
	}
	target := *generatedField

	for {
		if end == len(operations)-1 {
			break
		}

		begin = end
		end = findIfNotEqual(operations[begin:])
		generatedField, err = ub.addTableOperationItems(operations[begin:end])
		if err != nil {
			return nil, err
		}
		target += ","
		target += *generatedField
	}

	return &target, nil
}

func (ub *updateBuilder) addTableOperationItems(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	begin := operations[0]
	if begin.GetSource().GetSchemaName() != "" ||
		begin.GetSource().GetTableName() != "" ||
		begin.GetSource().GetName() == "" {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	switch begin.GetOperation() {
	case Mysqlx_Crud.UpdateOperation_SET:
		if len(begin.GetSource().GetDocumentPath()) != 0 {
			return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
		}

		gen, err := expr.AddForEach(operations, addFieldWithValue, 0, ",")
		if err != nil {
			return nil, err
		}
		target += *gen

		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_REMOVE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addMember, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_SET:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_SET("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addMemberWithValue, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_REPLACE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_REPLACE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addMemberWithValue, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_MERGE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addValue, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ARRAY_INSERT:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_ARRAY_INSERT("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addMemberWithValue, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ARRAY_APPEND:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_ARRAY_APPEND("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		gen, err := expr.AddForEach(operations, addMemberWithValue, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	default:
		return nil, util.ErrXBadTypeOfUpdate.GenByArgs("Invalid type of update operations for table")
	}
}

func addMember(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	if len(operation.GetSource().GetDocumentPath()) == 0 {
		return nil, util.ErrXBadMemberToUpdate.GenByArgs("Invalid member location")
	}

	target := ","

	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource().GetDocumentPath(), false, nil, nil))
	if err != nil {
		return nil, err
	}
	target += *gen

	return &target, nil
}

func addValue(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ","
	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), false, nil, nil))
	if err != nil {
		return nil, err
	}
	target += *gen

	return &target, nil
}

func addMemberWithValue(c interface{}) (*string, error) {
	gen, err := addMember(c)
	if err != nil {
		return nil, err
	}
	target := *gen

	gen, err = addValue(c)
	if err != nil {
		return nil, err
	}
	target += *gen
	return &target, nil
}

func addFieldWithValue(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource(), false, nil, nil))
	if err != nil {
		return nil, err
	}

	target += *gen + "="

	gen, err = expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), false, nil, nil))
	if err != nil {
		return nil, err
	}

	target += *gen
	return &target, nil

}

func findIfNotEqual(operation []*Mysqlx_Crud.UpdateOperation) int {
	if len(operation) == 1 {
		return 0
	}
	b := operation[0]
	for i, op := range operation[1:] {
		if op.GetSource().GetName() != b.GetSource().GetName() &&
			op.GetOperation() != b.GetOperation() {
			return i + 1
		}
	}
	return len(operation) - 1
}

func (ub *updateBuilder) addDocumentOperation(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	prev := Mysqlx_Crud.UpdateOperation_UpdateType(-1)
	target := "doc="

	for _, op := range operations {
		if prev == op.GetOperation() {
			continue
		}

		switch op.GetOperation() {
		case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
			target += "JSON_REMOVE("
		case Mysqlx_Crud.UpdateOperation_ITEM_SET:
			target += "JSON_SET("
		case Mysqlx_Crud.UpdateOperation_ITEM_REPLACE:
			target += "JSON_REPLACE("
		case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
			target += "JSON_MERGE("
		case Mysqlx_Crud.UpdateOperation_ARRAY_INSERT:
			target += "JSON_ARRAY_INSERT("
		case Mysqlx_Crud.UpdateOperation_ARRAY_APPEND:
			target += "JSON_ARRAY_APPEND("
		default:
			return nil, util.ErrXBadTypeOfUpdate.GenByArgs("Invalid type of update operations for document")
		}
		prev = op.GetOperation()
	}
	target += "doc"

	bi := 0
	prev = operations[0].GetOperation()
	for i, op := range operations {
		if prev == op.GetOperation() {
			continue
		}

		gen, err := expr.AddForEach(operations[bi:i-1], addDocumentOperationItem, 0, "")
		if err != nil {
			return nil, err
		}
		target += *gen + ")"

		bi = i
		prev = op.GetOperation()
	}

	gen, err := expr.AddForEach(operations[bi:], addDocumentOperationItem, 0, "")
	if err != nil {
		return nil, err
	}
	target += *gen + ")"

	return &target, nil
}

func addDocumentOperationItem(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	if operation.GetSource().GetSchemaName() != "" ||
		operation.GetSource().GetTableName() != "" ||
		operation.GetSource().GetName() != "" {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	if operation.GetOperation() != Mysqlx_Crud.UpdateOperation_ITEM_MERGE {
		if len(operation.GetSource().GetDocumentPath()) == 0 ||
			(operation.GetSource().GetDocumentPath()[0].GetType() != Mysqlx_Expr.DocumentPathItem_MEMBER &&
				operation.GetSource().GetDocumentPath()[0].GetType() != Mysqlx_Expr.DocumentPathItem_MEMBER_ASTERISK) {
			return nil, util.ErrXBadMemberToUpdate.GenByArgs("Invalid document member location")
		}

		if len(operation.GetSource().GetDocumentPath()) == 1 &&
			operation.GetSource().GetDocumentPath()[0].GetType() == Mysqlx_Expr.DocumentPathItem_MEMBER &&
			operation.GetSource().GetDocumentPath()[0].GetValue() == "_id" {
			return nil, util.ErrXBadColumnToUpdate.GenByArgs("Forbidden update operation on '$._id' member")
		}
		target += ","

		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource().GetDocumentPath(), false, nil, nil))
		if err != nil {
			return nil, err
		}
		target += *gen
	}

	switch operation.GetOperation() {
	case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
		if operation.GetValue() != nil {
			return nil, util.ErrXBadUpdateData.GenByArgs("Unexpected value argument for ITEM_REMOVE operation")
		}
	case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), false, nil, nil))
		if err != nil {
			return nil, err
		}
		target += ",IF(JSON_TYPE(" + *gen + ")='OBJECT',JSON_REMOVE(" + *gen + ",'$._id'),'_ERROR_')"
	default:
		target += ","

		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), false, nil, nil))
		if err != nil {
			return nil, err
		}
		target += *gen
	}

	return &target, nil
}
