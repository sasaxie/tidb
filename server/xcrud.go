// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Crud"
)

type builder interface {
	build([]byte) (*string, error)
}

type baseBuilder struct{}

func (b *baseBuilder) build(payload []byte) (*string, error) {
	panic("method bulid of baseBulider should not be called directly")
}

func (b *baseBuilder) addCollection(c *Mysqlx_Crud.Collection) *string {
	target := util.QuoteIdentifier(*c.Schema)
	target += "."
	target += util.QuoteIdentifier(*c.Name)
	return &target
}

func (crud *xCrud) createCrudBuilder(msgType Mysqlx.ClientMessages_Type) (builder, error) {
	switch msgType {
	case Mysqlx.ClientMessages_CRUD_FIND:
	case Mysqlx.ClientMessages_CRUD_INSERT:
		return &insertBuilder{}, nil
	case Mysqlx.ClientMessages_CRUD_UPDATE:
	case Mysqlx.ClientMessages_CRUD_DELETE:
	case Mysqlx.ClientMessages_CRUD_CREATE_VIEW:
	case Mysqlx.ClientMessages_CRUD_MODIFY_VIEW:
	case Mysqlx.ClientMessages_CRUD_DROP_VIEW:
	default:
		return nil, util.ErrXBadMessage
	}
	// @TODO should be moved to default
	log.Warnf("[XUWT] unknown crud builder type %d", msgType.String())
	return nil, util.ErrXBadMessage
}

type xCrud struct {
	ctx   QueryCtx
	pkt   *xpacketio.XPacketIO
	alloc arena.Allocator
}

func (crud *xCrud) dealCrudStmtExecute(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var sqlQuery *string
	builder, err := crud.createCrudBuilder(msgType)
	if err != nil {
		log.Warnf("[XUWT] error occurs when create builder %s", msgType.String())
		return err
	}

	sqlQuery, err = builder.build(payload)
	if err != nil {
		log.Warnf("[XUWT] error occurs when build msg %s", msgType.String())
		return err
	}

	log.Infof("[XUWT] mysqlx reported 'CRUD query: %s'", *sqlQuery)
	_, err = crud.ctx.Execute(*sqlQuery)
	if err != nil {
		return err
	}
	return SendExecOk(crud.pkt, crud.ctx.LastInsertID())
}

func createCrud(xcc *mysqlXClientConn) *xCrud {
	return &xCrud{
		ctx:   xcc.ctx,
		pkt:   xcc.pkt,
		alloc: xcc.alloc,
	}
}
