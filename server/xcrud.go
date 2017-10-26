package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
)

type builder interface {
	build([]byte) (*string, error)
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
	log.Warnf("[XUWT] unknown crud builder type %d", msgType)
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
		return err
	}

	sqlQuery, err = builder.build(payload)
	if err != nil {
		log.Warnf("[XUWT] error occurs when build msg %d", msgType)
		return err
	}

	log.Infof("[XUWT] mysqlx reported 'CRUD query: %s'", *sqlQuery)
	_, err = crud.ctx.Execute(*sqlQuery)
	if err != nil {
		return err
	}
	return notice.SendNoticeOK(crud.pkt, "ok")
}

func createCrud(xcc *mysqlXClientConn) *xCrud {
	return &xCrud{
		ctx:   xcc.ctx,
		pkt:   xcc.pkt,
		alloc: xcc.alloc,
	}
}
