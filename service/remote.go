package service

import (
	"context"
	"runtime/debug"

	"github.com/kevin-chtw/tw_common/utils"
	"github.com/kevin-chtw/tw_proto/sproto"
	"github.com/pitaya/tw_tourney_svr/storage"
	pitaya "github.com/topfreegames/pitaya/v3/pkg"
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Remote 独立的匹配服务
type Remote struct {
	component.Base
	app      pitaya.Pitaya
	handlers map[string]func(context.Context, proto.Message) (proto.Message, error)
	storage  *storage.RedisStorage
}

func NewRemote(app pitaya.Pitaya, storage *storage.RedisStorage) *Remote {
	return &Remote{
		app:      app,
		handlers: make(map[string]func(context.Context, proto.Message) (proto.Message, error)),
		storage:  storage,
	}
}

// Init 组件初始化
func (m *Remote) Init() {
	m.handlers[utils.TypeUrl(&sproto.TourneyUpdateReq{})] = m.handleTourneyUpdate
}

// handleTourneyUpdate 处理tourney更新
func (m *Remote) handleTourneyUpdate(ctx context.Context, msg proto.Message) (proto.Message, error) {
	req := msg.(*sproto.TourneyUpdateReq)
	for _, info := range req.Infos {
		if err := m.storage.UpdateTourney(ctx, info); err != nil {
			return nil, err
		}
	}
	return &sproto.TourneyAck{}, nil
}

func (m *Remote) Message(ctx context.Context, req *sproto.TourneyReq) (*sproto.TourneyAck, error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Log.Errorf("panic recovered %s\n %s", r, string(debug.Stack()))
		}
	}()
	logger.Log.Infof("match: %v", req)

	msg, err := req.Req.UnmarshalNew()
	if err != nil {
		return nil, err
	}

	if handler, ok := m.handlers[req.Req.TypeUrl]; ok {
		rsp, err := handler(ctx, msg)
		if err != nil {
			return nil, err
		}
		return m.newTourneyAck(rsp)
	}

	return &sproto.TourneyAck{}, nil
}

func (m *Remote) newTourneyAck(msg proto.Message) (*sproto.TourneyAck, error) {
	data, err := anypb.New(msg)
	if err != nil {
		return nil, err
	}
	return &sproto.TourneyAck{Ack: data}, nil
}
