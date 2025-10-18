package service

import (
	"context"
	"runtime/debug"

	"github.com/kevin-chtw/tw_common/utils"
	"github.com/kevin-chtw/tw_proto/cproto"
	"github.com/kevin-chtw/tw_proto/sproto"
	"github.com/pitaya/tw_tourney_svr/storage"
	pitaya "github.com/topfreegames/pitaya/v3/pkg"
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/logger"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Player struct {
	component.Base
	app      pitaya.Pitaya
	handlers map[string]func(context.Context, proto.Message) (proto.Message, error)
	storage  *storage.RedisStorage
}

func NewPlayer(app pitaya.Pitaya, storage *storage.RedisStorage) *Player {
	return &Player{
		app:      app,
		handlers: make(map[string]func(context.Context, proto.Message) (proto.Message, error)),
		storage:  storage,
	}
}

func (l *Player) Init() {
	l.handlers[utils.TypeUrl(&cproto.TouneyListReq{})] = l.handleTourneyList
}

func (l *Player) Message(ctx context.Context, req *cproto.TourneyReq) (*cproto.TourneyAck, error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Log.Errorf("panic recovered %s\n %s", r, string(debug.Stack()))
		}
	}()
	logger.Log.Infof("PlayerMsg: %v", req)

	msg, err := req.Req.UnmarshalNew()
	if err != nil {
		return nil, err
	}

	if handler, ok := l.handlers[req.Req.TypeUrl]; ok {
		rsp, err := handler(ctx, msg)
		if err != nil {
			return nil, err
		}
		return l.newAccountAck(rsp)
	}

	return &cproto.TourneyAck{}, nil
}

func (l *Player) newAccountAck(msg proto.Message) (*cproto.TourneyAck, error) {
	data, err := anypb.New(msg)
	if err != nil {
		return nil, err
	}
	return &cproto.TourneyAck{Ack: data}, nil
}

// handleTourneyList 处理tourney列表查询
func (l *Player) handleTourneyList(ctx context.Context, msg proto.Message) (proto.Message, error) {
	req := msg.(*cproto.TouneyListReq)

	// 从存储中获取tourney列表
	sprotoTourneys, err := l.storage.ListTourneys(ctx, func(t *sproto.TourneyInfo) bool {
		return t.GameType == req.GameType && t.MatchType == req.MatchType
	})
	if err != nil {
		return nil, err
	}

	// convertTourneyInfo 将 sproto.TounreyInfo 转换为 cproto.TounreyInfo
	convertTourneyInfo := func(src *sproto.TourneyInfo) *cproto.TounreyInfo {
		return &cproto.TounreyInfo{
			Id:        src.Id,
			Name:      src.Name,
			GameType:  src.GameType,
			MatchType: src.MatchType,
			Diamond:   src.Diamond,
			Serverid:  src.Serverid,
			Online:    src.Online,
		}
	}

	// 转换类型
	cprotoTourneys := make([]*cproto.TounreyInfo, len(sprotoTourneys))
	for i, t := range sprotoTourneys {
		cprotoTourneys[i] = convertTourneyInfo(t)
	}

	return &cproto.TouneyListAck{
		Tounreys: cprotoTourneys,
	}, nil
}
