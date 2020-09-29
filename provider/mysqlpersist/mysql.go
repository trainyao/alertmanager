package mysqlpersist

import (
	"context"
	"database/sql"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/alertmanager/provider"
)

type MysqlPersistProvider struct {
	c      *sql.Conn
	cancel context.CancelFunc
	alerts provider.Alerts
	logger log.Logger
}

func NewMysqlPersistProvider(alerts provider.Alerts, dsn string, logger log.Logger) *MysqlPersistProvider {
	return &MysqlPersistProvider{
		alerts: alerts,
		logger: logger,
	}
}

func (mp *MysqlPersistProvider) Run() {
	it := mp.alerts.Subscribe()
	var ctx context.Context
	ctx, mp.cancel = context.WithCancel(context.Background())

	itc := it.Next()
	for {
		select {
		case <-ctx.Done():
			return
		case alert := <-itc:
			level.Warn(mp.logger).Log("msg", "mpp get one alert, ready to persist",
				"alert", alert.String(),
				"start", alert.StartsAt,
				"end", alert.EndsAt,
			)
		}
	}

}

// Stop
// TODO Stop should be manually called
func (mp *MysqlPersistProvider) Stop() {
	if mp.c != nil {
		mp.c.Close()
	}

	if mp.cancel != nil {
		mp.cancel()
	}
}
