package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-language-exconf-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-language-exconf-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-language-exconf-rmq-kube/database"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

type ExistenceConf struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewExistenceConf(ctx context.Context, db *database.Mysql, l *logger.Logger) *ExistenceConf {
	return &ExistenceConf{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (e *ExistenceConf) Conf(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.Language {
	language := *input.Language.Language
	notKeyExistence := make([]string, 0, 1)
	KeyExistence := make([]string, 0, 1)

	existData := &dpfm_api_output_formatter.Language{
		Language:      language,
		ExistenceConf: false,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !e.confLanguage(language) {
			notKeyExistence = append(notKeyExistence, language)
			return
		}
		KeyExistence = append(KeyExistence, language)
	}()

	wg.Wait()

	if len(KeyExistence) == 0 {
		return existData
	}
	if len(notKeyExistence) > 0 {
		return existData
	}

	existData.ExistenceConf = true
	return existData
}

func (e *ExistenceConf) confLanguage(val string) bool {
	rows, err := e.db.Query(
		`SELECT Language 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_language_language_data 
		WHERE Language = ?;`, val,
	)
	if err != nil {
		e.l.Error(err)
		return false
	}

	for rows.Next() {
		var language string
		err := rows.Scan(&language)
		if err != nil {
			e.l.Error(err)
			continue
		}
		if language == val {
			return true
		}
	}
	return false
}
