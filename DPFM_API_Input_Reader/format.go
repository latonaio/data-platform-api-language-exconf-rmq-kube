package dpfm_api_input_reader

import (
	"data-platform-api-language-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToLanguage() *requests.Language {
	data := sdc.Language
	return &requests.Language{
		Language: data.Language,
	}
}
