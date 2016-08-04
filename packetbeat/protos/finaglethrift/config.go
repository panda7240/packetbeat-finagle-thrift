package finaglethrift

import (
	"github.com/elastic/beats/packetbeat/config"
	"github.com/elastic/beats/packetbeat/protos"
)

type thriftConfig struct {
	config.ProtocolCommon  `config:",inline"`
	StringMaxSize          int      `config:"string_max_size"`
	CollectionMaxSize      int      `config:"collection_max_size"`
	DropAfterNStructFields int      `config:"drop_after_n_struct_fields"`
	TransportType          string   `config:"transport_type"`
	ProtocolType           string   `config:"protocol_type"`
	CaptureReply           bool     `config:"capture_reply"`
	ObfuscateStrings       bool     `config:"obfuscate_strings"`
	IdlFiles               []string `config:"idl_files"`
	DiggerLogLevel         string   `config:"digger_log_level"`
	DiggerEnabled          bool     `config:"digger_enabled"`
	IdlFilePath            string   `config:"idl_file_path"`
}

var (
	defaultConfig = thriftConfig{
		ProtocolCommon: config.ProtocolCommon{
			TransactionTimeout: protos.DefaultTransactionExpiration,
		},
		StringMaxSize:          200,
		CollectionMaxSize:      15,
		DropAfterNStructFields: 500,
		TransportType:          "socket",
		ProtocolType:           "binary",
		CaptureReply:           true,
		DiggerEnabled:          true,
		DiggerLogLevel:         "error",
	}
)
