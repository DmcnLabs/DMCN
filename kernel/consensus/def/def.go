package def

const (
	SubModName = "consensus"
)

// ConsensusConfig
type ConsensusConfig struct {

	ConsensusName string `json:"name"`

	Config string `json:"config"`

	StartHeight int64 `json:"height,omitempty"`

	Index int `json:"index,omitempty"`
}
