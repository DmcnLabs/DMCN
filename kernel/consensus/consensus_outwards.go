package consensus

import (
	"github.com/xuperchain/xupercore/kernel/common/xcontext"
	"github.com/xuperchain/xupercore/kernel/consensus/base"
	cctx "github.com/xuperchain/xupercore/kernel/consensus/context"
)

// ConsensusInterface
type ConsensusInterface interface {
	// CompeteMaster
	CompeteMaster(height int64) (bool, bool, error)
	// CheckMinerMatch
	CheckMinerMatch(ctx xcontext.XContext, block cctx.BlockInterface) (bool, error)
	// ProcessBeforeMiner
	ProcessBeforeMiner(timestamp int64) ([]byte, []byte, error)
	// CalculateBlock
	CalculateBlock(block cctx.BlockInterface) error
	// ProcessConfirmBlock
	ProcessConfirmBlock(block cctx.BlockInterface) error
	// GetStatus
	GetConsensusStatus() (base.ConsensusStatus, error)
}
