package consensus

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/xuperchain/xupercore/kernel/common/xcontext"
	"github.com/xuperchain/xupercore/kernel/consensus/base"
	common "github.com/xuperchain/xupercore/kernel/consensus/base/common"
	cctx "github.com/xuperchain/xupercore/kernel/consensus/context"
	"github.com/xuperchain/xupercore/kernel/consensus/def"
	"github.com/xuperchain/xupercore/kernel/contract"
)

const (
	// pluggable_consensus
	// contractUpdateMethod
	contractUpdateMethod = "updateConsensus"
	// pluggable_consensus

	// <"PluggableConfig", configJson>
	// <index, consensusJson<STRING>> ，eg. <"1", "{"name":"pow", "config":"{}", "beginHeight":"100"}">
	// consensusJson
	// <name, consensusName<STRING>>
	// <config, configJson<STRING>>
	// <beginHeight, height>
	contractBucket = "$consensus"
	consensusKey   = "PluggableConfig"
)

var (
	EmptyConsensusListErr = errors.New("Consensus list of PluggableConsensus is empty.")
	EmptyConsensusName    = errors.New("Consensus name can not be empty")
	EmptyConfig           = errors.New("Config name can not be empty")
	UpdateTriggerError    = errors.New("Update trigger height invalid")
	BeginBlockIdErr       = errors.New("Consensus begin blockid err")
	BuildConsensusError   = errors.New("Build consensus Error")
	ConsensusNotRegister  = errors.New("Consensus hasn't been register. Please use consensus.Register({NAME},{FUNCTION_POINTER}) to register in consensusMap")
	ContractMngErr        = errors.New("Contract manager is empty.")
)

// PluggableConsensus consensus_interface
type PluggableConsensus struct {
	ctx           cctx.ConsensusCtx
	stepConsensus *stepConsensus
}

// NewPluggableConsensus 初次创建PluggableConsensus实例，初始化cons列表
func NewPluggableConsensus(cCtx cctx.ConsensusCtx) (ConsensusInterface, error) {
	if cCtx.BcName == "" {
		cCtx.XLog.Error("Pluggable Consensus::NewPluggableConsensus::bcName is empty.")
	}
	pc := &PluggableConsensus{
		ctx: cCtx,
		stepConsensus: &stepConsensus{
			cons: []ConsensusInterface{},
		},
	}
	if cCtx.Contract.GetKernRegistry() == nil {
		return nil, ContractMngErr
	}

	cCtx.Contract.GetKernRegistry().RegisterKernMethod(contractBucket, contractUpdateMethod, pc.updateConsensus)
	xMReader, err := cCtx.Ledger.GetTipXMSnapshotReader()
	if err != nil {
		return nil, err
	}
	res, err := xMReader.Get(contractBucket, []byte(consensusKey))

	if res == nil {
		consensusBuf, err := cCtx.Ledger.GetConsensusConf()
		if err != nil {
			return nil, err
		}

		cfg := def.ConsensusConfig{}
		err = json.Unmarshal(consensusBuf, &cfg)
		if err != nil {
			cCtx.XLog.Error("Pluggable Consensus::NewPluggableConsensus::parse consensus configuration error!", "conf", string(consensusBuf), "error", err.Error())
			return nil, err
		}
		cfg.StartHeight = 1
		cfg.Index = 0
		genesisConsensus, err := pc.makeConsensusItem(cCtx, cfg)
		if err != nil {
			cCtx.XLog.Error("Pluggable Consensus::NewPluggableConsensus::make first consensus item error!", "error", err.Error())
			return nil, err
		}
		pc.stepConsensus.put(genesisConsensus)

		genesisConsensus.Start()
		cCtx.XLog.Debug("Pluggable Consensus::NewPluggableConsensus::create a instance for the first time.")
		return pc, nil
	}
	// pluggable consensus
	c := map[int]def.ConsensusConfig{}
	err = json.Unmarshal(res, &c)
	if err != nil {
		//
		cCtx.XLog.Error("Pluggable Consensus::history consensus storage invalid, pls check function.")
		return nil, err
	}
	for i := 0; i < len(c); i++ {
		config := c[i]
		oldConsensus, err := pc.makeConsensusItem(cCtx, config)
		if err != nil {
			cCtx.XLog.Warn("Pluggable Consensus::NewPluggableConsensus::make old consensus item error!", "error", err.Error())
		}
		pc.stepConsensus.put(oldConsensus)
		//
		if i == len(c)-1 {
			oldConsensus.Start()
		}
		cCtx.XLog.Debug("Pluggable Consensus::NewPluggableConsensus::create a instance with history reader.", "StepConsensus", pc.stepConsensus)
	}
	return pc, nil
}

// makeConsensusItem
func (pc *PluggableConsensus) makeConsensusItem(cCtx cctx.ConsensusCtx, cCfg def.ConsensusConfig) (base.ConsensusImplInterface, error) {
	if cCfg.ConsensusName == "" {
		cCtx.XLog.Error("Pluggable Consensus::makeConsensusItem::consensus name is empty")
		return nil, EmptyConsensusName
	}
	// version check
	specificCon, err := NewPluginConsensus(pc.ctx, cCfg)
	if err != nil {
		cCtx.XLog.Error("Pluggable Consensus::NewPluginConsensus error", "error", err)
		return nil, err
	}
	if specificCon == nil {
		cCtx.XLog.Error("Pluggable Consensus::NewPluginConsensus::empty error", "error", BuildConsensusError)
		return nil, BuildConsensusError
	}
	cCtx.XLog.Debug("Pluggable Consensus::makeConsensusItem::create a consensus item.", "type", cCfg.ConsensusName)
	return specificCon, nil
}

func (pc *PluggableConsensus) proposalArgsUnmarshal(ctxArgs map[string][]byte) (*def.ConsensusConfig, error) {
	if _, ok := ctxArgs["height"]; !ok {
		return nil, UpdateTriggerError
	}
	consensusHeight, err := strconv.ParseInt(string(ctxArgs["height"]), 10, 64)
	if err != nil {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::height value invalid.", "err", err)
		return nil, err
	}
	args := make(map[string]interface{})
	err = json.Unmarshal(ctxArgs["args"], &args)
	if err != nil {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::unmarshal err.", "err", err)
		return nil, err
	}
	if _, ok := args["name"]; !ok {
		return nil, EmptyConsensusName
	}
	if _, ok := args["config"]; !ok {
		return nil, ConsensusNotRegister
	}

	consensusName, ok := args["name"].(string)
	if !ok {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::name should be string.")
		return nil, EmptyConsensusName
	}
	if _, dup := consensusMap[consensusName]; !dup {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::consensus's type invalid when update", "name", consensusName)
		return nil, ConsensusNotRegister
	}
	// consensusConfig
	consensusConfigMap, ok := args["config"].(map[string]interface{})
	if !ok {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::config should be map.")
		return nil, EmptyConfig
	}
	consensusConfigBytes, err := json.Marshal(&consensusConfigMap)
	if err != nil {
		pc.ctx.XLog.Error("Pluggable Consensus::updateConsensus::unmarshal config err.", "err", err)
		return nil, EmptyConfig
	}
	return &def.ConsensusConfig{
		ConsensusName: consensusName,
		Config:        string(consensusConfigBytes),
		Index:         pc.stepConsensus.len(),
		StartHeight:   consensusHeight,
	}, nil
}

// updateConsensus
func (pc *PluggableConsensus) updateConsensus(contractCtx contract.KContext) (*contract.Response, error) {
	//
	cfg, err := pc.proposalArgsUnmarshal(contractCtx.Args())
	if err != nil {
		return common.NewContractErrResponse(common.StatusErr, err.Error()), err
	}
	consensusItem, err := pc.makeConsensusItem(pc.ctx, *cfg)
	if err != nil {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::make consensu item error! Use old one.", "error", err.Error())
		return common.NewContractErrResponse(common.StatusErr, err.Error()), err
	}
	transCon, ok := consensusItem.(ConsensusInterface)
	if !ok {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::consensus transfer error! Use old one.")
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}
	pc.ctx.XLog.Debug("Pluggable Consensus::updateConsensus::make a new consensus item successfully during updating process.")


	pluggableConfig, err := contractCtx.Get(contractBucket, []byte(consensusKey))
	c := map[int]def.ConsensusConfig{}
	if pluggableConfig == nil {

		consensusBuf, _ := pc.ctx.Ledger.GetConsensusConf()

		config := def.ConsensusConfig{}
		_ = json.Unmarshal(consensusBuf, &config)
		config.StartHeight = 1
		config.Index = 0
		c[0] = config
	} else {
		err = json.Unmarshal(pluggableConfig, &c)
		if err != nil {
			pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::unmarshal error", "error", err)
			return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
		}
	}
	//
	if checkSameNameConsensus(c, cfg) {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::same name consensus.")
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}
	c[len(c)] = *cfg
	newBytes, err := json.Marshal(c)
	if err != nil {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::marshal error", "error", err)
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}
	if err = contractCtx.Put(contractBucket, []byte(consensusKey), newBytes); err != nil {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::refresh contract storage error", "error", err)
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}

	//
	lastCon, ok := pc.getCurrentConsensusComponent().(base.ConsensusImplInterface)
	if !ok {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::last consensus transfer error! Stop.")
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}
	lastCon.Stop()

	//
	err = pc.stepConsensus.put(transCon)
	if err != nil {
		pc.ctx.XLog.Warn("Pluggable Consensus::updateConsensus::put item into stepConsensus failed", "error", err)
		return common.NewContractErrResponse(common.StatusErr, BuildConsensusError.Error()), BuildConsensusError
	}
	//
	consensusItem.Start()
	pc.ctx.XLog.Debug("Pluggable Consensus::updateConsensus::key has been modified.", "ConsensusMap", c)
	return common.NewContractOKResponse([]byte("ok")), nil
}

// rollbackConsensus
// TODO:
func (pc *PluggableConsensus) rollbackConsensus(contractCtx contract.KContext) error {
	return nil
}

// PluggableConsensus
func (pc *PluggableConsensus) getCurrentConsensusComponent() ConsensusInterface {
	return pc.stepConsensus.tail()
}

// CompeteMaster
// param: height
func (pc *PluggableConsensus) CompeteMaster(height int64) (bool, bool, error) {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::CompeteMaster::Cannot get consensus Instance.")
		return false, false, EmptyConsensusListErr
	}
	return con.CompeteMaster(height)
}

// CheckMinerMatch
func (pc *PluggableConsensus) CheckMinerMatch(ctx xcontext.XContext, block cctx.BlockInterface) (bool, error) {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::CheckMinerMatch::tail consensus item is empty", "err", EmptyConsensusListErr)
		return false, EmptyConsensusListErr
	}
	return con.CheckMinerMatch(ctx, block)
}

// ProcessBeforeMinerm
func (pc *PluggableConsensus) ProcessBeforeMiner(timestamp int64) ([]byte, []byte, error) {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::ProcessBeforeMiner::tail consensus item is empty", "err", EmptyConsensusListErr)
		return nil, nil, EmptyConsensusListErr
	}
	return con.ProcessBeforeMiner(timestamp)
}

// CalculateBlock
func (pc *PluggableConsensus) CalculateBlock(block cctx.BlockInterface) error {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::CalculateBlock::tail consensus item is empty", "err", EmptyConsensusListErr)
		return EmptyConsensusListErr
	}
	return con.CalculateBlock(block)
}

// ProcessConfirmBlock
func (pc *PluggableConsensus) ProcessConfirmBlock(block cctx.BlockInterface) error {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::ProcessConfirmBlock::tail consensus item is empty", "err", EmptyConsensusListErr)
		return EmptyConsensusListErr
	}
	return con.ProcessConfirmBlock(block)
}

// GetConsensusStatus
func (pc *PluggableConsensus) GetConsensusStatus() (base.ConsensusStatus, error) {
	con := pc.getCurrentConsensusComponent()
	if con == nil {
		pc.ctx.XLog.Error("Pluggable Consensus::GetConsensusStatus::tail consensus item is empty", "err", EmptyConsensusListErr)
		return nil, EmptyConsensusListErr
	}
	return con.GetConsensusStatus()
}

/////////////////// stepConsensus //////////////////

// stepConsensus
type stepConsensus struct {
	cons []ConsensusInterface
	// mutex保护StepConsensus数据结构cons的读写操作
	mutex sync.RWMutex
}

// 向可插拔共识数组put item
func (sc *stepConsensus) put(con ConsensusInterface) error {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.cons = append(sc.cons, con)
	return nil
}

// 获取最新的共识实例
func (sc *stepConsensus) tail() ConsensusInterface {
	//getCurrentConsensusComponent
	sc.mutex.RLock()
	sc.mutex.RUnlock()
	if len(sc.cons) == 0 {
		return nil
	}
	return sc.cons[len(sc.cons)-1]
}

func (sc *stepConsensus) len() int {
	sc.mutex.RLock()
	sc.mutex.RUnlock()
	return len(sc.cons)
}

var consensusMap = make(map[string]NewStepConsensus)

type NewStepConsensus func(cCtx cctx.ConsensusCtx, cCfg def.ConsensusConfig) base.ConsensusImplInterface

// Register 不同类型的共识需要提前完成注册
func Register(name string, f NewStepConsensus) error {
	if f == nil {
		panic("Pluggable Consensus::Register::new function is nil")
	}
	if _, dup := consensusMap[name]; dup {
		panic("Pluggable Consensus::Register::called twice for func " + name)
	}
	consensusMap[name] = f
	return nil
}

// NewPluginConsensus 新建可插拔共识实例
func NewPluginConsensus(cCtx cctx.ConsensusCtx, cCfg def.ConsensusConfig) (base.ConsensusImplInterface, error) {
	if cCfg.ConsensusName == "" {
		return nil, EmptyConsensusName
	}
	if cCfg.StartHeight < 0 {
		return nil, BeginBlockIdErr
	}
	if f, ok := consensusMap[cCfg.ConsensusName]; ok {
		return f(cCtx, cCfg), nil
	}
	return nil, ConsensusNotRegister
}

// checkSameNameConsensus 不允许同名但配置文件不同的共识新组件
func checkSameNameConsensus(hisMap map[int]def.ConsensusConfig, cfg *def.ConsensusConfig) bool {
	for k, v := range hisMap {
		if v.ConsensusName != cfg.ConsensusName {
			continue
		}
		if v.Config == cfg.Config {
			if k != len(hisMap)-1 { // 允许回到历史共识实例
				return false
			}
			return true // 不允许相同共识配置的升级，无意义
		}
		// 比对Version和bft组件是否一致即可
		type tempStruct struct {
			EnableBFT map[string]bool `json:"bft_config,omitempty"`
		}
		var newConf tempStruct
		if err := json.Unmarshal([]byte(cfg.Config), &newConf); err != nil {
			return true
		}
		var oldConf tempStruct
		if err := json.Unmarshal([]byte(v.Config), &oldConf); err != nil {
			return true
		}
		// 共识名称相同，注意: xpos和tdpos在name上都称为tdpos，但xpos的enableBFT!=nil
		if (newConf.EnableBFT != nil && oldConf.EnableBFT != nil) || (newConf.EnableBFT == nil && oldConf.EnableBFT == nil) {
			return true //
		}
	}
	return false
}
