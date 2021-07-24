package testnode

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	builder "github.com/filecoin-project/lotus/node/test"
	logging "github.com/ipfs/go-log/v2"
	"testing"
	"time"
)

func init() {
	_ = logging.SetLogLevel("*", "INFO")

	policy.SetConsensusMinerMinPower(abi.NewStoragePower(2048))
	policy.SetSupportedProofTypes(abi.RegisteredSealProof_StackedDrg2KiBV1)
	policy.SetMinVerifiedDealSize(abi.NewStoragePower(256))
}

func TestPledgeSectors(t *testing.T) {
	logging.SetLogLevel("miner", "ERROR")
	logging.SetLogLevel("chainstore", "ERROR")
	logging.SetLogLevel("chain", "ERROR")
	logging.SetLogLevel("sub", "ERROR")
	logging.SetLogLevel("storageminer", "ERROR")
	logging.SetLogLevel("advmgr", "DEBUG")


	t.Run("1", func(t *testing.T) {
		TestPledgeSector(t, MockSSBuilder, 50*time.Millisecond, 1)
	})

	t.Run("100", func(t *testing.T) {
		TestPledgeSector(t, builder.MockSbBuilder, 50*time.Millisecond, 100)
	})

	t.Run("1000", func(t *testing.T) {
		if testing.Short() { // takes ~16s
			t.Skip("skipping test in short mode")
		}

		TestPledgeSector(t, builder.MockSbBuilder, 50*time.Millisecond, 1000)
	})
}
