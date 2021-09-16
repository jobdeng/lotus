package auto

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

var TasksLimitTable = map[sealtasks.TaskType]map[abi.RegisteredSealProof]TasksLimit{
	sealtasks.TTAddPiece: {
		abi.RegisteredSealProof_StackedDrg64GiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg32GiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg512MiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg2KiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg8MiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
	},
	sealtasks.TTPreCommit1: {
		abi.RegisteredSealProof_StackedDrg64GiBV1: TasksLimit{
			Assigned: 0,
			Request: 1,
		},
		abi.RegisteredSealProof_StackedDrg32GiBV1: TasksLimit{
			Assigned: 0,
			Request: 1,
		},
		abi.RegisteredSealProof_StackedDrg512MiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg2KiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
		abi.RegisteredSealProof_StackedDrg8MiBV1: TasksLimit{
			Assigned: 1,
			Request: 0,
		},
	},
	sealtasks.TTPreCommit2: {
		abi.RegisteredSealProof_StackedDrg64GiBV1: TasksLimit{
			Assigned: 0,
			Request: 6,
		},
		abi.RegisteredSealProof_StackedDrg32GiBV1: TasksLimit{
			Assigned: 0,
			Request: 12,
		},
		abi.RegisteredSealProof_StackedDrg512MiBV1: TasksLimit{
			Assigned: 0,
			Request: 12,
		},
		abi.RegisteredSealProof_StackedDrg2KiBV1: TasksLimit{
			Assigned: 0,
			Request: 12,
		},
		abi.RegisteredSealProof_StackedDrg8MiBV1: TasksLimit{
			Assigned: 0,
			Request: 12,
		},
	},
	sealtasks.TTCommit2: {
		abi.RegisteredSealProof_StackedDrg64GiBV1: TasksLimit{
			Assigned: 3,
			Request: 6,
		},
		abi.RegisteredSealProof_StackedDrg32GiBV1: TasksLimit{
			Assigned: 3,
			Request: 6,
		},
		abi.RegisteredSealProof_StackedDrg512MiBV1: TasksLimit{
			Assigned: 3,
			Request: 6,
		},
		abi.RegisteredSealProof_StackedDrg2KiBV1: TasksLimit{
			Assigned: 3,
			Request: 6,
		},
		abi.RegisteredSealProof_StackedDrg8MiBV1: TasksLimit{
			Assigned: 3,
			Request: 6,
		},
	},
}
