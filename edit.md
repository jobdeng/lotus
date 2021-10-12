# 关键优化内容

## lotus优化

lotus优化分支为：https://github.com/zhiquan911/lotus/tree/mzqdev-1.11

### 优化文件

- auto包：负责自动质押扇区的功能。检查当前worker机资源使用情况，触发worker机质押新扇区。
- extern/sector-storage/select_add_piece.go：为了保证miner质押新扇区指定的host机发能收到AP任务。
- extern/sector-storage/select_alloc_existing.go：为了worker机上有sector的数据才处理PC1和Fin任务，避免数据跨机移动，节省时间。
- extern/sector-storage/select_fix.go：为了worker机上有sector的数据才处理PC2任务，并且当前接受一个PC2任务处理，避免数据跨机移动，避免GPU并发多任务处理时偶尔死锁，节省时间。
- extern/sector-storage/sched.go：不进行多线程分派任务，避免把大量任务压死一台worker机，新增获取调度信息的公共方法。
- extern/sector-storage/sched_resources.go：新增检查worker是否可以处理任务请求的方法，用于自动质押扇区可以访问。
- extern/sector-storage/sched_worker.go：schedWorker新增字段，纪录worker当前正在处理的任务及其状态。
- extern/sector-storage/stores/local.go：按照七牛云挂载云盘方案，做适配的代码修改。
- extern/storage-sealing/garbage.go：PledgeSector方法增加了pledgeHostname参数，让自动质押扇区时，可以指定worker机。
- extern/storage-sealing/states_sealing.go：在发送AP任务时，把pledgeHostname注入到Context，使得select_add_piece.go能够判断。
- extern/storage-sealing/states_failed.go：SealPrecommit1Failed状态的扇区都不重试，直接删除。

## bellperson优化

优化分支为：https://github.com/zhiquan911/bellperson/tree/mzqdev-0.14

该项目是C2任务时使用的算法，此优化版本改进了一台机多个GPU可以独立锁定处理C2任务。

### 优化文件

- src/gpu/utils.rs：新增get_lock_gpu_device()方法，可以通过环境变量`BELLMAN_LOCK_GPU_DEVICE_BUS_ID`的值来锁定某个GPU。
- src/gpu/fft.rs：在处理FFT阶段时，不锁死机器的首个GPU，而是获取get_lock_gpu_device()的GPU来处理。
- src/gpu/multiexp.rs：在处理Multiexp阶段时，不锁死机器所有GPU，而是获取get_lock_gpu_device()的GPU来处理。

### 如何查看`BUS_ID`

在主机执行：nvidia-smi，表格第二列Bus-Id，每行GPU信息的00000000:1C:00.0值中`:`间隔的第二值，`1C`就是BUS_ID。
