# Enviorment set up

1. off the mod `go env -w  GO111MODULE=off`
2. change to `GPATH` to project directory `go env -w GOPATH=$HOME/coroprove`

# Usage

This project serves the article: **Coroutine based Deterministic 2PC Distributed Transaction**

## Run Single Transaction Workloads

```shell
// TXN = {"tpl", "calvin", "bohm", "pwv", "aria", "toro"}
go run src/t_txn/{$TXN}/benchmark/main.go
```

### Sample

```shell
go run src/t_txn/toro/benchmark/main.go
Toro
Low Conflict:
thread  tps     time    write_conflict  read_conflict
1       5322.339508843866       187.8873ms      0       983
2       10444.704169421457      95.7423ms       0       981
4       16171.887760630187      61.8357ms       0       972
8       23317.74153682571       42.8858ms       0       971
12      24090.000240900004      41.511ms        0       975
16      23529.134951353517      42.5005ms       0       974
32      25240.160123575824      39.6194ms       0       974
64      25729.098323749244      38.8665ms       0       973
High Conflict:
thread  tps     time    write_conflict  read_conflict
1       599.7368354765929       1.667398s       0       880
2       1023.1656992293925      977.3588ms      0       870
...
Toro_RO
Low Conflict:
thread  tps     time    write_conflict  read_conflict
1       5305.9996529876225      188.4659ms      0       984
2       9873.871169678527       101.2774ms      0       980
4       15425.623812226966      64.8272ms       0       974
8       23333.030003943284      42.8577ms       0       976
12      25049.78645057051       39.9205ms       0       975
16      24297.55760950909       41.1564ms       0       974
32      24701.848686355686      40.4828ms       0       973
64      25185.427711526114      39.7055ms       0       972
High Conflict:
thread  tps     time    write_conflict  read_conflict
1       641.5065241855017       1.5588306s      0       884
2       1070.0198520783156      934.5621ms      0       882
...
```

## Run Distributed Transaction Workload

```shell
// SyncType = ["sync", "async"]
go run src/t_distributed/benchmark/{$SyncType}/main.go -h
Usage of main:
  -txn string
        string in: ["tpl", "calvin", "bohm", "pwv", "toro", "aria"] (default "toro")
  -v  verbose in ["true", "false"]

```

> tpl (Two-Phase-Lock) is only use for sync Type

### Sample

```shell
go run src/t_distributed/benchmark/async/main.go -txn=toro -v=false
Type: Toro
Client time: 14:16:46 tps: 2753.612322378305 Log rate: 0 Network rate: 0.523119083098302 CommitAndExec rate: 0.05427516319637497 LogOrCommitExec rate: 0.1261406055404222 Other: 0.29646514816490077
Coo_Commit Average Batch size: 19.586
Client time: 14:16:46 tps: 2850.154238872874 Log rate: 0 Network rate: 0.5306905049322337 CommitAndExec rate: 0.048076378565993956 LogOrCommitExec rate: 0.1281180629534985 Other: 0.29311505354827383
Client time: 14:16:47 tps: 2857.234722524427 Log rate: 0 Network rate: 0.5316673806936336 CommitAndExec rate: 0.04966137198587057 LogOrCommitExec rate: 0.12784581057018904 Other: 0.2908254367503068
Coo_Commit Average Batch size: 18.802270577105013
Client time: 14:16:47 tps: 2837.567779320934 Log rate: 0 Network rate: 0.528035254338551 CommitAndExec rate: 0.057632587369472174 LogOrCommitExec rate: 0.12667931964484 Other: 0.28765283864713675
Client time: 14:16:48 tps: 2795.8516029767716 Log rate: 0 Network rate: 0.5193453221403469 CommitAndExec rate: 0.07108635576521607 LogOrCommitExec rate: 0.12509922200670287 Other: 0.2844691000877342
Coo_Commit Average Batch size: 16.34246575342466
Client time: 14:16:48 tps: 2765.1454903806543 Log rate: 0 Network rate: 0.5134333647000605 CommitAndExec rate: 0.08314159390783814 LogOrCommitExec rate: 0.12333638936949386 Other: 0.28008865202260746
Client time: 14:16:49 tps: 2731.4896147847403 Log rate: 0 Network rate: 0.5081803662711172 CommitAndExec rate: 0.09048304211539605 LogOrCommitExec rate: 0.12270568200672706 Other: 0.27863090960675974
Coo_Commit Average Batch size: 15.145912910618794
...
```

## Run Reordering Test

```shell
// Optimization rate test: go run src/t_txn/toro/ro/benchmark/count.go
go run src/t_txn/toro/ro/benchmark/count.go
skew :0.0001
Calvin  Bohm    PWV     Toro
5285.328456736944       5147.3691796123 5710.076572126832       3852.8518809622883
Calvin_ro       Bohm_ro PWV_ro  Toro_ro
5789.650420907585       7160.964152213453       8357.709987463435       5247.415647793461
===============================================
skew :0.001
Calvin  Bohm    PWV     Toro
8354.21888053467        11141.1923304032        11140.571734141397      6897.97889218459
Calvin_ro       Bohm_ro PWV_ro  Toro_ro
7980.463824557483       9092.066262978924       11171.436870210246      7715.9303097174425
===============================================
skew :0.01
Calvin  Bohm    PWV     Toro
11140.819964349377      11140.4476231855        16711.7884956048        10026.771479851204
Calvin_ro       Bohm_ro PWV_ro  Toro_ro
9542.25788906171        13357.733459786543      14322.33855143868       11141.688857196974
...
```

## Prios Test

```shell
go run src/t_txn/toro/prios/main.go
```

